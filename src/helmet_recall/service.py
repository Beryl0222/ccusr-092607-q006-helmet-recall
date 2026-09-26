"""头部护具召回协同服务。

在领域契约之上提供：

- 角色分权：认证核验、检测结论与处置批准分别由核验员、检测员和监管员完成，
  商家提交的材料只登记存档，不能自行解除冻结。
- 批次谱系：批次拆分与合并形成有向谱系，高风险批次先冻结未售库存，
  再沿谱系定位已售产品与责任主体。
- 幂等消息：销售与退回消息按编号幂等；编号相同而序列、数量或去向不同的
  消息进入隔离区，不重复入账。
- 去向仲裁：退货、销毁、换货并发时，每件产品只保留一个最终去向，
  后来的不同去向记为冲突。
- 可控时钟：停售、通知、消费者响应与逾期升级由时钟驱动；
  状态写入 JSONL 日志，服务重启后重放即可继续未完成的召回。
- 分角色视图：监管员可见处置覆盖缺口，平台与商家只获取职责内数据，
  消费者输入产品信息即可得到当前风险、处理方式及其依据。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


# ---------------------------------------------------------------------------
# 角色与错误
# ---------------------------------------------------------------------------


class Role(str, Enum):
    REGULATOR = "regulator"  # 监管员：风险定级、冻结/解冻、召回批准
    CERTIFIER = "certifier"  # 核验员：认证证书核验
    TESTER = "tester"  # 检测员：检测结论登记与更正
    PLATFORM = "platform"
    MERCHANT = "merchant"
    CONSUMER = "consumer"


@dataclass(frozen=True)
class Actor:
    actor_id: str
    role: Role
    org_id: Optional[str] = None  # 平台或商家的职责范围标识


class ServiceError(Exception):
    """服务层基础错误。"""


class ForbiddenError(ServiceError):
    """角色无权执行该操作。"""


class NotFoundError(ServiceError):
    """引用的对象不存在。"""


class ConflictError(ServiceError):
    """请求与当前状态冲突。"""


# ---------------------------------------------------------------------------
# 领域状态
# ---------------------------------------------------------------------------


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Disposition(str, Enum):
    RETURNED = "returned"  # 退货
    DESTROYED = "destroyed"  # 销毁
    EXCHANGED = "exchanged"  # 换货


DISPOSITION_ACTIONS = {
    "return": Disposition.RETURNED,
    "destroy": Disposition.DESTROYED,
    "exchange": Disposition.EXCHANGED,
}


@dataclass
class Producer:
    producer_id: str
    name: str


@dataclass
class HelmetModel:
    model_id: str
    producer_id: str
    structure: dict
    categories: list


@dataclass
class Certificate:
    certificate_id: str
    model_id: str
    cert_no: str
    scope: str
    valid_until: datetime
    submitted_by: str
    status: str = "submitted"  # submitted / verified / rejected / expired
    verified_by: str = ""


@dataclass
class TestSample:
    sample_id: str
    model_id: str
    lot_id: Optional[str]
    conclusion: str  # pass / fail / inconclusive
    note: str
    recorded_by: str
    superseded: bool = False


@dataclass
class ProductLot:
    lot_id: str
    model_id: str
    quantity: int
    status: str = "active"  # active / frozen / split / merged
    children: list = field(default_factory=list)
    assessed: Optional[RiskLevel] = None  # 监管定级
    freeze_reason: str = ""
    sold_anonymous: int = 0


@dataclass
class Shop:
    shop_id: str
    platform_id: str
    merchant_id: str
    name: str
    aliases: set = field(default_factory=set)
    model_ids: set = field(default_factory=set)


@dataclass
class Unit:
    serial: str
    lot_id: str
    model_id: str
    location: str = "warehouse"
    holder: str = ""
    consumer_ref: Optional[str] = None
    sold_by: str = ""
    platform_id: str = ""
    disposition: Optional[Disposition] = None  # 最终去向，唯一
    held: bool = False


@dataclass
class MessageRecord:
    message_id: str
    fingerprint: str
    result: dict


@dataclass
class Campaign:
    campaign_id: str
    lot_ids: list
    measures: list
    scope_lots: set = field(default_factory=set)
    scope_units: set = field(default_factory=set)
    responsible: dict = field(default_factory=dict)
    approved: bool = False
    stop_sale_at: Optional[datetime] = None
    notify_by: Optional[datetime] = None
    respond_by: Optional[datetime] = None
    stop_sale_enforced: bool = False
    notified: set = field(default_factory=set)
    responded: set = field(default_factory=set)
    escalated_stages: set = field(default_factory=set)


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("时间必须携带时区")
    return parsed


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


class RecallService:
    """召回协同服务。所有写操作先落日志再应用，重启后重放恢复。"""

    def __init__(self, clock: Any, journal_path: Optional[str] = None) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._seq = 0
        self._versions: dict[str, int] = {}
        # 状态
        self.producers: dict[str, Producer] = {}
        self.models: dict[str, HelmetModel] = {}
        self.certificates: dict[str, Certificate] = {}
        self.samples: dict[str, TestSample] = {}
        self.lots: dict[str, ProductLot] = {}
        self.shops: dict[str, Shop] = {}
        self.units: dict[str, Unit] = {}
        self.messages: dict[str, MessageRecord] = {}
        self.quarantine: list[dict] = []
        self.campaigns: dict[str, Campaign] = {}
        self.notifications: list[dict] = []  # 只增不改
        self.escalations: list[dict] = []
        self.disposition_conflicts: list[dict] = []
        self._notification_seq = 0
        # 重启恢复：先重放，再打开追加写入
        self._journal_path = str(journal_path) if journal_path else None
        if self._journal_path and Path(self._journal_path).exists():
            for line in Path(self._journal_path).read_text(encoding="utf-8").splitlines():
                if line.strip():
                    self._apply(json.loads(line))
        self._journal = (
            open(self._journal_path, "a", encoding="utf-8") if self._journal_path else None
        )

    def close(self) -> None:
        if self._journal is not None:
            self._journal.close()
            self._journal = None

    def __enter__(self) -> "RecallService":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 事件落盘与重放
    # ------------------------------------------------------------------

    def _emit(self, event_type: str, aggregate_type: str, aggregate_id: str, payload: dict) -> dict:
        self._seq += 1
        version = self._versions.get(aggregate_id, 0) + 1
        self._versions[aggregate_id] = version
        envelope = {
            "event_id": f"evt-{self._seq:08d}",
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": self._clock.now().isoformat(),
            "version": version,
            "payload": payload,
        }
        if self._journal is not None:
            self._journal.write(json.dumps(envelope, ensure_ascii=False) + "\n")
            self._journal.flush()
        self._apply(envelope)
        return envelope

    def _apply(self, event: Mapping[str, Any]) -> None:
        et = event["event_type"]
        agg = event["aggregate_id"]
        body = event["payload"]
        seq = int(str(event["event_id"]).rsplit("-", 1)[-1])
        self._seq = max(self._seq, seq)
        self._versions[agg] = max(self._versions.get(agg, 0), int(event["version"]))

        if et == "PRODUCER_REGISTERED":
            self.producers[agg] = Producer(producer_id=agg, name=body["name"])
        elif et == "MODEL_REGISTERED":
            self.models[agg] = HelmetModel(
                model_id=agg,
                producer_id=body["producer_ref"],
                structure=dict(body.get("structure", {})),
                categories=list(body.get("categories", [])),
            )
        elif et == "CERTIFICATE_SUBMITTED":
            self.certificates[agg] = Certificate(
                certificate_id=agg,
                model_id=body["model_ref"],
                cert_no=body["certificate_ref"],
                scope=body.get("scope", ""),
                valid_until=_parse_dt(body["valid_until"]),
                submitted_by=body.get("submitted_by", ""),
            )
        elif et == "CERTIFICATE_VERIFIED":
            cert = self.certificates[agg]
            cert.status = "verified" if body.get("approved", True) else "rejected"
            cert.verified_by = body.get("verifier", "")
            cert.valid_until = _parse_dt(body["valid_until"])
        elif et == "CERTIFICATE_EXPIRED":
            self.certificates[agg].status = "expired"
        elif et == "TEST_RESULT_RECORDED":
            corrects = body.get("corrects")
            if corrects and corrects in self.samples:
                self.samples[corrects].superseded = True
            self.samples[agg] = TestSample(
                sample_id=agg,
                model_id=body["model_ref"],
                lot_id=body.get("lot_ref"),
                conclusion=body["conclusion"],
                note=body.get("note", ""),
                recorded_by=body.get("recorded_by", ""),
            )
        elif et == "LOT_REGISTERED":
            self.lots[agg] = ProductLot(
                lot_id=agg, model_id=body["model_ref"], quantity=int(body["quantity"])
            )
            for serial in body.get("serials", []):
                self.units[serial] = Unit(serial=serial, lot_id=agg, model_id=body["model_ref"])
        elif et == "LOT_SPLIT":
            source = self.lots[agg]
            source.status = "split"
            source.quantity = 0
            for part in body["parts"]:
                child = ProductLot(
                    lot_id=part["lot_id"], model_id=source.model_id, quantity=int(part["quantity"])
                )
                self.lots[part["lot_id"]] = child
                source.children.append(part["lot_id"])
                for serial in part.get("serials", []):
                    unit = self.units[serial]
                    unit.lot_id = part["lot_id"]
        elif et == "LOT_MERGED":
            target = self.lots[agg]
            for source_id in body["source_lots"]:
                source = self.lots[source_id]
                source.status = "merged"
                source.children.append(agg)
                source.quantity = 0
                for unit in self.units.values():
                    if unit.lot_id == source_id:
                        unit.lot_id = agg
        elif et == "MATERIAL_SUBMITTED":
            pass  # 宣传素材只登记存档，不改变任何控制状态
        elif et == "SHOP_LINKED":
            shop = self.shops.get(agg)
            if shop is None:
                shop = Shop(
                    shop_id=agg,
                    platform_id=body["platform_ref"],
                    merchant_id=body.get("merchant_ref", ""),
                    name=body["name"],
                )
                self.shops[agg] = shop
            else:  # 换名：旧名进入别名，关联关系延续
                shop.aliases.add(shop.name)
                shop.name = body["name"]
                shop.platform_id = body["platform_ref"]
            shop.aliases.update(body.get("aliases", []))
            shop.model_ids.update(body.get("model_refs", []))
        elif et == "INVENTORY_MOVED":
            for serial in body["serials"]:
                unit = self.units[serial]
                unit.location = body["to_location"]
                unit.holder = body.get("holder", "")
        elif et == "SALE_RECORDED":
            lot = self.lots[body["lot_ref"]]
            for serial in body["serials"]:
                unit = self.units.get(serial)
                if unit is None:
                    unit = Unit(serial=serial, lot_id=body["lot_ref"], model_id=lot.model_id)
                    self.units[serial] = unit
                unit.consumer_ref = body["destination"]
                unit.location = "consumer"
                unit.holder = ""
                unit.sold_by = body.get("shop_ref", "")
                unit.platform_id = body.get("platform_ref", "")
            lot.sold_anonymous += int(body.get("quantity", 0))
        elif et == "RETURN_RECORDED":
            for serial in body["serials"]:
                self.units[serial].disposition = Disposition.RETURNED
        elif et == "MESSAGE_QUARANTINED":
            self.quarantine.append(
                {
                    "message_id": agg,
                    "reason": body["reason"],
                    "detail": body.get("detail", {}),
                    "occurred_at": event["occurred_at"],
                }
            )
        elif et == "RISK_ASSESSED":
            self.lots[agg].assessed = RiskLevel(body["level"])
        elif et == "LOT_FROZEN":
            lot = self.lots[agg]
            lot.status = "frozen"
            lot.freeze_reason = body["risk_reason"]
            for serial in body["affected_units"]:
                self.units[serial].held = True
        elif et == "LOT_UNFROZEN":
            lot = self.lots[agg]
            lot.status = "active"
            lot.freeze_reason = ""
            for unit in self.units.values():
                if unit.lot_id == agg:
                    unit.held = False
        elif et == "RECALL_LAUNCHED":
            self.campaigns[agg] = Campaign(
                campaign_id=agg,
                lot_ids=list(body["lots"]),
                measures=list(body["measures"]),
                scope_lots=set(body.get("scope_lots", [])),
                scope_units=set(body.get("scope_units", [])),
                responsible=dict(body.get("responsible", {})),
                stop_sale_at=_parse_dt(body["stop_sale_at"]) if body.get("stop_sale_at") else None,
                notify_by=_parse_dt(body["notify_by"]) if body.get("notify_by") else None,
                respond_by=_parse_dt(body["respond_by"]) if body.get("respond_by") else None,
            )
        elif et == "CAMPAIGN_APPROVED":
            campaign = self.campaigns[agg]
            campaign.approved = True
        elif et == "STOP_SALE_ENFORCED":
            campaign = self.campaigns[agg]
            campaign.stop_sale_enforced = True
            for serial in body["held_units"]:
                self.units[serial].held = True
        elif et == "SCOPE_ADJUSTED":
            campaign = self.campaigns[agg]
            campaign.scope_units -= set(body["removed"])
            campaign.scope_units |= set(body["added"])
            if "scope_lots" in body:
                campaign.scope_lots = set(body["scope_lots"])
            campaign.notified &= campaign.scope_units
            campaign.responded &= campaign.scope_units
        elif et == "NOTIFICATION_SENT":
            self.campaigns[agg].notified.add(body["unit_ref"])
            self.notifications.append(
                {
                    "notification_id": body["notification_id"],
                    "campaign_id": agg,
                    "consumer_ref": body["consumer_ref"],
                    "unit_ref": body["unit_ref"],
                    "channel": body["channel"],
                    "occurred_at": event["occurred_at"],
                }
            )
        elif et == "CONSUMER_RESPONDED":
            self.campaigns[agg].responded.add(body["unit_ref"])
        elif et == "ESCALATION_RAISED":
            record = {
                "campaign_id": agg,
                "stage": body["stage"],
                "pending": list(body.get("pending", [])),
                "occurred_at": event["occurred_at"],
            }
            self.campaigns[agg].escalated_stages.add(body["stage"])
            self.escalations.append(record)
        elif et == "UNIT_DISPOSED":
            self.units[agg].disposition = Disposition(body["disposition"])
        elif et == "DISPOSITION_CONFLICT":
            self.disposition_conflicts.append(
                {
                    "unit_ref": agg,
                    "attempted": body["attempted"],
                    "existing": body["existing"],
                    "occurred_at": event["occurred_at"],
                }
            )
        else:  # pragma: no cover - 防御未知事件
            raise ServiceError(f"未知事件类型: {et}")

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _require(actor: Actor, *roles: Role) -> None:
        if actor.role not in roles:
            names = "/".join(role.value for role in roles)
            raise ForbiddenError(f"角色 {actor.role.value} 无权执行，需 {names}")

    def _lot(self, lot_id: str) -> ProductLot:
        lot = self.lots.get(lot_id)
        if lot is None:
            raise NotFoundError(f"批次不存在: {lot_id}")
        return lot

    def _descendants(self, lot_ids: Iterable[str]) -> set:
        seen = set(lot_ids)
        stack = list(lot_ids)
        while stack:
            current = stack.pop()
            for child in self.lots[current].children:
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return seen

    def _lot_units(self, lot_id: str) -> list:
        return [u for u in self.units.values() if u.lot_id == lot_id]

    def _unsold_serials(self, lot_id: str) -> list:
        return sorted(
            u.serial
            for u in self._lot_units(lot_id)
            if u.consumer_ref is None and u.disposition is None
        )

    def _active_fail(self, lot_id: str) -> bool:
        lot = self.lots[lot_id]
        return any(
            s.conclusion == "fail"
            and not s.superseded
            and (s.lot_id == lot_id or (s.lot_id is None and s.model_id == lot.model_id))
            for s in self.samples.values()
        )

    def _effective_high(self, lot_id: str) -> bool:
        lot = self.lots[lot_id]
        return lot.assessed == RiskLevel.HIGH or self._active_fail(lot_id)

    def _freeze_genealogy(self, lot_id: str, reason: str) -> None:
        for related in sorted(self._descendants([lot_id])):
            lot = self.lots[related]
            if lot.status == "frozen":
                continue
            affected = self._unsold_serials(related)
            self._emit(
                "LOT_FROZEN",
                "product_lot",
                related,
                {"affected_units": affected, "risk_reason": reason},
            )

    def _apply_disposition(self, serial: str, action: Disposition, note: str) -> str:
        """返回 applied / duplicate / conflict；保证每件产品只有一个最终去向。"""
        unit = self.units.get(serial)
        if unit is None:
            raise NotFoundError(f"产品不存在: {serial}")
        if unit.disposition is None:
            self._emit(
                "UNIT_DISPOSED", "unit", serial,
                {"unit_ref": serial, "disposition": action.value, "note": note},
            )
            return "applied"
        if unit.disposition == action:
            return "duplicate"
        self._emit(
            "DISPOSITION_CONFLICT", "unit", serial,
            {
                "unit_ref": serial,
                "attempted": action.value,
                "existing": unit.disposition.value,
            },
        )
        return "conflict"

    @staticmethod
    def _fingerprint(parts: Mapping[str, Any]) -> str:
        return json.dumps(parts, sort_keys=True, ensure_ascii=False)

    def _quarantine(self, message_id: str, reason: str, detail: dict) -> dict:
        self._emit(
            "MESSAGE_QUARANTINED", "message", message_id,
            {"message_id": message_id, "reason": reason, "detail": detail},
        )
        return {"message_id": message_id, "status": "quarantined", "reason": reason}

    # ------------------------------------------------------------------
    # 注册与档案
    # ------------------------------------------------------------------

    def register_producer(self, actor: Actor, producer_id: str, name: str) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM)
            self._emit("PRODUCER_REGISTERED", "producer", producer_id, {"name": name})

    def register_model(
        self,
        actor: Actor,
        model_id: str,
        producer_id: str,
        structure: Optional[dict] = None,
        categories: Optional[list] = None,
    ) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            if producer_id not in self.producers:
                raise NotFoundError(f"生产主体不存在: {producer_id}")
            self._emit(
                "MODEL_REGISTERED", "helmet_model", model_id,
                {
                    "producer_ref": producer_id,
                    "structure": structure or {},
                    "categories": categories or [],
                },
            )

    def submit_certificate(
        self,
        actor: Actor,
        certificate_id: str,
        model_id: str,
        cert_no: str,
        valid_until: datetime,
        scope: str = "",
    ) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            if model_id not in self.models:
                raise NotFoundError(f"型号不存在: {model_id}")
            self._emit(
                "CERTIFICATE_SUBMITTED", "certification_record", certificate_id,
                {
                    "certificate_ref": cert_no,
                    "model_ref": model_id,
                    "scope": scope,
                    "valid_until": valid_until.isoformat(),
                    "submitted_by": actor.actor_id,
                },
            )

    def verify_certificate(self, actor: Actor, certificate_id: str, approved: bool, note: str = "") -> None:
        with self._lock:
            self._require(actor, Role.CERTIFIER)
            cert = self.certificates.get(certificate_id)
            if cert is None:
                raise NotFoundError(f"证书不存在: {certificate_id}")
            self._emit(
                "CERTIFICATE_VERIFIED", "certification_record", certificate_id,
                {
                    "certificate_ref": cert.cert_no,
                    "valid_until": cert.valid_until.isoformat(),
                    "approved": approved,
                    "verifier": actor.actor_id,
                    "note": note,
                },
            )

    def record_test_result(
        self,
        actor: Actor,
        sample_id: str,
        model_id: str,
        conclusion: str,
        lot_id: Optional[str] = None,
        note: str = "",
        corrects: Optional[str] = None,
    ) -> None:
        """登记检测结论；fail 自动按高风险处置，更正只调整受影响范围。"""
        with self._lock:
            self._require(actor, Role.TESTER)
            if model_id not in self.models:
                raise NotFoundError(f"型号不存在: {model_id}")
            if lot_id is not None:
                self._lot(lot_id)
            if corrects is not None and corrects not in self.samples:
                raise NotFoundError(f"被更正的样品不存在: {corrects}")
            self._emit(
                "TEST_RESULT_RECORDED", "test_sample", sample_id,
                {
                    "sample_ref": sample_id,
                    "model_ref": model_id,
                    "lot_ref": lot_id,
                    "conclusion": conclusion,
                    "note": note,
                    "recorded_by": actor.actor_id,
                    "corrects": corrects,
                },
            )
            affected_lots = [lot_id] if lot_id else [
                lid for lid, lot in self.lots.items() if lot.model_id == model_id
            ]
            if conclusion == "fail":
                for lid in affected_lots:
                    if self.lots[lid].assessed != RiskLevel.HIGH:
                        self._emit(
                            "RISK_ASSESSED", "product_lot", lid,
                            {"level": "high", "reason": f"检测不合格: {sample_id}", "source": sample_id},
                        )
                    self._freeze_genealogy(lid, f"检测不合格: {sample_id}")
            if corrects is not None:
                for lid in affected_lots:
                    if not self._active_fail(lid) and self.lots[lid].assessed == RiskLevel.HIGH:
                        self._emit(
                            "RISK_ASSESSED", "product_lot", lid,
                            {"level": "low", "reason": f"检测更正: {sample_id}", "source": sample_id},
                        )
                self._adjust_campaign_scopes(reason=f"检测更正: {sample_id}")

    def _adjust_campaign_scopes(self, reason: str) -> None:
        """检测更正后重算召回范围；已发出的通知保留不撤销。"""
        for campaign in self.campaigns.values():
            kept = [lid for lid in campaign.lot_ids if self._effective_high(lid)]
            scope_lots = self._descendants(kept) if kept else set()
            scope_units = {
                u.serial for u in self.units.values() if u.lot_id in scope_lots
            }
            added = sorted(scope_units - campaign.scope_units)
            removed = sorted(campaign.scope_units - scope_units)
            if added or removed:
                self._emit(
                    "SCOPE_ADJUSTED", "recall_campaign", campaign.campaign_id,
                    {
                        "added": added,
                        "removed": removed,
                        "scope_lots": sorted(scope_lots),
                        "reason": reason,
                    },
                )

    # ------------------------------------------------------------------
    # 批次谱系与流通
    # ------------------------------------------------------------------

    def register_lot(
        self,
        actor: Actor,
        lot_id: str,
        model_id: str,
        quantity: int,
        serials: Optional[list] = None,
    ) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            if model_id not in self.models:
                raise NotFoundError(f"型号不存在: {model_id}")
            if lot_id in self.lots:
                raise ConflictError(f"批次已存在: {lot_id}")
            serials = list(serials or [])
            if len(serials) != len(set(serials)):
                raise ConflictError("序列号存在重复")
            self._emit(
                "LOT_REGISTERED", "product_lot", lot_id,
                {"model_ref": model_id, "quantity": quantity, "serials": serials},
            )

    def split_lot(self, actor: Actor, source_lot_id: str, parts: list) -> None:
        """拆分批次；parts 为 [{lot_id, quantity, serials?}]，数量须等于源批次。"""
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            source = self._lot(source_lot_id)
            if source.status != "active":
                raise ConflictError(f"批次状态不允许拆分: {source.status}")
            if any(p["lot_id"] in self.lots for p in parts):
                raise ConflictError("拆分目标批次编号已存在")
            if sum(int(p["quantity"]) for p in parts) != source.quantity:
                raise ConflictError("拆分数量之和必须等于源批次数量")
            moved = [s for p in parts for s in p.get("serials", [])]
            for serial in moved:
                unit = self.units.get(serial)
                if unit is None or unit.lot_id != source_lot_id:
                    raise NotFoundError(f"序列号不属于源批次: {serial}")
            self._emit(
                "LOT_SPLIT", "product_lot", source_lot_id,
                {"source_lot": source_lot_id, "parts": parts},
            )

    def merge_lots(self, actor: Actor, target_lot_id: str, source_lot_ids: list) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            if target_lot_id in self.lots:
                raise ConflictError(f"批次已存在: {target_lot_id}")
            sources = [self._lot(lid) for lid in source_lot_ids]
            if any(s.status != "active" for s in sources):
                raise ConflictError("只有活动批次可以合并")
            model_ids = {s.model_id for s in sources}
            if len(model_ids) != 1:
                raise ConflictError("跨型号批次不能合并")
            quantity = sum(s.quantity for s in sources)
            self._emit(
                "LOT_REGISTERED", "product_lot", target_lot_id,
                {"model_ref": model_ids.pop(), "quantity": quantity, "serials": []},
            )
            self._emit(
                "LOT_MERGED", "product_lot", target_lot_id,
                {"target_lot": target_lot_id, "source_lots": list(source_lot_ids)},
            )

    def link_shop(
        self,
        actor: Actor,
        shop_id: str,
        platform_id: str,
        name: str,
        merchant_id: str = "",
        aliases: Optional[list] = None,
        model_ids: Optional[list] = None,
    ) -> None:
        """登记店铺与型号的关联；再次登记视为换名，旧名进入别名。"""
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM)
            self._emit(
                "SHOP_LINKED", "shop", shop_id,
                {
                    "platform_ref": platform_id,
                    "merchant_ref": merchant_id,
                    "name": name,
                    "aliases": list(aliases or []),
                    "model_refs": list(model_ids or []),
                },
            )

    def submit_material(
        self, actor: Actor, material_id: str, model_id: str, kind: str, content_ref: str
    ) -> None:
        """商家提交宣传或申诉材料：只登记存档，不影响冻结状态。"""
        with self._lock:
            self._require(actor, Role.MERCHANT, Role.PLATFORM)
            if model_id not in self.models:
                raise NotFoundError(f"型号不存在: {model_id}")
            self._emit(
                "MATERIAL_SUBMITTED", "marketing_material", material_id,
                {
                    "material_ref": material_id,
                    "model_ref": model_id,
                    "kind": kind,
                    "content_ref": content_ref,
                    "submitted_by": actor.actor_id,
                },
            )

    def move_inventory(
        self, actor: Actor, lot_id: str, serials: list, to_location: str, holder: str = ""
    ) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            self._lot(lot_id)
            for serial in serials:
                unit = self.units.get(serial)
                if unit is None or unit.lot_id != lot_id:
                    raise NotFoundError(f"序列号不属于批次 {lot_id}: {serial}")
                if unit.consumer_ref is not None or unit.disposition is not None:
                    raise ConflictError(f"产品已售出或已处置: {serial}")
            self._emit(
                "INVENTORY_MOVED", "product_lot", lot_id,
                {"serials": list(serials), "to_location": to_location, "holder": holder},
            )

    # ------------------------------------------------------------------
    # 幂等消息：销售回执与退回
    # ------------------------------------------------------------------

    def record_sale(self, actor: Actor, message: Mapping[str, Any]) -> dict:
        """销售回执。按 message_id 幂等；编号相同而内容不同则隔离。"""
        with self._lock:
            self._require(actor, Role.PLATFORM, Role.MERCHANT)
            message_id = str(message["message_id"])
            lot = self._lot(str(message["lot_id"]))
            serials = sorted(str(s) for s in message.get("serials", []))
            quantity = int(message.get("quantity", 0))
            fingerprint = self._fingerprint(
                {
                    "kind": "sale",
                    "lot_id": lot.lot_id,
                    "shop_id": message.get("shop_id", ""),
                    "serials": serials,
                    "quantity": quantity,
                    "destination": message.get("destination", ""),
                }
            )
            known = self.messages.get(message_id)
            if known is not None:
                if known.fingerprint == fingerprint:
                    return dict(known.result, status="duplicate")
                return self._quarantine(
                    message_id, "conflicting_message_id",
                    {"first": known.fingerprint, "again": fingerprint},
                )
            for serial in serials:
                unit = self.units.get(serial)
                if unit is not None and unit.lot_id != lot.lot_id:
                    return self._quarantine(
                        message_id, "serial_in_other_lot",
                        {"serial": serial, "lot_id": unit.lot_id},
                    )
                if unit is not None and (unit.consumer_ref is not None or unit.disposition is not None):
                    return self._quarantine(
                        message_id, "unit_already_sold_or_disposed", {"serial": serial},
                    )
            sold_count = sum(
                1 for u in self._lot_units(lot.lot_id) if u.consumer_ref is not None
            )
            if quantity > lot.quantity - lot.sold_anonymous - sold_count - len(serials):
                return self._quarantine(
                    message_id, "quantity_exceeds_stock", {"quantity": quantity},
                )
            shop = self.shops.get(str(message.get("shop_id", "")))
            self._emit(
                "SALE_RECORDED", "sale_receipt", message_id,
                {
                    "message_id": message_id,
                    "lot_ref": lot.lot_id,
                    "shop_ref": shop.shop_id if shop else "",
                    "platform_ref": shop.platform_id if shop else "",
                    "serials": serials,
                    "quantity": quantity,
                    "destination": str(message.get("destination", "")),
                },
            )
            result = {"message_id": message_id, "status": "recorded", "serials": serials}
            self.messages[message_id] = MessageRecord(message_id, fingerprint, result)
            return dict(result)

    def record_return(self, actor: Actor, message: Mapping[str, Any]) -> dict:
        """退回消息。按 message_id 幂等；编号相同而序列不同则隔离。"""
        with self._lock:
            self._require(actor, Role.PLATFORM, Role.MERCHANT)
            message_id = str(message["message_id"])
            serials = sorted(str(s) for s in message.get("serials", []))
            fingerprint = self._fingerprint({"kind": "return", "serials": serials})
            known = self.messages.get(message_id)
            if known is not None:
                if known.fingerprint == fingerprint:
                    return dict(known.result, status="duplicate")
                return self._quarantine(
                    message_id, "conflicting_message_id",
                    {"first": known.fingerprint, "again": fingerprint},
                )
            for serial in serials:
                unit = self.units.get(serial)
                if unit is None or unit.consumer_ref is None:
                    return self._quarantine(
                        message_id, "unit_not_sold", {"serial": serial},
                    )
                if unit.disposition is not None:
                    return self._quarantine(
                        message_id, "unit_already_disposed",
                        {"serial": serial, "existing": unit.disposition.value},
                    )
            self._emit(
                "RETURN_RECORDED", "return_receipt", message_id,
                {"message_id": message_id, "serials": serials},
            )
            result = {"message_id": message_id, "status": "recorded", "serials": serials}
            self.messages[message_id] = MessageRecord(message_id, fingerprint, result)
            return dict(result)

    # ------------------------------------------------------------------
    # 风险控制与召回
    # ------------------------------------------------------------------

    def assess_risk(self, actor: Actor, lot_id: str, level: str, reason: str) -> None:
        """监管定级；高风险先冻结未售库存（含拆分/合并出的下游批次）。"""
        with self._lock:
            self._require(actor, Role.REGULATOR)
            self._lot(lot_id)
            risk = RiskLevel(level)
            self._emit(
                "RISK_ASSESSED", "product_lot", lot_id,
                {"level": risk.value, "reason": reason, "source": actor.actor_id},
            )
            if risk == RiskLevel.HIGH:
                self._freeze_genealogy(lot_id, reason)

    def unfreeze_lot(self, actor: Actor, lot_id: str) -> None:
        """解冻只能由监管员执行；商家提交的材料不产生解冻效果。"""
        with self._lock:
            self._require(actor, Role.REGULATOR)
            self._lot(lot_id)
            self._emit(
                "LOT_UNFROZEN", "product_lot", lot_id, {"approved_by": actor.actor_id}
            )

    def launch_recall(
        self,
        actor: Actor,
        campaign_id: str,
        lot_ids: list,
        measures: list,
        stop_sale_at: Optional[datetime] = None,
        notify_by: Optional[datetime] = None,
        respond_by: Optional[datetime] = None,
    ) -> None:
        """发起召回：沿谱系锁定受影响范围与责任主体。"""
        with self._lock:
            self._require(actor, Role.REGULATOR)
            for lid in lot_ids:
                self._lot(lid)
            if campaign_id in self.campaigns:
                raise ConflictError(f"召回已存在: {campaign_id}")
            scope_lots = self._descendants(lot_ids)
            scope_units = sorted(
                u.serial for u in self.units.values() if u.lot_id in scope_lots
            )
            producers, shops, platforms = set(), set(), set()
            for lid in scope_lots:
                model = self.models.get(self.lots[lid].model_id)
                if model:
                    producers.add(model.producer_id)
            for serial in scope_units:
                unit = self.units[serial]
                if unit.sold_by:
                    shops.add(unit.sold_by)
                if unit.platform_id:
                    platforms.add(unit.platform_id)
            self._emit(
                "RECALL_LAUNCHED", "recall_campaign", campaign_id,
                {
                    "lots": list(lot_ids),
                    "measures": list(measures),
                    "scope_lots": sorted(scope_lots),
                    "scope_units": scope_units,
                    "responsible": {
                        "producers": sorted(producers),
                        "shops": sorted(shops),
                        "platforms": sorted(platforms),
                    },
                    "stop_sale_at": _iso(stop_sale_at),
                    "notify_by": _iso(notify_by),
                    "respond_by": _iso(respond_by),
                },
            )

    def approve_campaign(self, actor: Actor, campaign_id: str) -> None:
        with self._lock:
            self._require(actor, Role.REGULATOR)
            campaign = self._campaign(campaign_id)
            if campaign.approved:
                raise ConflictError(f"召回已批准: {campaign_id}")
            self._emit(
                "CAMPAIGN_APPROVED", "recall_campaign", campaign_id,
                {"approved_by": actor.actor_id},
            )

    def _campaign(self, campaign_id: str) -> Campaign:
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            raise NotFoundError(f"召回不存在: {campaign_id}")
        return campaign

    def notify_consumers(self, actor: Actor, campaign_id: str, channel: str = "sms") -> list:
        """向已售未通知的消费者发出通知；通知记录只增不改。"""
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM)
            campaign = self._campaign(campaign_id)
            if not campaign.approved:
                raise ConflictError("召回未批准，不能通知")
            sent = []
            for serial in sorted(campaign.scope_units - campaign.notified):
                unit = self.units[serial]
                if unit.consumer_ref is None:
                    continue
                self._notification_seq += 1
                notification_id = f"ntf-{self._notification_seq:06d}"
                self._emit(
                    "NOTIFICATION_SENT", "recall_campaign", campaign_id,
                    {
                        "notification_id": notification_id,
                        "consumer_ref": unit.consumer_ref,
                        "unit_ref": serial,
                        "channel": channel,
                    },
                )
                sent.append(notification_id)
            return sent

    def record_consumer_response(
        self, actor: Actor, campaign_id: str, serial: str, choice: str
    ) -> str:
        """消费者响应：return / exchange 直接进入去向仲裁。"""
        with self._lock:
            campaign = self._campaign(campaign_id)
            if serial not in campaign.scope_units:
                raise NotFoundError(f"产品不在召回范围: {serial}")
            unit = self.units[serial]
            self._emit(
                "CONSUMER_RESPONDED", "recall_campaign", campaign_id,
                {
                    "unit_ref": serial,
                    "consumer_ref": unit.consumer_ref or "",
                    "choice": choice,
                },
            )
            if choice in ("return", "exchange"):
                return self._apply_disposition(
                    serial, DISPOSITION_ACTIONS[choice], f"消费者响应: {campaign_id}"
                )
            return "acknowledged"

    def dispose(self, actor: Actor, serials: list, action: str, note: str = "") -> list:
        """退货/销毁/换货仲裁：每件产品只保留一个最终去向。"""
        with self._lock:
            self._require(actor, Role.REGULATOR, Role.PLATFORM, Role.MERCHANT)
            try:
                disposition = DISPOSITION_ACTIONS[action]
            except KeyError:
                raise ServiceError(f"未知处置方式: {action}") from None
            return [
                {"serial": s, "outcome": self._apply_disposition(s, disposition, note)}
                for s in serials
            ]

    # ------------------------------------------------------------------
    # 时钟驱动：停售、通知逾期、响应逾期、证书到期
    # ------------------------------------------------------------------

    def tick(self) -> None:
        with self._lock:
            now = self._clock.now()
            for cert in sorted(self.certificates.values(), key=lambda c: c.certificate_id):
                if cert.status == "verified" and cert.valid_until <= now:
                    self._emit(
                        "CERTIFICATE_EXPIRED", "certification_record", cert.certificate_id,
                        {"certificate_ref": cert.cert_no},
                    )
            for campaign in sorted(self.campaigns.values(), key=lambda c: c.campaign_id):
                if not campaign.approved:
                    continue
                if (
                    campaign.stop_sale_at is not None
                    and not campaign.stop_sale_enforced
                    and now >= campaign.stop_sale_at
                ):
                    held = sorted(
                        s
                        for s in campaign.scope_units
                        if self.units[s].consumer_ref is None
                        and self.units[s].disposition is None
                        and not self.units[s].held
                    )
                    self._emit(
                        "STOP_SALE_ENFORCED", "recall_campaign", campaign.campaign_id,
                        {"held_units": held},
                    )
                if (
                    campaign.notify_by is not None
                    and now > campaign.notify_by
                    and "notify_overdue" not in campaign.escalated_stages
                ):
                    pending = sorted(
                        s
                        for s in campaign.scope_units - campaign.notified
                        if self.units[s].consumer_ref is not None
                    )
                    if pending:
                        self._emit(
                            "ESCALATION_RAISED", "recall_campaign", campaign.campaign_id,
                            {"stage": "notify_overdue", "pending": pending},
                        )
                    else:
                        campaign.escalated_stages.add("notify_overdue")
                if (
                    campaign.respond_by is not None
                    and now > campaign.respond_by
                    and "response_overdue" not in campaign.escalated_stages
                ):
                    pending = sorted(
                        s
                        for s in campaign.notified - campaign.responded
                        if self.units[s].disposition is None
                    )
                    if pending:
                        self._emit(
                            "ESCALATION_RAISED", "recall_campaign", campaign.campaign_id,
                            {"stage": "response_overdue", "pending": pending},
                        )
                    else:
                        campaign.escalated_stages.add("response_overdue")

    # ------------------------------------------------------------------
    # 分角色视图
    # ------------------------------------------------------------------

    def regulator_coverage(self, actor: Actor) -> dict:
        """监管视图：处置覆盖缺口、隔离消息、升级记录。"""
        with self._lock:
            self._require(actor, Role.REGULATOR)
            campaigns = []
            for campaign in self.campaigns.values():
                disposed = {
                    s for s in campaign.scope_units if self.units[s].disposition is not None
                }
                gap = sorted(campaign.scope_units - disposed)
                campaigns.append(
                    {
                        "campaign_id": campaign.campaign_id,
                        "approved": campaign.approved,
                        "measures": list(campaign.measures),
                        "scope": len(campaign.scope_units),
                        "notified": len(campaign.notified & campaign.scope_units),
                        "responded": len(campaign.responded & campaign.scope_units),
                        "disposed": len(disposed),
                        "coverage_gap": gap,
                        "responsible": dict(campaign.responsible),
                    }
                )
            return {
                "campaigns": campaigns,
                "quarantined_messages": list(self.quarantine),
                "escalations": list(self.escalations),
                "disposition_conflicts": list(self.disposition_conflicts),
                "frozen_lots": sorted(
                    lid for lid, lot in self.lots.items() if lot.status == "frozen"
                ),
            }

    def platform_view(self, actor: Actor) -> dict:
        """平台视图：仅本平台职责范围内的数据。"""
        with self._lock:
            self._require(actor, Role.PLATFORM)
            platform_id = actor.org_id
            shops = [
                {
                    "shop_id": s.shop_id,
                    "name": s.name,
                    "aliases": sorted(s.aliases),
                    "model_ids": sorted(s.model_ids),
                }
                for s in self.shops.values()
                if s.platform_id == platform_id
            ]
            sales = [
                {
                    "serial": u.serial,
                    "lot_id": u.lot_id,
                    "model_id": u.model_id,
                    "consumer_ref": u.consumer_ref,
                    "disposition": u.disposition.value if u.disposition else None,
                }
                for u in self.units.values()
                if u.platform_id == platform_id
            ]
            sold_serials = {s["serial"] for s in sales}
            recalls = [
                {
                    "campaign_id": c.campaign_id,
                    "measures": list(c.measures),
                    "affected_serials": sorted(c.scope_units & sold_serials),
                }
                for c in self.campaigns.values()
                if c.scope_units & sold_serials
            ]
            return {"platform_id": platform_id, "shops": shops, "sales": sales, "recalls": recalls}

    def merchant_view(self, actor: Actor) -> dict:
        """商家视图：仅本商家店铺、所售型号与相关召回。"""
        with self._lock:
            self._require(actor, Role.MERCHANT)
            shops = [s for s in self.shops.values() if s.merchant_id == actor.org_id]
            model_ids = set().union(*(s.model_ids for s in shops)) if shops else set()
            certificates = [
                {
                    "certificate_id": c.certificate_id,
                    "model_id": c.model_id,
                    "status": c.status,
                    "valid_until": c.valid_until.isoformat(),
                }
                for c in self.certificates.values()
                if c.model_id in model_ids
            ]
            lot_ids = {lid for lid, lot in self.lots.items() if lot.model_id in model_ids}
            recalls = [
                {
                    "campaign_id": c.campaign_id,
                    "measures": list(c.measures),
                    "approved": c.approved,
                }
                for c in self.campaigns.values()
                if c.scope_lots & lot_ids
            ]
            return {
                "merchant_id": actor.org_id,
                "shops": [
                    {"shop_id": s.shop_id, "name": s.name, "aliases": sorted(s.aliases)}
                    for s in shops
                ],
                "model_ids": sorted(model_ids),
                "certificates": certificates,
                "recalls": recalls,
            }

    def consumer_query(
        self,
        serial: Optional[str] = None,
        model_id: Optional[str] = None,
        lot_id: Optional[str] = None,
        shop_name: Optional[str] = None,
    ) -> dict:
        """消费者查询：输入产品信息，得到当前风险、处理方式及其依据。"""
        with self._lock:
            unit = self.units.get(serial) if serial else None
            if unit is not None:
                lot_id, model_id = unit.lot_id, unit.model_id
            lot = self.lots.get(lot_id) if lot_id else None
            if lot is not None:
                model_id = lot.model_id
            model = self.models.get(model_id) if model_id else None
            if model is None and shop_name:
                for shop in self.shops.values():
                    if shop_name == shop.name or shop_name in shop.aliases:
                        model = self.models.get(sorted(shop.model_ids)[0]) if shop.model_ids else None
                        if model is not None:
                            break
            if model is None and lot is None and unit is None:
                return {
                    "found": False,
                    "risk": "unknown",
                    "handling": "未查询到产品信息，请核对序列号或批次号",
                    "basis": [],
                }

            now = self._clock.now()
            basis = []
            valid_cert = False
            if model is not None:
                for cert in self.certificates.values():
                    if cert.model_id != model.model_id:
                        continue
                    basis.append(
                        {
                            "type": "certificate",
                            "ref": cert.certificate_id,
                            "status": cert.status,
                            "valid_until": cert.valid_until.isoformat(),
                        }
                    )
                    if cert.status == "verified" and cert.valid_until > now:
                        valid_cert = True
            fails = [
                s
                for s in self.samples.values()
                if s.conclusion == "fail"
                and not s.superseded
                and (
                    (lot is not None and s.lot_id == lot.lot_id)
                    or (s.lot_id is None and model is not None and s.model_id == model.model_id)
                )
            ]
            for sample in fails:
                basis.append(
                    {"type": "test", "ref": sample.sample_id, "conclusion": sample.conclusion}
                )

            campaigns = [
                c
                for c in self.campaigns.values()
                if (unit is not None and unit.serial in c.scope_units)
                or (lot is not None and lot.lot_id in c.scope_lots)
            ]
            for campaign in campaigns:
                basis.append(
                    {
                        "type": "recall",
                        "ref": campaign.campaign_id,
                        "measures": list(campaign.measures),
                    }
                )
            if unit is not None:
                for note in self.notifications:
                    if note["unit_ref"] == unit.serial:
                        basis.append(
                            {
                                "type": "notification",
                                "ref": note["notification_id"],
                                "channel": note["channel"],
                                "occurred_at": note["occurred_at"],
                            }
                        )

            lot_high = lot is not None and self._effective_high(lot.lot_id)
            if fails or lot_high or campaigns or not valid_cert:
                risk = RiskLevel.HIGH.value
            else:
                risk = RiskLevel.LOW.value

            if unit is not None and unit.disposition is not None:
                done = {
                    Disposition.RETURNED: "退货",
                    Disposition.DESTROYED: "销毁",
                    Disposition.EXCHANGED: "换货",
                }[unit.disposition]
                handling = f"该产品已完成{done}处置"
            elif campaigns:
                measure_text = {
                    "refund": "联系商家退货退款",
                    "exchange": "联系商家换货",
                    "destroy": "停止使用并按指引销毁",
                    "repair": "联系商家检修",
                }
                steps = [measure_text.get(m, m) for c in campaigns for m in c.measures]
                deadline = next(
                    (c.respond_by for c in campaigns if c.respond_by is not None), None
                )
                handling = "；".join(dict.fromkeys(steps)) or "停止使用并等待处置指引"
                if deadline is not None:
                    handling += f"；请在 {deadline.isoformat()} 前响应"
            elif lot is not None and (lot.status == "frozen" or (unit is not None and unit.held)):
                handling = "该批次已被控制，停止销售与使用，等待监管处置"
            elif not valid_cert:
                handling = "该型号缺少有效认证，建议停止使用并关注处置信息"
            else:
                handling = "未发现风险，可正常使用"

            return {
                "found": True,
                "risk": risk,
                "handling": handling,
                "basis": basis,
                "model_id": model.model_id if model else None,
                "lot_id": lot.lot_id if lot else None,
                "serial": unit.serial if unit else None,
            }
