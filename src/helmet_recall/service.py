"""头部护具召回协同服务。

角色分离、批次谱系追溯、编号幂等与冲突隔离、单件唯一最终去向、
可控时钟下的通知期限与逾期升级，全部基于事件流重放，服务重启后可继续。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .clock import ControlledClock
from .contracts import validate_event
from .errors import AuthorizationError, ConflictingMessageError, StateError, UnknownReferenceError
from .store import EventStore

# 角色：认证核验、检测结论、处置批准必须由不同角色完成
ROLE_REGULATOR = "regulator"          # 监管：立案监督、覆盖缺口、批准处置
ROLE_CERT_VERIFIER = "cert_verifier"  # 认证核验员
ROLE_TESTER = "tester"                # 检测机构
ROLE_APPROVER = "approver"            # 处置批准人
ROLE_PLATFORM = "platform"            # 平台
ROLE_MERCHANT = "merchant"            # 商家（店铺/生产主体申报）
ROLE_WAREHOUSE = "warehouse"          # 仓库/物流
ROLE_CONSUMER = "consumer"            # 消费者

RISK_ORDER = {"low": 1, "medium": 2, "high": 3}
DISPOSITIONS = {"return", "destroy", "exchange"}
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "domain.schema.json"


@dataclass(frozen=True)
class Actor:
    role: str
    ref: str

    def requires(self, *roles: str) -> None:
        if self.role not in roles:
            raise AuthorizationError(f"角色 {self.role} 无权执行该操作，需要 {roles} 之一")


@dataclass
class _Model:
    producer_ref: str = ""
    name: str = ""
    uses: list[str] = field(default_factory=list)
    listings: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class _Lot:
    model_ref: str = ""
    producer_ref: str = ""
    parents: list[str] = field(default_factory=list)
    children: list[str] = field(default_factory=list)
    status: str = "active"  # active / frozen / released
    frozen_units: list[str] = field(default_factory=list)


@dataclass
class _Campaign:
    lot_refs: set[str] = field(default_factory=set)
    model_refs: set[str] = field(default_factory=set)
    risk_level: str = "low"
    status: str = "open"
    basis: list[str] = field(default_factory=list)
    escalated_notices: set[str] = field(default_factory=set)
    adjustments: list[dict[str, Any]] = field(default_factory=list)


class RecallService:
    """命令与查询都折叠自同一事件流；命令在锁内原子追加。"""

    def __init__(
        self,
        clock: Optional[ControlledClock] = None,
        store: Optional[EventStore] = None,
        schema_path: str | Path = SCHEMA_PATH,
    ) -> None:
        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        self.schema = schema
        self.clock = clock or ControlledClock()
        self.store = store or EventStore(schema)
        self._lock = threading.RLock()
        # 已重放事件之后再生成内部编号，保证重启后自增序号不与历史编号碰撞
        self._seq = 0
        self._applied: set[str] = set()
        # 读模型
        self.models: dict[str, _Model] = {}
        self.certs: dict[str, dict[str, Any]] = {}
        self.lots: dict[str, _Lot] = {}
        self.units: dict[str, dict[str, Any]] = {}
        self.receipts: dict[str, dict[str, Any]] = {}
        self.tests: list[dict[str, Any]] = []
        self.materials: list[dict[str, Any]] = []
        self.campaigns: dict[str, _Campaign] = {}
        self.notices: dict[str, dict[str, Any]] = {}
        self.responses: list[dict[str, Any]] = []
        self.dispositions: dict[str, dict[str, Any]] = {}
        for event in self.store.events:
            self._apply(event)
            self._applied.add(event["event_id"])
        self._seq = len(self.store.events)

    # ------------------------------------------------------------------ 基础

    def _new_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self.clock.now().strftime('%m%d%H%M')}-{self._seq:04d}"

    def _append(
        self,
        event_id: Optional[str],
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        event = {
            "event_id": event_id or self._new_id(event_type.lower()),
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": self.clock.now().isoformat(),
            "version": self.store.next_version(aggregate_id),
            "payload": payload,
        }
        issues = validate_event(event, self.schema)
        if issues:
            raise StateError(f"事件不符合契约: {issues[0].field} {issues[0].code}")
        stored = self.store.append(event)
        if event["event_id"] not in self._applied:
            self._apply(stored)
            self._applied.add(event["event_id"])
        return stored

    def _seen(self, event_id: Optional[str]) -> bool:
        return bool(event_id) and event_id in self._applied

    def _pre_ingest(
        self, event_id: Optional[str], event_type: str, aggregate_type: str,
        aggregate_id: str, payload: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """状态校验前判定重复/冲突消息（同编号不同内容即隔离）。"""
        if not event_id:
            return None
        return self.store.ingest_duplicate({
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": self.clock.now().isoformat(),
            "version": self.store.next_version(aggregate_id),
            "payload": payload,
        })

    def _event(self, event_id: str) -> Optional[dict[str, Any]]:
        return next((e for e in self.store.events if e["event_id"] == event_id), None)

    def _descendants(self, lot_ref: str) -> set[str]:
        seen: set[str] = set()
        stack = [lot_ref]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            lot = self.lots.get(current)
            if lot:
                stack.extend(lot.children)
        return seen

    def _roots(self, lot_ref: str) -> set[str]:
        seen: set[str] = set()
        stack = [lot_ref]
        while stack:
            current = stack.pop()
            lot = self.lots.get(current)
            if not lot or not lot.parents:
                seen.add(current)
                continue
            stack.extend(lot.parents)
        return seen

    def _apply(self, e: dict[str, Any]) -> None:
        etype = e["event_type"]
        p = e["payload"]
        if etype == "MODEL_REGISTERED":
            self.models[e["aggregate_id"]] = _Model(
                producer_ref=p.get("producer_ref", ""),
                name=p.get("model_name", ""),
                uses=list(p.get("designated_uses", [])),
            )
        elif etype == "MODEL_LISTING_PUBLISHED":
            model = self.models.setdefault(p["model_ref"], _Model())
            model.listings[p["listing_ref"]] = {"shop_ref": p["shop_ref"], "status": "listed"}
        elif etype == "LISTING_DELISTED":
            model = self.models.get(p["model_ref"])
            if model and p.get("listing_ref") in model.listings:
                model.listings[p["listing_ref"]]["status"] = "delisted"
        elif etype == "CERTIFICATE_VERIFIED":
            self.certs[p["certificate_ref"]] = {
                "model_ref": p.get("model_ref", e["aggregate_id"]),
                "valid_until": p["valid_until"],
                "status": "valid",
            }
        elif etype == "CERTIFICATE_REVOKED":
            cert = self.certs.get(p["certificate_ref"])
            if cert:
                cert["status"] = "revoked"
        elif etype == "LOT_CREATED":
            lot = _Lot(model_ref=p.get("model_ref", ""), producer_ref=p.get("producer_ref", ""))
            self.lots[p["lot_ref"]] = lot
            for unit_ref in p.get("unit_refs", []):
                self.units.setdefault(unit_ref, {"lot_ref": p["lot_ref"], "location": None, "sold": False})
        elif etype == "LOT_SPLIT":
            for child, qty in zip(p["child_lot_refs"], p["quantities"]):
                parent_lot = self.lots.get(p["parent_lot_ref"])
                model_ref = parent_lot.model_ref if parent_lot else ""
                producer_ref = parent_lot.producer_ref if parent_lot else ""
                child_lot = self.lots.setdefault(child, _Lot(model_ref=model_ref, producer_ref=producer_ref))
                child_lot.parents.append(p["parent_lot_ref"])
                if parent_lot:
                    parent_lot.children.append(child)
            for child, unit_refs in p.get("unit_assignments", {}).items():
                for unit_ref in unit_refs:
                    data = self.units.get(unit_ref)
                    if data is not None:
                        data["lot_ref"] = child
                    else:
                        self.units[unit_ref] = {"lot_ref": child, "location": None, "sold": False}
        elif etype == "LOT_MERGED":
            target = self.lots.setdefault(p["child_lot_ref"], _Lot())
            for parent in p["parent_lot_refs"]:
                target.parents.append(parent)
                parent_lot = self.lots.get(parent)
                if parent_lot:
                    parent_lot.children.append(p["child_lot_ref"])
                    if not target.model_ref:
                        target.model_ref = parent_lot.model_ref
                        target.producer_ref = parent_lot.producer_ref
        elif etype == "LOT_FROZEN":
            lot = self.lots.get(e["aggregate_id"])
            if lot:
                lot.status = "frozen"
                lot.frozen_units = list(p.get("affected_units", []))
        elif etype == "LOT_RELEASED":
            lot = self.lots.get(e["aggregate_id"])
            if lot:
                lot.status = "released"
                lot.frozen_units = []
        elif etype == "MERCHANT_MATERIAL_SUBMITTED":
            self.materials.append(dict(p, submitted_at=e["occurred_at"]))
        elif etype == "STOCK_MOVED":
            for unit_ref in p.get("unit_refs", []):
                data = self.units.setdefault(unit_ref, {"lot_ref": p["lot_ref"], "location": None, "sold": False})
                data["location"] = p["to_location"]
        elif etype == "UNIT_SOLD":
            receipt = {
                "units": list(p["unit_refs"]),
                "shop_ref": p.get("shop_ref", ""),
                "channel": p.get("channel", ""),
                "lot_ref": p["lot_ref"],
            }
            self.receipts[p["sales_receipt_ref"]] = receipt
            for unit_ref in p["unit_refs"]:
                data = self.units.setdefault(unit_ref, {"lot_ref": p["lot_ref"], "location": None, "sold": False})
                data["sold"] = True
                data["receipt_ref"] = p["sales_receipt_ref"]
        elif etype == "TEST_RESULT_RECORDED":
            self.tests.append(
                {"event_id": e["event_id"], "lot_ref": e["aggregate_id"], "conclusion": p["conclusion"], "corrected": False}
            )
        elif etype == "TEST_RESULT_CORRECTED":
            for test in self.tests:
                if test["event_id"] == p["test_event_id"]:
                    test["corrected"] = True
            self.tests.append(
                {
                    "event_id": e["event_id"],
                    "lot_ref": e["aggregate_id"],
                    "conclusion": p["conclusion"],
                    "corrected": False,
                    "corrects": p["test_event_id"],
                }
            )
        elif etype == "RECALL_OPENED":
            self.campaigns[e["aggregate_id"]] = _Campaign(
                lot_refs=set(p["lot_refs"]),
                model_refs={self.lots[r].model_ref for r in p["lot_refs"] if r in self.lots},
                risk_level=p["risk_level"],
                basis=list(p.get("basis", [])),
            )
        elif etype == "RECALL_SCOPE_ADJUSTED":
            campaign = self.campaigns.get(e["aggregate_id"])
            if campaign:
                campaign.lot_refs = set(p["lot_refs"])
                campaign.adjustments.append({"lot_refs": list(p["lot_refs"]), "reason": p.get("reason", "")})
        elif etype == "RECALL_CLOSED":
            campaign = self.campaigns.get(e["aggregate_id"])
            if campaign:
                campaign.status = "closed"
        elif etype == "RECALL_NOTICE_SENT":
            self.notices[p["notice_ref"]] = {
                "campaign_ref": e["aggregate_id"],
                "receipts": list(p["sales_receipt_refs"]),
                "deadline": p.get("response_deadline"),
                "sent_at": e["occurred_at"],
            }
        elif etype == "CONSUMER_RESPONDED":
            self.responses.append({"notice_ref": p["notice_ref"], "unit_ref": p["unit_ref"], "choice": p["choice"]})
        elif etype == "RECALL_ESCALATED":
            campaign = self.campaigns.get(e["aggregate_id"])
            if campaign:
                campaign.risk_level = p["to_level"]
                if p.get("notice_ref"):
                    campaign.escalated_notices.add(p["notice_ref"])
        elif etype == "UNIT_DISPOSAL_REQUESTED":
            self.dispositions[p["unit_ref"]] = {
                "state": "pending",
                "disposition": p["disposition"],
                "request_ref": p.get("request_ref", e["event_id"]),
            }
        elif etype == "UNIT_DISPOSED":
            self.dispositions[p["unit_ref"]] = {
                "state": "final",
                "disposition": p["disposition"],
                "request_ref": p.get("request_ref", e["event_id"]),
            }
        elif etype == "DISPOSITION_REJECTED":
            # 不覆盖该件已有的待执行/最终去向
            self.dispositions.setdefault(
                p["unit_ref"], {"state": "rejected_only", "disposition": None, "request_ref": None}
            )

    # ----------------------------------------------------------- 型号与流通

    def register_model(
        self, actor: Actor, model_ref: str, producer_ref: str, model_name: str,
        designated_uses: Sequence[str], event_id: Optional[str] = None,
    ) -> str:
        actor.requires(ROLE_REGULATOR, ROLE_MERCHANT)
        with self._lock:
            self._append(event_id, "MODEL_REGISTERED", "helmet_model", model_ref, {
                "producer_ref": producer_ref, "model_name": model_name,
                "designated_uses": list(designated_uses),
            })
            return model_ref

    def publish_listing(
        self, actor: Actor, model_ref: str, shop_ref: str, listing_ref: str,
        promo_material_refs: Sequence[str], claimed_uses: Sequence[str],
        event_id: Optional[str] = None,
    ) -> str:
        actor.requires(ROLE_MERCHANT, ROLE_PLATFORM)
        with self._lock:
            if model_ref not in self.models:
                raise UnknownReferenceError(f"型号 {model_ref} 未登记")
            self._append(event_id, "MODEL_LISTING_PUBLISHED", "helmet_model", model_ref, {
                "model_ref": model_ref, "shop_ref": shop_ref, "listing_ref": listing_ref,
                "promo_material_refs": list(promo_material_refs), "claimed_uses": list(claimed_uses),
            })
            return listing_ref

    def delist_listing(
        self, actor: Actor, model_ref: str, listing_ref: str, reason: str,
        event_id: Optional[str] = None,
    ) -> None:
        """下架只切断新销售；不解除冻结、不替代召回。"""
        actor.requires(ROLE_PLATFORM, ROLE_REGULATOR)
        with self._lock:
            self._append(event_id, "LISTING_DELISTED", "helmet_model", model_ref, {
                "model_ref": model_ref, "listing_ref": listing_ref, "reason": reason,
            })

    def create_lot(
        self, actor: Actor, lot_ref: str, producer_ref: str, model_ref: str,
        unit_refs: Sequence[str], event_id: Optional[str] = None,
    ) -> str:
        actor.requires(ROLE_MERCHANT, ROLE_REGULATOR)
        with self._lock:
            self._append(event_id, "LOT_CREATED", "product_lot", lot_ref, {
                "lot_ref": lot_ref, "producer_ref": producer_ref, "model_ref": model_ref,
                "quantity": len(unit_refs), "unit_refs": list(unit_refs),
            })
            return lot_ref

    def split_lot(
        self, actor: Actor, parent_lot_ref: str, child_lot_refs: Sequence[str],
        quantities: Sequence[int], unit_assignments: Optional[Mapping[str, Sequence[str]]] = None,
        event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_MERCHANT, ROLE_WAREHOUSE)
        with self._lock:
            lot = self.lots.get(parent_lot_ref)
            if lot is None:
                raise UnknownReferenceError(f"批次 {parent_lot_ref} 不存在")
            if lot.status == "frozen":
                raise StateError(f"批次 {parent_lot_ref} 已冻结，禁止拆批转移")
            assignments = {k: list(v) for k, v in (unit_assignments or {}).items()}
            self._append(event_id, "LOT_SPLIT", "product_lot", parent_lot_ref, {
                "parent_lot_ref": parent_lot_ref, "child_lot_refs": list(child_lot_refs),
                "quantities": list(quantities), "unit_assignments": assignments,
            })

    def merge_lots(
        self, actor: Actor, parent_lot_refs: Sequence[str], child_lot_ref: str,
        quantity: int, event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_MERCHANT, ROLE_WAREHOUSE)
        with self._lock:
            self._append(event_id, "LOT_MERGED", "product_lot", child_lot_ref, {
                "parent_lot_refs": list(parent_lot_refs), "child_lot_ref": child_lot_ref,
                "quantity": quantity,
            })

    def move_stock(
        self, actor: Actor, lot_ref: str, unit_refs: Sequence[str],
        from_location: str, to_location: str, event_id: Optional[str] = None,
    ) -> None:
        """冻结库存不得移动。"""
        actor.requires(ROLE_WAREHOUSE, ROLE_MERCHANT, ROLE_PLATFORM)
        with self._lock:
            payload = {
                "lot_ref": lot_ref, "unit_refs": list(unit_refs),
                "from_location": from_location, "to_location": to_location,
            }
            dup = self._pre_ingest(event_id, "STOCK_MOVED", "product_lot", lot_ref, payload)
            if dup is not None:
                return
            lot = self.lots.get(lot_ref)
            if lot is None:
                raise UnknownReferenceError(f"批次 {lot_ref} 不存在")
            if lot.status == "frozen":
                raise StateError(f"批次 {lot_ref} 已冻结，库存移动被阻止")
            self._append(event_id, "STOCK_MOVED", "product_lot", lot_ref, payload)

    def record_sale(
        self, actor: Actor, lot_ref: str, unit_refs: Sequence[str],
        sales_receipt_ref: str, channel: str, shop_ref: str,
        event_id: Optional[str] = None,
    ) -> str:
        """销售回执幂等：同编号同内容忽略；同编号序列/数量/去向不同则隔离。"""
        actor.requires(ROLE_MERCHANT, ROLE_PLATFORM)
        with self._lock:
            payload = {
                "lot_ref": lot_ref, "unit_refs": list(unit_refs),
                "sales_receipt_ref": sales_receipt_ref, "channel": channel, "shop_ref": shop_ref,
            }
            dup = self._pre_ingest(event_id, "UNIT_SOLD", "product_lot", lot_ref, payload)
            if dup is not None:
                return sales_receipt_ref
            lot = self.lots.get(lot_ref)
            if lot is None:
                raise UnknownReferenceError(f"批次 {lot_ref} 不存在")
            if lot.status == "frozen":
                raise StateError(f"批次 {lot_ref} 已冻结，禁止继续销售")
            self._append(event_id, "UNIT_SOLD", "product_lot", lot_ref, payload)
            return sales_receipt_ref

    # ------------------------------------------------------------- 认证与检测

    def verify_certificate(
        self, actor: Actor, certificate_ref: str, model_ref: str,
        valid_until: str, event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_CERT_VERIFIER)
        with self._lock:
            self._append(event_id, "CERTIFICATE_VERIFIED", "certification_record", certificate_ref, {
                "certificate_ref": certificate_ref, "model_ref": model_ref, "valid_until": valid_until,
            })

    def revoke_certificate(
        self, actor: Actor, certificate_ref: str, reason: str, event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_CERT_VERIFIER, ROLE_REGULATOR)
        with self._lock:
            if certificate_ref not in self.certs:
                raise UnknownReferenceError(f"证书 {certificate_ref} 不存在")
            self._append(event_id, "CERTIFICATE_REVOKED", "certification_record", certificate_ref, {
                "certificate_ref": certificate_ref, "reason": reason,
            })

    def record_test_result(
        self, actor: Actor, lot_ref: str, sample_ref: str, conclusion: str,
        event_id: Optional[str] = None,
    ) -> str:
        actor.requires(ROLE_TESTER)
        with self._lock:
            if lot_ref not in self.lots:
                raise UnknownReferenceError(f"批次 {lot_ref} 不存在")
            if conclusion not in ("pass", "fail"):
                raise StateError("检测结论必须是 pass 或 fail")
            self._append(event_id, "TEST_RESULT_RECORDED", "product_lot", lot_ref, {
                "sample_ref": sample_ref, "conclusion": conclusion,
            })
            return event_id or self.store.events[-1]["event_id"]

    def correct_test_result(
        self, actor: Actor, lot_ref: str, test_event_id: str, sample_ref: str,
        new_conclusion: str, event_id: Optional[str] = None,
    ) -> None:
        """检测更正只追加事实并标记原结论，不删除原事件与已发通知。"""
        actor.requires(ROLE_TESTER)
        with self._lock:
            if not any(t["event_id"] == test_event_id for t in self.tests):
                raise UnknownReferenceError(f"原检测事件 {test_event_id} 不存在")
            self._append(event_id, "TEST_RESULT_CORRECTED", "product_lot", lot_ref, {
                "test_event_id": test_event_id, "sample_ref": sample_ref, "conclusion": new_conclusion,
            })

    def submit_merchant_material(
        self, actor: Actor, lot_ref: str, material_ref: str, shop_ref: str,
        summary: str, event_id: Optional[str] = None,
    ) -> None:
        """商家提交材料：仅登记在案，绝不自行解除冻结。"""
        actor.requires(ROLE_MERCHANT)
        with self._lock:
            self._append(event_id, "MERCHANT_MATERIAL_SUBMITTED", "product_lot", lot_ref, {
                "lot_ref": lot_ref, "material_ref": material_ref, "shop_ref": shop_ref, "summary": summary,
            })

    # ----------------------------------------------------------------- 召回

    def _active_fail_tests(self, lot_ref: str) -> list[dict[str, Any]]:
        return [t for t in self.tests if t["lot_ref"] == lot_ref and t["conclusion"] == "fail" and not t["corrected"]]

    def open_recall(
        self, actor: Actor, lot_refs: Sequence[str], risk_level: str,
        basis: Sequence[str], campaign_ref: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> str:
        """高风险批次：先冻结尚未售出的库存，再按谱系定位已售产品与责任主体。"""
        actor.requires(ROLE_APPROVER)
        with self._lock:
            if self._seen(event_id):
                return self._event(event_id)["aggregate_id"]
            if risk_level not in RISK_ORDER:
                raise StateError("风险等级必须是 low/medium/high")
            for ref in lot_refs:
                if ref not in self.lots:
                    raise UnknownReferenceError(f"批次 {ref} 不存在")
            if not any(self._active_fail_tests(ref) for ref in lot_refs):
                raise StateError("立案必须依据未被更正的不合格检测结论")
            campaign_ref = campaign_ref or self._new_id("recall")

            # 1) 先控制尚未售出的库存（含拆合谱系下游）
            closure: set[str] = set()
            for ref in lot_refs:
                closure |= self._descendants(ref)
            unsold_control: dict[str, list[str]] = {}
            for ref in sorted(closure):
                lot = self.lots[ref]
                if lot.status == "frozen":
                    unsold_control.setdefault(ref, list(lot.frozen_units))
                    continue
                unsold = [u for u, d in self.units.items() if d["lot_ref"] == ref and not d["sold"]]
                if unsold:
                    self._append(None, "LOT_FROZEN", "product_lot", ref, {
                        "affected_units": unsold,
                        "risk_reason": f"召回 {campaign_ref} 前置库存控制",
                    })
                    unsold_control[ref] = unsold

            # 2) 同步下架相关店铺商品（停售），不替代召回
            model_refs = {self.lots[r].model_ref for r in lot_refs if r in self.lots}
            for model_ref in sorted(model_refs):
                model = self.models.get(model_ref)
                if not model:
                    continue
                for listing_ref, listing in model.listings.items():
                    if listing["status"] == "listed":
                        self._append(None, "LISTING_DELISTED", "helmet_model", model_ref, {
                            "model_ref": model_ref, "listing_ref": listing_ref,
                            "reason": f"召回 {campaign_ref} 停售",
                        })

            # 3) 按谱系定位已售产品与责任主体（仅作为立案依据落账）
            sold_receipts = sorted({
                d["receipt_ref"]
                for d in self.units.values()
                if d["sold"] and d["lot_ref"] in closure and "receipt_ref" in d
            })
            responsible_producers = sorted({
                self.lots[r].producer_ref for r in self._roots_multi(lot_refs) if self.lots.get(r, _Lot()).producer_ref
            })
            self._append(event_id, "RECALL_OPENED", "recall_campaign", campaign_ref, {
                "lot_refs": list(lot_refs),
                "risk_level": risk_level,
                "basis": list(basis),
                "approved_by": actor.ref,
                "unsold_control": {k: v for k, v in unsold_control.items() if v},
                "sold_receipts_located": sold_receipts,
                "responsible_producers": responsible_producers,
            })
            return campaign_ref

    def _roots_multi(self, lot_refs: Sequence[str]) -> set[str]:
        roots: set[str] = set()
        for ref in lot_refs:
            roots |= self._roots(ref)
        return roots

    def adjust_scope(
        self, actor: Actor, campaign_ref: str, lot_refs: Sequence[str],
        reason: str, event_id: Optional[str] = None,
    ) -> None:
        """检测更正后调整受影响范围；移出的批次由批准人解除冻结，已发通知保留。"""
        actor.requires(ROLE_APPROVER)
        with self._lock:
            if self._seen(event_id):
                return
            campaign = self.campaigns.get(campaign_ref)
            if campaign is None:
                raise UnknownReferenceError(f"召回 {campaign_ref} 不存在")
            old_closure: set[str] = set()
            for ref in campaign.lot_refs:
                old_closure |= self._descendants(ref)
            self._append(event_id, "RECALL_SCOPE_ADJUSTED", "recall_campaign", campaign_ref, {
                "lot_refs": list(lot_refs), "reason": reason,
            })
            remaining: set[str] = set()
            for ref in lot_refs:
                remaining |= self._descendants(ref)
            for ref in sorted(old_closure - remaining):
                lot = self.lots.get(ref)
                if lot and lot.status == "frozen":
                    self._append(None, "LOT_RELEASED", "product_lot", ref, {
                        "reason": f"召回 {campaign_ref} 范围调整: {reason}",
                    })

    def send_notice(
        self, actor: Actor, campaign_ref: str, sales_receipt_refs: Sequence[str],
        notice_ref: str, response_window: timedelta = timedelta(days=15),
        channel: str = "platform", event_id: Optional[str] = None,
    ) -> datetime:
        actor.requires(ROLE_PLATFORM, ROLE_REGULATOR)
        with self._lock:
            if campaign_ref not in self.campaigns:
                raise UnknownReferenceError(f"召回 {campaign_ref} 不存在")
            for receipt in sales_receipt_refs:
                if receipt not in self.receipts:
                    raise UnknownReferenceError(f"销售回执 {receipt} 不存在")
            deadline = self.clock.now() + response_window
            self._append(event_id, "RECALL_NOTICE_SENT", "recall_campaign", campaign_ref, {
                "notice_ref": notice_ref, "channel": channel,
                "sales_receipt_refs": list(sales_receipt_refs),
                "response_deadline": deadline.isoformat(),
            })
            return deadline

    def record_consumer_response(
        self, actor: Actor, notice_ref: str, unit_ref: str, choice: str,
        event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_CONSUMER, ROLE_PLATFORM)
        with self._lock:
            campaign_ref = self.notices.get(notice_ref, {}).get("campaign_ref", "")
            payload = {"notice_ref": notice_ref, "unit_ref": unit_ref, "choice": choice}
            dup = self._pre_ingest(event_id, "CONSUMER_RESPONDED", "recall_campaign", campaign_ref, payload)
            if dup is not None:
                return
            if notice_ref not in self.notices:
                raise UnknownReferenceError(f"通知 {notice_ref} 不存在")
            if choice not in ("return", "destroy", "exchange", "none"):
                raise StateError("消费者选择必须是 return/destroy/exchange/none")
            self._append(event_id, "CONSUMER_RESPONDED", "recall_campaign",
                         self.notices[notice_ref]["campaign_ref"], payload)
            if choice in DISPOSITIONS:
                self._request_disposal(unit_ref, choice, f"req-{notice_ref}-{unit_ref}", lock_held=True)

    def request_disposal(
        self, actor: Actor, unit_ref: str, disposition: str, request_ref: str,
        event_id: Optional[str] = None,
    ) -> dict[str, Any]:
        actor.requires(ROLE_MERCHANT, ROLE_PLATFORM, ROLE_WAREHOUSE, ROLE_REGULATOR)
        with self._lock:
            return self._request_disposal(unit_ref, disposition, request_ref, event_id, lock_held=True)

    def _request_disposal(
        self, unit_ref: str, disposition: str, request_ref: str,
        event_id: Optional[str] = None, lock_held: bool = False,
    ) -> dict[str, Any]:
        """并发退货/销毁/换货：每件产品只保留一个待执行去向，竞争请求被拒绝并记录。"""
        if disposition not in DISPOSITIONS:
            raise StateError("去向必须是 return/destroy/exchange")
        if unit_ref not in self.units:
            raise UnknownReferenceError(f"产品 {unit_ref} 不存在")
        lot_ref = self.units[unit_ref]["lot_ref"]
        payload = {"unit_ref": unit_ref, "disposition": disposition, "request_ref": request_ref}
        dup = self._pre_ingest(event_id, "UNIT_DISPOSAL_REQUESTED", "product_lot", lot_ref, payload)
        if dup is not None:
            return {"outcome": "duplicate", "unit_ref": unit_ref, "disposition": disposition}
        current = self.dispositions.get(unit_ref)
        if current and current["state"] in ("pending", "final"):
            if current["disposition"] == disposition:
                return {"outcome": "duplicate", "unit_ref": unit_ref, "disposition": disposition}
            self._append(None, "DISPOSITION_REJECTED", "product_lot", lot_ref, {
                "request_ref": request_ref, "unit_ref": unit_ref,
                "reason": f"单件产品只能有一个最终去向，已有 {current['state']} 去向 {current['disposition']}",
                "rejected_disposition": disposition,
            })
            return {"outcome": "rejected", "unit_ref": unit_ref, "reason": "已有最终/待执行去向"}
        self._append(event_id, "UNIT_DISPOSAL_REQUESTED", "product_lot", lot_ref, payload)
        return {"outcome": "accepted", "unit_ref": unit_ref, "disposition": disposition}

    def confirm_disposal(
        self, actor: Actor, unit_ref: str, event_id: Optional[str] = None,
    ) -> dict[str, Any]:
        actor.requires(ROLE_APPROVER, ROLE_REGULATOR)
        with self._lock:
            current = self.dispositions.get(unit_ref)
            if not current or current["state"] not in ("pending", "final"):
                raise StateError(f"产品 {unit_ref} 没有可确认的处置请求")
            if current["state"] == "final":
                return {"outcome": "duplicate", "unit_ref": unit_ref, "disposition": current["disposition"]}
            self._append(event_id, "UNIT_DISPOSED", "product_lot", self.units[unit_ref]["lot_ref"], {
                "unit_ref": unit_ref, "disposition": current["disposition"],
                "request_ref": current["request_ref"],
            })
            return {"outcome": "final", "unit_ref": unit_ref, "disposition": current["disposition"]}

    def poll_timeouts(self, actor: Optional[Actor] = None) -> list[dict[str, Any]]:
        """按可控时钟推进：通知逾期未响应则升级；重启后调用即可继续未完成召回。"""
        if actor is not None:
            actor.requires(ROLE_REGULATOR, ROLE_PLATFORM)
        escalations: list[dict[str, Any]] = []
        with self._lock:
            now = self.clock.now()
            for notice_ref, notice in self.notices.items():
                campaign = self.campaigns.get(notice["campaign_ref"])
                if not campaign or campaign.status != "open":
                    continue
                if notice_ref in campaign.escalated_notices:
                    continue
                deadline = datetime.fromisoformat(notice["deadline"])
                if now <= deadline:
                    continue
                noticed_units = {
                    u
                    for receipt_ref in notice["receipts"]
                    for u in self.receipts.get(receipt_ref, {}).get("units", [])
                }
                responded = {r["unit_ref"] for r in self.responses if r["notice_ref"] == notice_ref}
                pending = noticed_units - responded
                if not pending:
                    continue
                current = campaign.risk_level
                target = "high" if current in ("medium", "high") else "medium"
                # 已到 high：风险等级不再变化，逾期升级的是处置措施强度
                self._append(None, "RECALL_ESCALATED", "recall_campaign", notice["campaign_ref"], {
                    "notice_ref": notice_ref, "from_level": current, "to_level": target,
                    "reason": "通知逾期未响应" if target != current else "通知逾期未响应，升级处置措施",
                    "measures_escalated": target == current,
                    "pending_units": sorted(pending),
                })
                escalations.append({"notice_ref": notice_ref, "from_level": current, "to_level": target})
            return escalations

    def close_recall(
        self, actor: Actor, campaign_ref: str, reason: str, event_id: Optional[str] = None,
    ) -> None:
        actor.requires(ROLE_APPROVER, ROLE_REGULATOR)
        with self._lock:
            if campaign_ref not in self.campaigns:
                raise UnknownReferenceError(f"召回 {campaign_ref} 不存在")
            self._append(event_id, "RECALL_CLOSED", "recall_campaign", campaign_ref, {"reason": reason})

    # ------------------------------------------------------------------ 视图

    def _campaign_coverage(self, campaign_ref: str) -> dict[str, Any]:
        campaign = self.campaigns[campaign_ref]
        closure: set[str] = set()
        for ref in campaign.lot_refs:
            closure |= self._descendants(ref)
        sold = {u: d for u, d in self.units.items() if d["sold"] and d["lot_ref"] in closure}
        controlled = sorted({
            u for ref in closure for u in self.lots[ref].frozen_units if ref in self.lots
        })
        noticed_units: set[str] = set()
        notice_by_unit: dict[str, str] = {}
        for nref, notice in self.notices.items():
            if notice["campaign_ref"] != campaign_ref:
                continue
            for receipt_ref in notice["receipts"]:
                for u in self.receipts.get(receipt_ref, {}).get("units", []):
                    noticed_units.add(u)
                    notice_by_unit[u] = nref
        responded = {r["unit_ref"] for r in self.responses
                     if self.notices.get(r["notice_ref"], {}).get("campaign_ref") == campaign_ref}
        finalized = {u for u, d in self.dispositions.items() if d["state"] == "final" and u in sold}
        overdue: set[str] = set()
        now = self.clock.now()
        for nref, notice in self.notices.items():
            if notice["campaign_ref"] != campaign_ref:
                continue
            if datetime.fromisoformat(notice["deadline"]) < now:
                for receipt_ref in notice["receipts"]:
                    overdue.update(self.receipts.get(receipt_ref, {}).get("units", []))
        overdue -= responded
        return {
            "campaign_ref": campaign_ref,
            "status": campaign.status,
            "risk_level": campaign.risk_level,
            "lot_refs": sorted(campaign.lot_refs),
            "unsold_controlled": controlled,
            "sold_units": sorted(sold),
            "sold_not_notified": sorted(set(sold) - noticed_units),
            "awaiting_response": sorted(noticed_units - responded),
            "overdue_units": sorted(overdue),
            "responded_not_finalized": sorted(responded - finalized),
            "finalized": sorted(finalized),
        }

    def regulator_view(self, actor: Actor) -> dict[str, Any]:
        """监管视角：全部召回与处置覆盖缺口，以及责任主体。"""
        actor.requires(ROLE_REGULATOR)
        with self._lock:
            campaigns = []
            for cid in sorted(self.campaigns):
                coverage = self._campaign_coverage(cid)
                roots = self._roots_multi(self.campaigns[cid].lot_refs)
                coverage["responsible_producers"] = sorted({
                    self.lots[r].producer_ref for r in roots if self.lots.get(r, _Lot()).producer_ref
                })
                coverage["basis"] = list(self.campaigns[cid].basis)
                coverage["adjustments"] = list(self.campaigns[cid].adjustments)
                campaigns.append(coverage)
            return {"quarantine": [q.event_id for q in self.store.quarantine], "campaigns": campaigns}

    def platform_view(self, actor: Actor) -> dict[str, Any]:
        """平台视角：商品下架、停售执行与全平台通知/库存，不暴露检测样品细节。"""
        actor.requires(ROLE_PLATFORM)
        with self._lock:
            return {
                "listings": [
                    {"model_ref": mid, "listing_ref": lid, "shop_ref": l["shop_ref"], "status": l["status"]}
                    for mid, m in self.models.items() for lid, l in m.listings.items()
                ],
                "notices": [
                    {"notice_ref": nref, "campaign_ref": n["campaign_ref"],
                     "receipts": n["receipts"], "deadline": n["deadline"]}
                    for nref, n in self.notices.items()
                ],
                "frozen_lots": [ref for ref, lot in self.lots.items() if lot.status == "frozen"],
            }

    def merchant_view(self, actor: Actor) -> dict[str, Any]:
        """商家视角：只看本店回执、商品与涉及本店的召回措施。"""
        actor.requires(ROLE_MERCHANT)
        with self._lock:
            shop = actor.ref
            my_receipts = {r: d for r, d in self.receipts.items() if d["shop_ref"] == shop}
            my_units = {u for d in my_receipts.values() for u in d["units"]}
            my_lots = {d["lot_ref"] for d in my_receipts.values()}
            return {
                "shop_ref": shop,
                "listings": [
                    {"model_ref": mid, "listing_ref": lid, "status": l["status"]}
                    for mid, m in self.models.items() for lid, l in m.listings.items() if l["shop_ref"] == shop
                ],
                "receipts": sorted(my_receipts),
                "lot_status": {ref: self.lots[ref].status for ref in my_lots if ref in self.lots},
                "my_dispositions": {
                    u: self.dispositions[u] for u in my_units if u in self.dispositions
                },
                "materials_on_record": [m for m in self.materials if m.get("shop_ref") == shop],
            }

    def consumer_lookup(self, unit_ref: str) -> dict[str, Any]:
        """消费者输入产品信息即得当前风险、处理方式及依据。"""
        with self._lock:
            unit = self.units.get(unit_ref)
            if unit is None:
                return {"unit_ref": unit_ref, "risk_level": "unknown",
                        "instruction": "未查询到该产品信息", "basis": []}
            lot = self.lots.get(unit["lot_ref"])
            model_ref = lot.model_ref if lot else ""
            basis: list[dict[str, str]] = []
            for cert_ref, cert in self.certs.items():
                if cert["model_ref"] == model_ref and cert["status"] == "revoked":
                    basis.append({"type": "certificate_revoked", "ref": cert_ref})
            for test in self.tests:
                if test["lot_ref"] in self._ancestors_and_self(unit["lot_ref"]) and test["conclusion"] == "fail" and not test["corrected"]:
                    basis.append({"type": "test_fail", "ref": test["event_id"]})
            instruction = "暂无需处理措施"
            risk = "none"
            campaign_ref: Optional[str] = None
            for cid, campaign in self.campaigns.items():
                closure: set[str] = set()
                for ref in campaign.lot_refs:
                    closure |= self._descendants(ref)
                if unit["lot_ref"] not in closure:
                    continue
                risk = campaign.risk_level
                campaign_ref = cid
                basis.append({"type": "recall_opened", "ref": cid})
                disp = self.dispositions.get(unit_ref)
                receipt_ref = unit.get("receipt_ref")
                notice_ref = next(
                    (nref for nref, n in self.notices.items()
                     if n["campaign_ref"] == cid and receipt_ref in n["receipts"]),
                    None,
                )
                if disp and disp["state"] == "final":
                    instruction = {"return": "按退货流程办理，已完成", "destroy": "按销毁指引处理，已完成",
                                   "exchange": "等待/办理换货，已完成"}[disp["disposition"]]
                elif disp and disp["state"] == "pending":
                    instruction = {"return": "请办理退货", "destroy": "请按指引销毁",
                                   "exchange": "请办理换货"}[disp["disposition"]]
                elif notice_ref:
                    responses = [r for r in self.responses if r["notice_ref"] == notice_ref and r["unit_ref"] == unit_ref]
                    instruction = "等待您在通知期限内选择退货、销毁或换货" if not responses else "处理中"
                    basis.append({"type": "notice_sent", "ref": notice_ref})
                else:
                    instruction = "产品在召回范围内，通知发出后按指引处理"
            return {
                "unit_ref": unit_ref, "model_ref": model_ref,
                "risk_level": risk, "instruction": instruction,
                "campaign_ref": campaign_ref, "basis": basis,
            }

    def _ancestors_and_self(self, lot_ref: str) -> set[str]:
        seen = {lot_ref}
        stack = [lot_ref]
        while stack:
            current = stack.pop()
            lot = self.lots.get(current)
            if lot:
                stack.extend(p for p in lot.parents if p not in seen)
                seen.update(lot.parents)
        return seen
