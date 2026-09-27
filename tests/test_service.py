import json
import sys
import threading
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from helmet_recall.clock import ControlledClock
from helmet_recall.errors import (
    AuthorizationError,
    ConflictingMessageError,
    StateError,
)
from helmet_recall.service import (
    Actor,
    RecallService,
    ROLE_APPROVER,
    ROLE_CERT_VERIFIER,
    ROLE_CONSUMER,
    ROLE_MERCHANT,
    ROLE_PLATFORM,
    ROLE_REGULATOR,
    ROLE_TESTER,
    ROLE_WAREHOUSE,
)
from helmet_recall.store import EventStore

REG = Actor(ROLE_REGULATOR, "reg-001")
VERIFIER = Actor(ROLE_CERT_VERIFIER, "cert-001")
TESTER = Actor(ROLE_TESTER, "lab-001")
APPROVER = Actor(ROLE_APPROVER, "boss-001")
PLATFORM = Actor(ROLE_PLATFORM, "platform")
MERCHANT_A = Actor(ROLE_MERCHANT, "shop-A")
MERCHANT_B = Actor(ROLE_MERCHANT, "shop-B")
WAREHOUSE = Actor(ROLE_WAREHOUSE, "wh-001")
CONSUMER = Actor(ROLE_CONSUMER, "consumer-001")


class RecallScenario:
    """构造一个含拆批、多店铺、已售/未售库存的标准场景。"""

    def __init__(self, persist_path: str | None = None) -> None:
        self.clock = ControlledClock()
        store = None
        if persist_path is not None:
            schema = json.loads((ROOT / "contracts/domain.schema.json").read_text(encoding="utf-8"))
            store = EventStore(schema, persist_path)
        self.svc = RecallService(self.clock, store)
        self.svc.register_model(REG, "M1", "P1", "折叠盔F1", ["bicycle", "ebike", "motorcycle"])
        self.svc.verify_certificate(VERIFIER, "C1", "M1", "2027-09-25T00:00:00+08:00")
        self.svc.publish_listing(MERCHANT_A, "M1", "shop-A", "list-A", ["mat-1"], ["motorcycle"])
        self.svc.publish_listing(MERCHANT_B, "M1", "shop-B", "list-B", ["mat-2"], ["motorcycle"])
        # 原始批次 4 件，拆给两个店铺批次
        self.svc.create_lot(MERCHANT_A, "L1", "P1", "M1", ["u1", "u2", "u3", "u4"])
        self.svc.split_lot(
            MERCHANT_A, "L1", ["L1a", "L1b"], [2, 2],
            {"L1a": ["u1", "u2"], "L1b": ["u3", "u4"]},
        )
        self.svc.move_stock(WAREHOUSE, "L1a", ["u1", "u2"], "factory", "shop-A")
        # L1a 两件已售，L1b 两件未售
        self.svc.record_sale(MERCHANT_A, "L1a", ["u1"], "R1", "livestream", "shop-A", "sale-1")
        self.svc.record_sale(MERCHANT_A, "L1a", ["u2"], "R2", "offline", "shop-A", "sale-2")


class RoleSeparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s = RecallScenario()

    def test_cert_test_approval_are_distinct_roles(self) -> None:
        with self.assertRaises(AuthorizationError):
            self.s.svc.verify_certificate(TESTER, "CX", "M1", "2027-01-01T00:00:00+08:00")
        with self.assertRaises(AuthorizationError):
            self.s.svc.record_test_result(MERCHANT_A, "L1", "s1", "fail")
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        with self.assertRaises(AuthorizationError):
            self.s.svc.open_recall(MERCHANT_A, ["L1"], "high", ["test-1"])

    def test_merchant_material_cannot_unfreeze(self) -> None:
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        self.assertEqual("frozen", self.s.svc.lots["L1b"].status)
        # 商家反复提交“新认证”材料：只登记，不解冻
        self.s.svc.submit_merchant_material(MERCHANT_A, "L1b", "doc-9", "shop-A", "厂家新证书")
        self.s.svc.submit_merchant_material(MERCHANT_B, "L1b", "doc-10", "shop-B", "情况说明")
        self.assertEqual("frozen", self.s.svc.lots["L1b"].status)
        with self.assertRaises(StateError):
            self.s.svc.move_stock(WAREHOUSE, "L1b", ["u3"], "factory", "shop-B")
        # 只有批准人可以解除冻结
        with self.assertRaises(AuthorizationError):
            self.s.svc.adjust_scope(MERCHANT_A, "RC1", [], "商家自称合格")


class RecallFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s = RecallScenario()

    def test_high_risk_controls_unsold_then_traces_sold_and_responsibility(self) -> None:
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        types = [e["event_type"] for e in self.s.svc.store.events]
        campaign = self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        self.assertEqual("RC1", campaign)
        types = [e["event_type"] for e in self.s.svc.store.events]
        # 先冻结未售库存，再立案
        self.assertLess(types.index("LOT_FROZEN"), types.index("RECALL_OPENED"))
        self.assertEqual("frozen", self.s.svc.lots["L1b"].status)
        self.assertEqual(["u3", "u4"], self.s.svc.lots["L1b"].frozen_units)
        # 已售批次不被冻结，按谱系经回执定位
        self.assertNotEqual("frozen", self.s.svc.lots["L1a"].status)
        opened = next(e for e in self.s.svc.store.events if e["event_type"] == "RECALL_OPENED")["payload"]
        self.assertEqual(["R1", "R2"], opened["sold_receipts_located"])
        self.assertEqual(["P1"], opened["responsible_producers"])
        # 停售同步发生，且冻结后无法继续销售或转移
        self.assertEqual("delisted", self.s.svc.models["M1"].listings["list-A"]["status"])
        with self.assertRaises(StateError):
            self.s.svc.record_sale(MERCHANT_B, "L1b", ["u3"], "R3", "shop", "shop-B")
        # 监管视角能看到“已售未通知”的覆盖缺口
        view = self.s.svc.regulator_view(REG)["campaigns"][0]
        self.assertEqual(["u1", "u2"], view["sold_not_notified"])
        self.assertEqual(["u3", "u4"], view["unsold_controlled"])

    def test_delist_alone_is_not_recall_and_keeps_lot_active(self) -> None:
        self.s.svc.delist_listing(PLATFORM, "M1", "list-A", "抽检无认证，平台下架")
        self.assertEqual("active", self.s.svc.lots["L1a"].status)
        self.assertEqual([], [c for c in self.s.svc.campaigns])


class IdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s = RecallScenario()

    def test_duplicate_sale_is_idempotent(self) -> None:
        self.s.svc.record_sale(MERCHANT_B, "L1b", ["u3"], "R9", "shop", "shop-B", "sale-9")
        self.assertEqual(1, len([e for e in self.s.svc.store.events if e["event_id"] == "sale-9"]))
        # 同编号同内容重复投递：幂等忽略（即便随后批次被冻结）
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        mid = len(self.s.svc.store.events)
        self.s.svc.record_sale(MERCHANT_B, "L1b", ["u3"], "R9", "shop", "shop-B", "sale-9")
        self.assertEqual(mid, len(self.s.svc.store.events))
        self.assertEqual(1, len([e for e in self.s.svc.store.events if e["event_id"] == "sale-9"]))

    def test_same_id_different_units_is_quarantined(self) -> None:
        self.s.svc.record_sale(MERCHANT_B, "L1b", ["u3"], "R9", "shop", "shop-B", "sale-9")
        # 编号相同但序列不同：隔离，不覆盖
        with self.assertRaises(ConflictingMessageError):
            self.s.svc.record_sale(MERCHANT_B, "L1b", ["u4"], "R9", "shop", "shop-B", "sale-9")
        quarantined = self.s.svc.store.quarantine
        self.assertEqual(1, len(quarantined))
        self.assertEqual("sale-9", quarantined[0].event_id)
        self.assertEqual(["u3"], quarantined[0].existing["payload"]["unit_refs"])
        self.assertEqual(["u4"], quarantined[0].incoming["payload"]["unit_refs"])
        # 原始事实未被改写
        self.assertEqual(["u3"], self.s.svc.receipts["R9"]["units"])

    def test_duplicate_return_message_is_idempotent(self) -> None:
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        self.s.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")
        self.s.svc.record_consumer_response(CONSUMER, "N1", "u1", "return", "resp-1")
        self.s.svc.record_consumer_response(CONSUMER, "N1", "u1", "return", "resp-1")
        self.assertEqual(1, len([e for e in self.s.svc.store.events if e["event_id"] == "resp-1"]))


class DispositionConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s = RecallScenario()
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        self.s.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")

    def test_each_unit_has_single_final_disposition_under_race(self) -> None:
        barrier = threading.Barrier(2)
        outcomes: list[dict] = []

        def ask(actor: Actor, disposition: str, ref: str) -> None:
            barrier.wait()
            outcomes.append(self.s.svc.request_disposal(actor, "u1", disposition, ref))

        t1 = threading.Thread(target=ask, args=(MERCHANT_A, "return", "req-ret"))
        t2 = threading.Thread(target=ask, args=(WAREHOUSE, "destroy", "req-des"))
        t1.start(); t2.start(); t1.join(); t2.join()

        result = {o["outcome"] for o in outcomes}
        self.assertEqual({"accepted", "rejected"}, result)
        current = self.s.svc.dispositions["u1"]
        self.assertIn(current["state"], ("pending", "final"))
        # 最终确认只能落到唯一去向；确认后再来换货仍被拒绝
        confirmed = self.s.svc.confirm_disposal(APPROVER, "u1")
        self.assertEqual("final", confirmed["outcome"])
        again = self.s.svc.request_disposal(MERCHANT_A, "u1", "exchange", "req-exc")
        self.assertEqual("rejected", again["outcome"])
        # 重复确认幂等
        duplicate = self.s.svc.confirm_disposal(APPROVER, "u1")
        self.assertEqual("duplicate", duplicate["outcome"])
        rejected_types = [e for e in self.s.svc.store.events if e["event_type"] == "DISPOSITION_REJECTED"]
        self.assertTrue(any(e["payload"]["rejected_disposition"] in ("destroy", "exchange") for e in rejected_types))


class ClockAndRestartTests(unittest.TestCase):
    def test_notice_deadline_and_overdue_escalation(self) -> None:
        s = RecallScenario()
        s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        s.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")                    # u1 不响应
        s.svc.send_notice(PLATFORM, "RC1", ["R2"], "N2")                    # u2 已响应
        s.svc.record_consumer_response(CONSUMER, "N2", "u2", "return", "resp-u2")
        self.assertEqual([], s.svc.poll_timeouts(PLATFORM))
        s.clock.advance(timedelta(days=16))
        escalations = s.svc.poll_timeouts(PLATFORM)
        self.assertEqual(1, len(escalations))
        self.assertEqual("N1", escalations[0]["notice_ref"])
        # 再次轮询不重复升级
        self.assertEqual([], s.svc.poll_timeouts(PLATFORM))
        view = s.svc.regulator_view(REG)["campaigns"][0]
        self.assertEqual(["u1"], view["overdue_units"])
        self.assertNotIn("u2", view["overdue_units"])

    def test_restart_resumes_unfinished_recall_and_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "events.jsonl")
            s1 = RecallScenario(path)
            s1.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
            s1.svc.record_sale(MERCHANT_B, "L1b", ["u3"], "R9", "shop", "shop-B", "sale-9")
            s1.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
            s1.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")
            # 冻结后仍到达的同号异内容退回消息：不能放过，进隔离
            with self.assertRaises(ConflictingMessageError):
                s1.svc.record_sale(MERCHANT_B, "L1b", ["u4"], "R9", "shop", "shop-B", "sale-9")
            advanced = s1.clock.now() + timedelta(days=20)

            # 模拟服务重启：事件流与隔离账本从磁盘重放，时钟推进到 20 天后
            clock2 = ControlledClock(start=advanced)
            schema = json.loads((ROOT / "contracts/domain.schema.json").read_text(encoding="utf-8"))
            s2 = RecallService(clock2, EventStore(schema, path))
            self.assertEqual("frozen", s2.lots["L1b"].status)
            self.assertEqual(1, len(s2.store.quarantine))
            self.assertEqual("RC1", s2.regulator_view(REG)["campaigns"][0]["campaign_ref"])
            # 未完成召回继续推进：逾期升级被补执行
            escalations = s2.poll_timeouts(REG)
            self.assertEqual("N1", escalations[0]["notice_ref"])
            self.assertEqual("high", s2.campaigns["RC1"].risk_level)


class CorrectionTests(unittest.TestCase):
    def test_correction_adjusts_scope_but_keeps_notice_history(self) -> None:
        s = RecallScenario()
        s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        s.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")
        notice_event = next(e for e in s.svc.store.events if e["event_type"] == "RECALL_NOTICE_SENT")

        # 检测更正：原结论标记，原事件保留；范围收空后批准人解除冻结
        s.svc.correct_test_result(TESTER, "L1", "test-1", "sample-1b", "pass", "test-2")
        s.svc.adjust_scope(APPROVER, "RC1", [], "复检合格，撤除召回范围")
        self.assertEqual("released", s.svc.lots["L1b"].status)
        # 已执行的通知不能抹去
        self.assertIn(notice_event, list(s.svc.store.events))
        self.assertTrue(any(t["event_id"] == "test-1" and t["corrected"] for t in s.svc.tests))
        # 范围调整可追溯；消费者查询不再给出召回措施
        view = s.svc.regulator_view(REG)["campaigns"][0]
        self.assertEqual("复检合格，撤除召回范围", view["adjustments"][0]["reason"])
        lookup = s.svc.consumer_lookup("u1")
        self.assertEqual("none", lookup["risk_level"])
        self.assertNotIn("recall_opened", [b["type"] for b in lookup["basis"]])
        # 解冻后库存可重新流通
        s.svc.move_stock(WAREHOUSE, "L1b", ["u3"], "factory", "shop-B")


class ViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s = RecallScenario()
        self.s.svc.record_test_result(TESTER, "L1", "sample-1", "fail", "test-1")
        self.s.svc.open_recall(APPROVER, ["L1"], "high", ["test-1"], campaign_ref="RC1")
        self.s.svc.send_notice(PLATFORM, "RC1", ["R1"], "N1")

    def test_merchant_and_platform_see_only_their_scope(self) -> None:
        view_a = self.s.svc.merchant_view(MERCHANT_A)
        self.assertEqual(["R1", "R2"], view_a["receipts"])
        view_b = self.s.svc.merchant_view(MERCHANT_B)
        self.assertEqual([], view_b["receipts"])
        self.assertNotIn("sample-1", json.dumps(self.s.svc.platform_view(PLATFORM), ensure_ascii=False))
        with self.assertRaises(AuthorizationError):
            self.s.svc.merchant_view(REG)
        with self.assertRaises(AuthorizationError):
            self.s.svc.platform_view(MERCHANT_A)

    def test_consumer_lookup_reports_risk_instruction_and_basis(self) -> None:
        # u1 尚未响应：等待选择
        lookup = self.s.svc.consumer_lookup("u1")
        self.assertEqual("high", lookup["risk_level"])
        self.assertIn("退货", lookup["instruction"])
        self.assertIn({"type": "recall_opened", "ref": "RC1"}, lookup["basis"])
        self.assertIn({"type": "notice_sent", "ref": "N1"}, lookup["basis"])
        # 响应并确认销毁后，处理方式变为已完成
        self.s.svc.record_consumer_response(CONSUMER, "N1", "u1", "destroy", "resp-u1")
        self.s.svc.confirm_disposal(APPROVER, "u1")
        lookup = self.s.svc.consumer_lookup("u1")
        self.assertIn("已完成", lookup["instruction"])
        unknown = self.s.svc.consumer_lookup("ghost")
        self.assertEqual("unknown", unknown["risk_level"])

    def test_revoked_certificate_is_part_of_basis(self) -> None:
        self.s.svc.revoke_certificate(VERIFIER, "C1", "抽检发现认证无效")
        lookup = self.s.svc.consumer_lookup("u1")
        self.assertIn({"type": "certificate_revoked", "ref": "C1"}, lookup["basis"])


if __name__ == "__main__":
    unittest.main()
