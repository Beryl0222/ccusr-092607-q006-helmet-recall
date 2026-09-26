import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from helmet_recall.clock import ManualClock
from helmet_recall.contracts import validate_event
from helmet_recall.service import (
    Actor,
    Disposition,
    ForbiddenError,
    RecallService,
    Role,
)

TZ = timezone(timedelta(hours=8))
START = datetime(2026, 9, 26, 9, 0, tzinfo=TZ)
DAY = timedelta(days=1)
HOUR = timedelta(hours=1)

REGULATOR = Actor("reg-1", Role.REGULATOR)
CERTIFIER = Actor("cert-1", Role.CERTIFIER)
TESTER = Actor("test-1", Role.TESTER)
PLATFORM = Actor("plat-1", Role.PLATFORM, org_id="pf-1")
PLATFORM2 = Actor("plat-2", Role.PLATFORM, org_id="pf-2")
MERCHANT = Actor("mer-1", Role.MERCHANT, org_id="m-1")
CONSUMER = Actor("cons-1", Role.CONSUMER)


class ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.journal = str(Path(self.tmp.name) / "journal.jsonl")
        self.clock = ManualClock(START)
        self.service = RecallService(self.clock, self.journal)

    def tearDown(self) -> None:
        self.service.close()
        self.tmp.cleanup()

    def build_world(self, serials=("s1", "s2", "s3", "s4", "s5", "s6")) -> None:
        s = self.service
        s.register_producer(REGULATOR, "p-1", "某护具厂商")
        s.register_model(
            MERCHANT, "hm-1", "p-1",
            structure={"shell": "ABS", "liner": "EPS"},
            categories=["自行车", "电动车", "摩托车"],
        )
        s.submit_certificate(MERCHANT, "cert-1", "hm-1", "CERT-2026-001", START + 365 * DAY)
        s.verify_certificate(CERTIFIER, "cert-1", True)
        s.register_lot(MERCHANT, "lot-1", "hm-1", len(serials), serials=list(serials))
        s.link_shop(
            PLATFORM, "shop-1", "pf-1", "安心旗舰店",
            merchant_id="m-1", aliases=["安心老店"], model_ids=["hm-1"],
        )

    def journal_events(self, event_type=None):
        self.service.close()
        lines = [
            json.loads(x)
            for x in Path(self.journal).read_text(encoding="utf-8").splitlines()
            if x.strip()
        ]
        self.service._journal = open(self.journal, "a", encoding="utf-8")
        if event_type is None:
            return lines
        return [e for e in lines if e["event_type"] == event_type]


class RoleSeparationTests(ServiceTestCase):
    def test_duties_require_distinct_roles(self):
        self.build_world()
        s = self.service
        with self.assertRaises(ForbiddenError):
            s.verify_certificate(MERCHANT, "cert-1", True)  # 商家不能自核
        with self.assertRaises(ForbiddenError):
            s.verify_certificate(TESTER, "cert-1", True)  # 检测员不能核验
        with self.assertRaises(ForbiddenError):
            s.record_test_result(CERTIFIER, "sp-1", "hm-1", "fail")  # 核验员不能下结论
        with self.assertRaises(ForbiddenError):
            s.assess_risk(PLATFORM, "lot-1", "high", "平台不能定级")
        s.record_test_result(TESTER, "sp-1", "hm-1", "pass", lot_id="lot-1")
        s.assess_risk(REGULATOR, "lot-1", "high", "抽检无有效认证")
        with self.assertRaises(ForbiddenError):
            s.approve_campaign(TESTER, "rc-1")  # 检测员不能批准处置

    def test_merchant_material_cannot_unfreeze(self):
        self.build_world()
        s = self.service
        s.assess_risk(REGULATOR, "lot-1", "high", "抽检无有效认证")
        self.assertEqual("frozen", s.lots["lot-1"].status)
        s.submit_material(MERCHANT, "mat-1", "hm-1", "appeal", "oss://appeal/1")
        self.assertEqual("frozen", s.lots["lot-1"].status)  # 材料只存档
        with self.assertRaises(ForbiddenError):
            s.unfreeze_lot(MERCHANT, "lot-1")
        self.assertEqual("frozen", s.lots["lot-1"].status)
        s.unfreeze_lot(REGULATOR, "lot-1")
        self.assertEqual("active", s.lots["lot-1"].status)


class HighRiskFlowTests(ServiceTestCase):
    def test_freeze_unsold_first_then_trace_sold(self):
        self.build_world()
        s = self.service
        s.move_inventory(MERCHANT, "lot-1", ["s3"], "dealer", holder="d-1")
        s.record_sale(
            PLATFORM,
            {"message_id": "sale-1", "lot_id": "lot-1", "shop_id": "shop-1",
             "serials": ["s1", "s2"], "destination": "consumer-1"},
        )
        s.assess_risk(REGULATOR, "lot-1", "high", "抽检无有效认证")
        # 先控制未售库存：s3~s6 全部被冻结持有
        self.assertEqual("frozen", s.lots["lot-1"].status)
        for serial in ("s3", "s4", "s5", "s6"):
            self.assertTrue(s.units[serial].held, serial)
        self.assertFalse(s.units["s1"].held)  # 已售出的不在冻结范围
        # 再按谱系定位已售产品与责任主体
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund", "destroy"],
                        notify_by=START + DAY, respond_by=START + 2 * DAY)
        campaign = s.campaigns["rc-1"]
        self.assertEqual({f"s{i}" for i in range(1, 7)}, campaign.scope_units)
        self.assertEqual(["p-1"], campaign.responsible["producers"])
        self.assertEqual(["shop-1"], campaign.responsible["shops"])
        self.assertEqual(["pf-1"], campaign.responsible["platforms"])
        s.approve_campaign(REGULATOR, "rc-1")
        sent = s.notify_consumers(REGULATOR, "rc-1")
        self.assertEqual(2, len(sent))  # 只通知已售出的 s1、s2
        self.assertEqual({"s1", "s2"}, campaign.notified)

    def test_genealogy_covers_split_and_merge(self):
        self.build_world()
        s = self.service
        s.split_lot(MERCHANT, "lot-1", [
            {"lot_id": "lot-2", "quantity": 3, "serials": ["s1", "s2", "s3"]},
            {"lot_id": "lot-3", "quantity": 3, "serials": ["s4", "s5", "s6"]},
        ])
        s.merge_lots(MERCHANT, "lot-4", ["lot-2", "lot-3"])
        s.assess_risk(REGULATOR, "lot-1", "high", "源头批次不合格")
        # 拆分、合并后的下游批次一并受控
        self.assertEqual("frozen", s.lots["lot-4"].status)
        for serial in ("s1", "s4", "s6"):
            self.assertTrue(s.units[serial].held, serial)
            self.assertEqual("lot-4", s.units[serial].lot_id)

    def test_shop_rename_keeps_traceability(self):
        self.build_world()
        s = self.service
        s.link_shop(PLATFORM, "shop-1", "pf-1", "安心优选店",
                    merchant_id="m-1", model_ids=["hm-1"])
        shop = s.shops["shop-1"]
        self.assertEqual("安心优选店", shop.name)
        self.assertIn("安心旗舰店", shop.aliases)
        self.assertIn("安心老店", shop.aliases)
        result = s.consumer_query(shop_name="安心旗舰店")
        self.assertTrue(result["found"])
        self.assertEqual("hm-1", result["model_id"])


class CorrectionTests(ServiceTestCase):
    def test_correction_adjusts_scope_but_keeps_notifications(self):
        self.build_world()
        s = self.service
        s.record_sale(
            PLATFORM,
            {"message_id": "sale-1", "lot_id": "lot-1", "shop_id": "shop-1",
             "serials": ["s1"], "destination": "consumer-1"},
        )
        s.record_test_result(TESTER, "sp-1", "hm-1", "fail", lot_id="lot-1")
        self.assertEqual("frozen", s.lots["lot-1"].status)  # fail 自动触发控制
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund"])
        s.approve_campaign(REGULATOR, "rc-1")
        s.notify_consumers(REGULATOR, "rc-1")
        self.assertEqual(1, len(s.notifications))
        before = s.consumer_query(serial="s1")
        self.assertEqual("high", before["risk"])
        # 检测更正：原结论作废，范围收缩
        s.record_test_result(TESTER, "sp-2", "hm-1", "pass", lot_id="lot-1",
                             corrects="sp-1")
        self.assertEqual(1, len(self.journal_events("SCOPE_ADJUSTED")))
        campaign = s.campaigns["rc-1"]
        self.assertEqual(set(), campaign.scope_units)
        # 曾经执行的通知不撤销
        self.assertEqual(1, len(s.notifications))
        after = s.consumer_query(serial="s1")
        self.assertEqual("low", after["risk"])
        self.assertIn("notification", {b["type"] for b in after["basis"]})
        # 冻结仍需监管解冻，不由更正自动解除
        self.assertEqual("frozen", s.lots["lot-1"].status)


class IdempotencyTests(ServiceTestCase):
    def test_sale_and_return_messages_are_idempotent(self):
        self.build_world()
        s = self.service
        message = {"message_id": "sale-1", "lot_id": "lot-1", "shop_id": "shop-1",
                   "serials": ["s1"], "destination": "consumer-1"}
        first = s.record_sale(PLATFORM, message)
        again = s.record_sale(PLATFORM, message)
        self.assertEqual("recorded", first["status"])
        self.assertEqual("duplicate", again["status"])
        self.assertEqual(1, len(self.journal_events("SALE_RECORDED")))
        # 编号相同而序列不同：隔离，不入账
        conflict = s.record_sale(PLATFORM, dict(message, serials=["s2"]))
        self.assertEqual("quarantined", conflict["status"])
        self.assertIsNone(s.units["s2"].consumer_ref)
        self.assertEqual(1, len(s.quarantine))
        # 退回消息同样幂等
        ret = {"message_id": "ret-1", "serials": ["s1"]}
        self.assertEqual("recorded", s.record_return(PLATFORM, ret)["status"])
        self.assertEqual("duplicate", s.record_return(PLATFORM, ret)["status"])
        self.assertEqual(1, len(self.journal_events("RETURN_RECORDED")))
        self.assertEqual(Disposition.RETURNED, s.units["s1"].disposition)
        conflict = s.record_return(PLATFORM, {"message_id": "ret-1", "serials": ["s3"]})
        self.assertEqual("quarantined", conflict["status"])
        self.assertEqual(2, len(s.quarantine))

    def test_quantity_or_destination_mismatch_is_quarantined(self):
        self.build_world()
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-9", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "quantity": 2,
                                 "destination": "consumer-9"})
        changed = s.record_sale(PLATFORM, {"message_id": "sale-9", "lot_id": "lot-1",
                                           "shop_id": "shop-1", "quantity": 3,
                                           "destination": "consumer-9"})
        self.assertEqual("quarantined", changed["status"])
        changed = s.record_sale(PLATFORM, {"message_id": "sale-9", "lot_id": "lot-1",
                                           "shop_id": "shop-1", "quantity": 2,
                                           "destination": "consumer-10"})
        self.assertEqual("quarantined", changed["status"])
        self.assertEqual(2, s.lots["lot-1"].sold_anonymous)  # 只有首次入账


class ConcurrencyTests(ServiceTestCase):
    def test_concurrent_dispositions_leave_single_final_outcome(self):
        serials = [f"u{i}" for i in range(10)]
        self.build_world(serials=tuple(serials))
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": serials,
                                 "destination": "consumer-1"})

        def attempt(args):
            serial, action = args
            return s.dispose(PLATFORM, [serial], action)[0]["outcome"]

        tasks = [(serial, action) for serial in serials
                 for action in ("return", "destroy", "exchange")]
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(attempt, tasks))
        self.assertEqual(10, outcomes.count("applied"))
        self.assertEqual(20, outcomes.count("conflict"))
        self.assertEqual(0, outcomes.count("duplicate"))
        for serial in serials:
            self.assertIsNotNone(s.units[serial].disposition)
        self.assertEqual(10, len(self.journal_events("UNIT_DISPOSED")))
        self.assertEqual(20, len(self.journal_events("DISPOSITION_CONFLICT")))
        # 相同去向重放是幂等的
        action_of = {"returned": "return", "destroyed": "destroy", "exchanged": "exchange"}
        again = s.dispose(PLATFORM, [serials[0]], action_of[s.units[serials[0]].disposition.value])[0]
        self.assertEqual("duplicate", again["outcome"])


class ClockAndRestartTests(ServiceTestCase):
    def test_stop_sale_and_overdue_escalation(self):
        self.build_world()
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": ["s1"],
                                 "destination": "consumer-1"})
        s.assess_risk(REGULATOR, "lot-1", "high", "抽检无有效认证")
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund"],
                        stop_sale_at=START + HOUR,
                        notify_by=START + DAY,
                        respond_by=START + 2 * DAY)
        s.approve_campaign(REGULATOR, "rc-1")
        s.tick()
        self.assertEqual([], s.escalations)
        self.clock.advance(2 * HOUR)  # 停售时点已过
        s.tick()
        self.assertTrue(s.campaigns["rc-1"].stop_sale_enforced)
        self.assertEqual([], s.escalations)
        self.clock.advance(DAY)  # 通知逾期
        s.tick()
        self.assertEqual(["notify_overdue"], [e["stage"] for e in s.escalations])
        s.notify_consumers(REGULATOR, "rc-1")
        self.clock.advance(DAY)  # 响应逾期
        s.tick()
        self.assertIn("response_overdue", [e["stage"] for e in s.escalations])

    def test_certificate_expires_on_tick(self):
        self.build_world()
        s = self.service
        s.submit_certificate(MERCHANT, "cert-2", "hm-1", "CERT-2026-002", START + DAY)
        s.verify_certificate(CERTIFIER, "cert-2", True)
        self.clock.advance(2 * DAY)
        s.tick()
        self.assertEqual("expired", s.certificates["cert-2"].status)
        self.assertEqual("verified", s.certificates["cert-1"].status)

    def test_restart_resumes_unfinished_recall(self):
        self.build_world()
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": ["s1"],
                                 "destination": "consumer-1"})
        s.assess_risk(REGULATOR, "lot-1", "high", "抽检无有效认证")
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund"],
                        notify_by=START + DAY, respond_by=START + 2 * DAY)
        s.approve_campaign(REGULATOR, "rc-1")
        s.close()
        # 服务重启：重放日志，时钟已越过通知期限
        clock2 = ManualClock(START + 2 * DAY)
        resumed = RecallService(clock2, self.journal)
        try:
            self.assertTrue(resumed.campaigns["rc-1"].approved)
            self.assertEqual("frozen", resumed.lots["lot-1"].status)
            resumed.tick()
            self.assertEqual(["notify_overdue"], [e["stage"] for e in resumed.escalations])
            sent = resumed.notify_consumers(REGULATOR, "rc-1")  # 重启后仍可继续通知
            self.assertEqual(1, len(sent))
        finally:
            resumed.close()


class ViewTests(ServiceTestCase):
    def setUp(self):
        super().setUp()
        self.build_world()
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": ["s1", "s2"],
                                 "destination": "consumer-1"})
        s.record_test_result(TESTER, "sp-1", "hm-1", "fail", lot_id="lot-1")
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund", "destroy"],
                        respond_by=START + 2 * DAY)
        s.approve_campaign(REGULATOR, "rc-1")
        s.notify_consumers(REGULATOR, "rc-1")
        s.record_consumer_response(CONSUMER, "rc-1", "s1", "return")

    def test_regulator_sees_coverage_gap(self):
        view = self.service.regulator_coverage(REGULATOR)
        (campaign,) = view["campaigns"]
        self.assertEqual(6, campaign["scope"])
        self.assertEqual(2, campaign["notified"])
        self.assertEqual(1, campaign["disposed"])
        self.assertEqual(["s2", "s3", "s4", "s5", "s6"], campaign["coverage_gap"])
        self.assertEqual(["lot-1"], view["frozen_lots"])
        with self.assertRaises(ForbiddenError):
            self.service.regulator_coverage(PLATFORM)

    def test_platform_and_merchant_views_are_scoped(self):
        view = self.service.platform_view(PLATFORM)
        self.assertEqual(["shop-1"], [s["shop_id"] for s in view["shops"]])
        self.assertEqual({"s1", "s2"}, {x["serial"] for x in view["sales"]})
        self.assertEqual(["rc-1"], [r["campaign_id"] for r in view["recalls"]])
        empty = self.service.platform_view(PLATFORM2)
        self.assertEqual([], empty["shops"])
        self.assertEqual([], empty["sales"])
        merchant = self.service.merchant_view(MERCHANT)
        self.assertEqual(["hm-1"], merchant["model_ids"])
        self.assertEqual("verified", merchant["certificates"][0]["status"])
        self.assertEqual(["rc-1"], [r["campaign_id"] for r in merchant["recalls"]])
        with self.assertRaises(ForbiddenError):
            self.service.merchant_view(PLATFORM)

    def test_consumer_query_returns_risk_handling_and_basis(self):
        result = self.service.consumer_query(serial="s2")
        self.assertTrue(result["found"])
        self.assertEqual("high", result["risk"])
        self.assertIn("退货退款", result["handling"])
        self.assertIn("2026-09-28", result["handling"])  # 响应期限
        kinds = {b["type"] for b in result["basis"]}
        self.assertIn("certificate", kinds)
        self.assertIn("test", kinds)
        self.assertIn("recall", kinds)
        self.assertIn("notification", kinds)
        done = self.service.consumer_query(serial="s1")
        self.assertIn("退货", done["handling"])
        unknown = self.service.consumer_query(serial="nope")
        self.assertFalse(unknown["found"])
        self.assertEqual("unknown", unknown["risk"])


class ContractConformanceTests(ServiceTestCase):
    def test_journal_lines_validate_against_contract(self):
        self.build_world()
        s = self.service
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": ["s1"],
                                 "destination": "consumer-1"})
        s.record_test_result(TESTER, "sp-1", "hm-1", "fail", lot_id="lot-1")
        s.launch_recall(REGULATOR, "rc-1", ["lot-1"], ["refund"],
                        stop_sale_at=START + HOUR, notify_by=START + DAY)
        s.approve_campaign(REGULATOR, "rc-1")
        s.notify_consumers(REGULATOR, "rc-1")
        s.record_consumer_response(CONSUMER, "rc-1", "s1", "return")
        s.dispose(PLATFORM, ["s2"], "destroy")
        s.record_return(PLATFORM, {"message_id": "ret-1", "serials": ["s3"]})
        s.record_sale(PLATFORM, {"message_id": "sale-1", "lot_id": "lot-1",
                                 "shop_id": "shop-1", "serials": ["s9"],
                                 "destination": "consumer-2"})
        self.clock.advance(2 * DAY)
        s.tick()
        schema = json.loads((ROOT / "contracts/domain.schema.json").read_text(encoding="utf-8"))
        events = self.journal_events()
        self.assertGreater(len(events), 15)
        for event in events:
            self.assertEqual([], validate_event(event, schema), event["event_type"])


if __name__ == "__main__":
    unittest.main()
