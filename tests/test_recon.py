import http.client
import json
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import recon_store  # noqa: E402
from app import ApiHandler, MaritimeSARService  # noqa: E402
from recon_api import ReconError  # noqa: E402


class ReconFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.recon = self.service.recon
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface"], 31.0, 122.0, 20, 200, 5
        )
        self.area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-100", "surface", 31.1, 122.1, 8, 1
        )

    def tearDown(self):
        self.tmp.cleanup()

    def rec(self, event_id, **kw):
        payload = {
            "client_event_id": event_id,
            "incident_id": self.incident["id"],
            "area_id": self.area["id"],
            "area_version_seen": 1,
            "asset_name": "",
            "swept_pct": 40.0,
            "contacts": 1,
            "note": "发现疑似漂浮物",
            "recorded_at": "2026-09-30T02:00:00+00:00",
        }
        payload.update(kw)
        return payload

    def assign(self):
        return self.service.assign_area("coord1", "coordinator", self.area["id"], self.asset["id"], self.asset["version"])

    def withdraw(self):
        current = [a for a in self.service.list_assets() if a["id"] == self.asset["id"]][0]
        return self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "改派", current["version"])

    def test_merge_when_area_version_matches(self):
        result = self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1")])
        self.assertEqual("completed", result["status"])
        self.assertEqual("accepted", result["results"][0]["status"])
        record = self.recon.status()["records"][0]
        self.assertEqual("merged", record["recon_status"])
        self.assertEqual(1, record["area_version_seen"])
        self.assertEqual([], record["diff_reasons"])

    def test_same_record_only_enters_once(self):
        self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1")])
        again = self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1")])
        self.assertTrue(again["idempotent"])
        self.assertTrue(again["results"][0]["idempotent"])
        other_batch = self.recon.submit_batch("field1", "field", "B-2", [self.rec("E-1")])
        self.assertEqual("duplicate", other_batch["results"][0]["status"])
        self.assertEqual(1, len(self.recon.status()["records"]))

    def test_parked_with_both_sides_when_area_reassigned(self):
        self.assign()  # 区域版本变为 2
        result = self.recon.submit_batch(
            "field1", "field", "B-1", [self.rec("E-1", area_version_seen=1, asset_name="海巡01")]
        )
        item = result["results"][0]
        self.assertEqual("pending", item["status"])
        reason = [x for x in item["reasons"] if x["code"] == "area_version_changed"][0]
        self.assertEqual(1, reason["field_value"]["area_version"])
        self.assertEqual(2, reason["current_value"]["area_version"])
        self.assertEqual("海巡01", reason["current_value"]["asset"])
        record = self.recon.status()["records"][0]
        self.assertEqual("pending_review", record["recon_status"])
        self.assertEqual(40.0, record["swept_pct"])  # 现场数据保留

    def test_parked_when_resource_released(self):
        self.assign()
        self.withdraw()  # 资源释放，区域回到 planned，版本变为 3
        result = self.recon.submit_batch(
            "field1", "field", "B-1", [self.rec("E-1", area_version_seen=2, asset_name="海巡01")]
        )
        codes = [x["code"] for x in result["results"][0]["reasons"]]
        self.assertIn("area_version_changed", codes)
        self.assertIn("resource_released", codes)
        reason = [x for x in result["results"][0]["reasons"] if x["code"] == "resource_released"][0]
        self.assertEqual("海巡01", reason["field_value"]["asset"])
        self.assertIsNone(reason["current_value"]["asset"])

    def test_parked_when_incident_closed_and_field_data_kept(self):
        self.service.complete_area("coord1", "coordinator", self.area["id"], "completed", 1)
        current = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current["version"])
        result = self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1", area_version_seen=2)])
        item = result["results"][0]
        self.assertEqual("pending", item["status"])
        codes = [x["code"] for x in item["reasons"]]
        self.assertIn("incident_closed", codes)
        self.assertIn("area_finished", codes)
        record = self.recon.status()["records"][0]
        self.assertEqual("pending_review", record["recon_status"])
        self.assertEqual("发现疑似漂浮物", record["note"])

    def test_conclusion_invalidated_when_area_version_changes(self):
        self.assign()  # 版本 2
        self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1", area_version_seen=1, asset_name="海巡01")])
        record_id = self.recon.status()["records"][0]["id"]
        reviewed = self.recon.review("coord1", "coordinator", record_id, "confirmed", "与现场核对一致")
        self.assertEqual("confirmed", reviewed["record"]["recon_status"])
        self.assertEqual(2, reviewed["review"]["area_version_at_review"])
        self.withdraw()  # 区域版本变为 3，已有结论失效
        record = self.recon.status()["records"][0]
        self.assertEqual("pending_review", record["recon_status"])
        self.assertIsNone(record["active_review"])
        self.assertEqual(1, record["stale_reviews"])
        again = self.recon.review("coord1", "coordinator", record_id, "confirmed", "改派后重新确认")
        self.assertEqual(3, again["review"]["area_version_at_review"])
        self.assertEqual("confirmed", self.recon.status()["records"][0]["recon_status"])

    def test_partial_failure_retry_only_failed_records(self):
        first = self.recon.submit_batch(
            "field1", "field", "B-9", [self.rec("E-1"), self.rec("E-2", swept_pct=150.0)]
        )
        self.assertEqual("partial_failed", first["status"])
        self.assertEqual(1, first["summary"]["accepted"])
        self.assertEqual(1, first["summary"]["failed"])
        retry = self.recon.submit_batch(
            "field1", "field", "B-9", [self.rec("E-1"), self.rec("E-2", swept_pct=55.0)]
        )
        self.assertEqual("completed", retry["status"])
        by_id = {item["client_event_id"]: item for item in retry["results"]}
        self.assertTrue(by_id["E-1"]["idempotent"])  # 已接收不重复
        self.assertEqual("accepted", by_id["E-2"]["status"])
        self.assertFalse(by_id["E-2"]["idempotent"])
        self.assertEqual(2, retry["summary"]["accepted"])
        self.assertEqual(0, retry["summary"]["failed"])
        self.assertEqual(2, len(self.recon.status()["records"]))

    def test_review_guards(self):
        self.recon.submit_batch("field1", "field", "B-1", [self.rec("E-1")])
        merged_id = self.recon.status()["records"][0]["id"]
        with self.assertRaises(ReconError) as ctx:
            self.recon.review("coord1", "coordinator", merged_id, "confirmed")
        self.assertEqual(409, ctx.exception.status)
        self.assign()
        self.recon.submit_batch("field1", "field", "B-2", [self.rec("E-2", area_version_seen=1)])
        pending_id = [r for r in self.recon.status()["records"] if r["recon_status"] == "pending_review"][0]["id"]
        with self.assertRaises(ReconError) as ctx2:
            self.recon.review("field1", "field", pending_id, "confirmed")
        self.assertEqual(403, ctx2.exception.status)
        with self.assertRaises(ReconError) as ctx3:
            self.recon.submit_batch("viewer1", "viewer", "B-3", [self.rec("E-3")])
        self.assertEqual(403, ctx3.exception.status)

    def test_legacy_data_visible_after_upgrade(self):
        db_path = Path(self.tmp.name) / "old.db"
        with mock.patch.object(recon_store, "migrate", lambda conn: 1):
            old = MaritimeSARService(db_path)  # 模拟升级前的老库：没有对账表
        incident = old.create_incident("coord1", "coordinator", "SAR-OLD", "老船号", 31.0, 122.0, 10.0, 3, "东海中心")
        old.record_clue("field1", "field", incident["id"], "legacy-1", 31.05, 122.05, 0.8, "radio")
        old.merge_offline_batch(
            "field1", "field", "old-batch",
            [{"type": "clue", "client_event_id": "legacy-2", "incident_id": incident["id"],
              "latitude": 31.06, "longitude": 122.06, "confidence": 0.6, "source": "radio"}],
        )
        upgraded = MaritimeSARService(db_path)  # 重新打开触发迁移
        status = upgraded.recon.status()
        self.assertEqual(2, status["schema_version"])
        clue_ids = {c["client_event_id"] for c in status["legacy"]["clues"]}
        self.assertIn("legacy-1", clue_ids)
        self.assertIn("legacy-2", clue_ids)
        self.assertEqual("legacy_merged", status["legacy"]["clues"][0]["recon_status"])
        batch_ids = {b["client_batch_id"] for b in status["legacy"]["offline_batches"]}
        self.assertIn("old-batch", batch_ids)
        area = upgraded.create_search_area("coord1", "coordinator", incident["id"], "A-OLD", "surface", 31.1, 122.1, 5)
        result = upgraded.recon.submit_batch(
            "field1", "field", "B-NEW",
            [{"client_event_id": "E-NEW", "incident_id": incident["id"], "area_id": area["id"],
              "area_version_seen": 1, "swept_pct": 30.0, "contacts": 0}],
        )
        self.assertEqual("completed", result["status"])


class ReconHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "http.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-HTTP", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-HTTP", "surface", 31.1, 122.1, 8, 1
        )
        ApiHandler.service = self.service
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _request(self, method, path, payload=None, role="field", user="field1"):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"X-User": user, "X-Role": role}
        body = None
        if payload is not None:
            body = json.dumps(payload)
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        conn.close()
        return resp.status, data

    def test_batch_status_and_review_endpoints(self):
        code, body = self._request("POST", "/api/recon/batch", {
            "client_batch_id": "HB-1",
            "records": [{"client_event_id": "HE-1", "incident_id": self.incident["id"],
                         "area_id": self.area["id"], "area_version_seen": 1,
                         "swept_pct": 45.0, "contacts": 0}],
        })
        self.assertEqual(201, code)
        self.assertEqual("accepted", body["results"][0]["status"])
        code, state = self._request("GET", "/api/recon/status")
        self.assertEqual(200, code)
        self.assertEqual(1, state["summary"]["merged"])
        record_id = state["records"][0]["id"]
        code, body = self._request(
            "POST", "/api/recon/review", {"record_id": record_id, "conclusion": "confirmed"},
            role="coordinator", user="coord1",
        )
        self.assertEqual(409, code)  # 已并入的记录无需复核
        code, body = self._request("POST", "/api/recon/batch", {"client_batch_id": "HB-2", "records": []}, role="viewer")
        self.assertEqual(403, code)


if __name__ == "__main__":
    unittest.main()
