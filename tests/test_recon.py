import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402

LEGACY_SCHEMA = """
CREATE TABLE incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    vessel_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    uncertainty_km REAL NOT NULL,
    drift_direction REAL NOT NULL DEFAULT 0,
    drift_speed_kn REAL NOT NULL DEFAULT 0,
    sea_state INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'reported',
    lead_org TEXT NOT NULL,
    duplicate_of INTEGER REFERENCES incidents(id),
    version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    speed_kn REAL NOT NULL,
    range_km REAL NOT NULL,
    max_sea_state INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);
CREATE TABLE search_areas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL REFERENCES incidents(id),
    code TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    center_lat REAL NOT NULL,
    center_lon REAL NOT NULL,
    radius_km REAL NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'planned',
    assigned_asset_id INTEGER REFERENCES assets(id),
    note TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE clues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL REFERENCES incidents(id),
    area_id INTEGER REFERENCES search_areas(id),
    client_event_id TEXT NOT NULL UNIQUE,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    confidence REAL NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unverified',
    distance_from_incident_km REAL NOT NULL,
    reporter TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL,
    merged_at TEXT
);
CREATE TABLE offline_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_batch_id TEXT NOT NULL UNIQUE,
    actor TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at TEXT NOT NULL,
    merged_at TEXT,
    summary TEXT NOT NULL
);
CREATE TABLE timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER REFERENCES incidents(id),
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class ReconFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface"], 31.0, 122.0, 20, 200, 5
        )
        self.asset2 = self.service.add_asset(
            "coord1", "coordinator", "海巡02", "vessel", ["surface"], 31.0, 122.0, 20, 200, 5
        )
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-100", "surface", 31.1, 122.1, 8, 1
        )
        self.area = self.service.assign_area(
            "coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"]
        )

    def tearDown(self):
        self.tmp.cleanup()

    def sweep_event(self, event_id, **override):
        event = {
            "type": "sweep",
            "client_event_id": event_id,
            "incident_id": self.incident["id"],
            "area_id": self.area["id"],
            "area_version": self.area["version"],
            "asset_id": self.asset["id"],
            "coverage_pct": 30.0,
            "swept_at": "2026-09-29T08:00:00+00:00",
            "notes": "目视扫测",
        }
        event.update(override)
        return event

    def area_now(self):
        return [a for a in self.service.state()["search_areas"] if a["id"] == self.area["id"]][0]

    def recon_items(self, **filters):
        return self.service.list_recon_items(**filters)

    def test_sweep_merges_once_and_carries_area_version(self):
        batch = self.service.merge_offline_batch("field1", "field", "b-1", [self.sweep_event("sw-1")])
        self.assertEqual("merged", batch["status"])
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertEqual("merged", batch["summary"]["events"][0]["recon_status"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])
        sweep = self.service.state()["sweep_records"][0]
        self.assertEqual(self.area["version"], sweep["area_version"])

        replay = self.service.merge_offline_batch("field1", "field", "b-1", [self.sweep_event("sw-1")])
        self.assertTrue(replay["idempotent"])
        self.assertEqual("duplicate", replay["summary"]["events"][0]["status"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])

        other_batch = self.service.merge_offline_batch("field1", "field", "b-2", [self.sweep_event("sw-1")])
        self.assertEqual("duplicate", other_batch["summary"]["events"][0]["status"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])
        self.assertEqual(1, len(self.service.state()["sweep_records"]))

    def test_late_sweep_after_reassign_parks_for_review(self):
        self.service.reassign_area("coord1", "coordinator", self.area["id"], self.asset2["id"], reason="改派")
        batch = self.service.merge_offline_batch("field1", "field", "b-3", [self.sweep_event("sw-2")])
        event = batch["summary"]["events"][0]
        self.assertEqual("accepted", event["status"])
        self.assertEqual("pending_review", event["recon_status"])

        item = self.recon_items()[0]
        self.assertEqual("pending_review", item["status"])
        codes = [r["code"] for r in item["reasons"]]
        self.assertIn("area_reassigned", codes)
        self.assertIn("area_version_mismatch", codes)
        reassigned = [r for r in item["reasons"] if r["code"] == "area_reassigned"][0]
        self.assertEqual(self.asset["id"], reassigned["recorded"])
        self.assertEqual(self.asset2["id"], reassigned["current"])
        version_diff = [r for r in item["reasons"] if r["code"] == "area_version_mismatch"][0]
        self.assertEqual(self.area["version"], version_diff["recorded"])
        self.assertEqual(self.area_now()["version"], version_diff["current"])

        # 现场数据保留，当前救援资源不被迟到记录覆盖
        self.assertEqual(self.asset2["id"], self.area_now()["assigned_asset_id"])
        self.assertEqual(0.0, self.area_now()["swept_pct"])
        self.assertEqual(30.0, item["sweep"]["coverage_pct"])

    def test_sweep_after_incident_close_keeps_field_data(self):
        self.service.complete_area("coord1", "coordinator", self.area["id"], "completed", self.area_now()["version"])
        incident = [i for i in self.service.state()["incidents"] if i["id"] == self.incident["id"]][0]
        self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", incident["version"])

        batch = self.service.merge_offline_batch("field1", "field", "b-4", [self.sweep_event("sw-3")])
        self.assertEqual("pending_review", batch["summary"]["events"][0]["recon_status"])
        item = self.recon_items()[0]
        codes = [r["code"] for r in item["reasons"]]
        self.assertIn("incident_closed", codes)
        self.assertIn("area_closed", codes)
        sweep = [s for s in self.service.state()["sweep_records"] if s["client_event_id"] == "sw-3"][0]
        self.assertEqual("pending_review", sweep["recon_status"])
        self.assertEqual(30.0, sweep["coverage_pct"])

    def test_sweep_after_asset_release_flags_reason(self):
        asset = [a for a in self.service.list_assets() if a["id"] == self.asset["id"]][0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "撤离", asset["version"])
        batch = self.service.merge_offline_batch("field1", "field", "b-5", [self.sweep_event("sw-4")])
        self.assertEqual("pending_review", batch["summary"]["events"][0]["recon_status"])
        codes = [r["code"] for r in self.recon_items()[0]["reasons"]]
        self.assertIn("asset_released", codes)
        released = [r for r in self.recon_items()[0]["reasons"] if r["code"] == "asset_released"][0]
        self.assertEqual("available", released["current"])

    def test_conclusion_invalidated_on_version_change_and_reconfirmed(self):
        self.service.reassign_area("coord1", "coordinator", self.area["id"], self.asset2["id"])
        self.service.merge_offline_batch("field1", "field", "b-6", [self.sweep_event("sw-5")])
        item = self.recon_items()[0]
        resolved = self.service.resolve_recon_item("coord1", "coordinator", item["id"], "confirmed", "确认补录")
        self.assertEqual("confirmed", resolved["status"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])

        self.service.reassign_area("coord1", "coordinator", self.area["id"], self.asset["id"], reason="换回")
        stale = self.recon_items()[0]
        self.assertEqual("stale", stale["status"])
        self.assertIn("current_reasons", stale)

        again = self.service.resolve_recon_item("coord1", "coordinator", item["id"], "confirmed")
        self.assertEqual("confirmed", again["status"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])  # 重新确认不重复入账

        with self.assertRaises(DomainError) as ctx:
            self.service.resolve_recon_item("coord1", "coordinator", item["id"], "dismissed")
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.resolve_recon_item("field1", "field", item["id"], "confirmed")
        self.assertEqual(403, ctx2.exception.status)

    def test_partial_failure_retries_only_failed_records(self):
        good = self.sweep_event("sw-6")
        bad = self.sweep_event("sw-7", coverage_pct=150.0)
        batch = self.service.merge_offline_batch("field1", "field", "b-7", [good, bad])
        self.assertEqual("partial", batch["status"])
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertEqual(1, batch["summary"]["failed"])
        self.assertEqual(30.0, self.area_now()["swept_pct"])

        fixed = self.sweep_event("sw-7", coverage_pct=25.0)
        retry = self.service.merge_offline_batch("field1", "field", "b-7", [fixed])
        self.assertEqual("merged", retry["status"])
        self.assertEqual("accepted", retry["summary"]["events"][0]["status"])
        self.assertEqual(55.0, self.area_now()["swept_pct"])

        again = self.service.merge_offline_batch("field1", "field", "b-7", [good, fixed])
        self.assertEqual(["duplicate", "duplicate"], [e["status"] for e in again["summary"]["events"]])
        self.assertEqual(55.0, self.area_now()["swept_pct"])

        records = self.service.list_offline_records(client_batch_id="b-7")
        retried = [r for r in records if r["client_event_id"] == "sw-7"][0]
        self.assertEqual(2, retried["attempts"])
        self.assertEqual("accepted", retried["status"])
        self.assertEqual("", retried["error"])

    def test_clue_and_timeline_events_keep_working(self):
        batch = self.service.merge_offline_batch(
            "field1", "field", "b-8",
            [{"type": "clue", "client_event_id": "cl-1", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.8, "source": "radio"},
             {"type": "timeline", "client_event_id": "tl-1", "incident_id": self.incident["id"],
              "action": "offline.note", "details": {"text": "现场备注"}},
             {"type": "unknown", "client_event_id": "xx-1"}],
        )
        statuses = [e["status"] for e in batch["summary"]["events"]]
        self.assertEqual(["accepted", "accepted", "failed"], statuses)
        replay = self.service.merge_offline_batch("field1", "field", "b-8", [
            {"type": "clue", "client_event_id": "cl-1", "incident_id": self.incident["id"],
             "latitude": 31.1, "longitude": 122.1, "confidence": 0.8, "source": "radio"},
            {"type": "timeline", "client_event_id": "tl-1", "incident_id": self.incident["id"],
             "action": "offline.note", "details": {"text": "现场备注"}},
        ])
        self.assertEqual(["duplicate", "duplicate"], [e["status"] for e in replay["summary"]["events"]])
        notes = [t for t in self.service.incident_timeline(self.incident["id"]) if t["action"] == "offline.note"]
        self.assertEqual(1, len(notes))


class LegacyMigrationTest(unittest.TestCase):
    def test_legacy_db_upgrade_keeps_recon_visible(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(LEGACY_SCHEMA)
            summary = {
                "accepted": 1,
                "rejected": 1,
                "events": [
                    {"client_event_id": "old-1", "status": "merged", "record_id": 7},
                    {"client_event_id": "old-2", "status": "rejected", "error": "已结束事件不能新增线索"},
                ],
            }
            conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,merged_at,summary) VALUES(?,?,?,?,?,?)",
                ("legacy-b", "op1", "merged", "2026-09-01T00:00:00+00:00",
                 "2026-09-01T00:00:01+00:00", json.dumps(summary, ensure_ascii=False)),
            )
            conn.commit()
            conn.close()

            service = MaritimeSARService(db_path)
            with service.connect() as upgraded:
                version = upgraded.execute("PRAGMA user_version").fetchone()[0]
                columns = [r[1] for r in upgraded.execute("PRAGMA table_info(search_areas)").fetchall()]
            self.assertEqual(1, version)
            self.assertIn("swept_pct", columns)

            batches = service.list_offline_batches()
            self.assertEqual("legacy-b", batches[0]["client_batch_id"])
            records = service.list_offline_records(client_batch_id="legacy-b")
            self.assertEqual(2, len(records))
            failed = [r for r in records if r["status"] == "failed"]
            self.assertEqual("old-2", failed[0]["client_event_id"])
            self.assertIn("已结束事件", failed[0]["error"])
            state = service.state()
            self.assertIn("recon_items", state)
            self.assertIn("sweep_records", state)
            self.assertIn("offline_batches", state)

            # 升级后新流程立即可用
            incident = service.create_incident("coord1", "coordinator", "SAR-200", "海风号", 32.0, 123.0, 8.0, 2, "东海中心")
            asset = service.add_asset("coord1", "coordinator", "海巡09", "vessel", ["surface"], 32.0, 123.0, 20, 200, 5)
            area = service.create_search_area("coord1", "coordinator", incident["id"], "A-200", "surface", 32.1, 123.1, 5, 1)
            area = service.assign_area("coord1", "coordinator", area["id"], asset["id"], asset["version"])
            batch = service.merge_offline_batch("field1", "field", "b-new", [{
                "type": "sweep", "client_event_id": "sw-new", "incident_id": incident["id"],
                "area_id": area["id"], "area_version": area["version"], "asset_id": asset["id"],
                "coverage_pct": 40.0,
            }])
            self.assertEqual("merged", batch["status"])
            self.assertEqual(40.0, service.state()["search_areas"][0]["swept_pct"])


if __name__ == "__main__":
    unittest.main()
