"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import recon_rules
import recon_store

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
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
                CREATE TABLE IF NOT EXISTS assets (
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
                CREATE TABLE IF NOT EXISTS search_areas (
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
                CREATE TABLE IF NOT EXISTS clues (
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
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                """
            )
            recon_store.migrate(conn)

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow()),
        )

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            if area["assigned_asset_id"] is not None:
                raise DomainError("搜索区域已经分配", 409)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可分配", 409)
            if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] != "available":
                raise DomainError("资源当前不可用", 409)
            if incident["sea_state"] > asset["max_sea_state"]:
                raise DomainError("海况超出资源能力", 409)
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                raise DomainError("资源不具备该搜索区域能力", 409)
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                raise DomainError("搜索区域超出资源航程", 409)
            now = utcnow()
            changed = conn.execute(
                "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
                (now, asset_id, asset["version"]),
            )
            if changed.rowcount != 1:
                raise DomainError("资源已被其他任务占用", 409)
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                (asset_id, now, area_id),
            )
            self._invalidate_area_recon(conn, area_id, actor)
            self._audit(conn, area["incident_id"], actor, "area.assigned", {"area_id": area_id, "asset_id": asset_id, "distance_km": round(distance, 2)})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            areas = conn.execute("SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)).fetchall()
            for area in areas:
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?", (now, area["id"]))
                self._invalidate_area_recon(conn, area["id"], actor)
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (utcnow(), area["assigned_asset_id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?", (outcome, utcnow(), area_id))
            self._invalidate_area_recon(conn, area_id, actor)
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def reassign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                      expected_area_version: int | None = None, reason: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "改派搜索区域")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            if area["status"] in ("completed", "abandoned"):
                raise DomainError("搜索区域已结束，不能改派", 409)
            if area["assigned_asset_id"] == asset_id:
                raise DomainError("该区域已分配给此资源", 409)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可改派", 409)
            if expected_area_version is not None and area["version"] != int(expected_area_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            if asset["status"] != "available":
                raise DomainError("资源当前不可用", 409)
            if incident["sea_state"] > asset["max_sea_state"]:
                raise DomainError("海况超出资源能力", 409)
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                raise DomainError("资源不具备该搜索区域能力", 409)
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                raise DomainError("搜索区域超出资源航程", 409)
            now = utcnow()
            old_asset_id = area["assigned_asset_id"]
            if old_asset_id is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, old_asset_id))
            changed = conn.execute(
                "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
                (now, asset_id, asset["version"]),
            )
            if changed.rowcount != 1:
                raise DomainError("资源已被其他任务占用", 409)
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                (asset_id, now, area_id),
            )
            self._invalidate_area_recon(conn, area_id, actor)
            self._audit(conn, area["incident_id"], actor, "area.reassigned",
                        {"area_id": area_id, "from_asset_id": old_asset_id, "to_asset_id": asset_id,
                         "reason": reason.strip(), "distance_km": round(distance, 2)})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def _invalidate_area_recon(self, conn: sqlite3.Connection, area_id: int, actor: str) -> None:
        """区域版本变化后，已有对账结论失效，等待重新确认。"""
        invalidated = recon_store.invalidate_area_conclusions(conn, area_id, utcnow())
        if invalidated:
            row = conn.execute("SELECT incident_id FROM search_areas WHERE id=?", (area_id,)).fetchone()
            self._audit(conn, row["incident_id"] if row else None, actor, "recon.invalidated",
                        {"area_id": area_id, "item_ids": invalidated})

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        now = utcnow()
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = recon_store.get_batch(conn, batch_id)
            batch_row_id = batch["id"] if batch else recon_store.create_batch(conn, batch_id, actor, now)
            conn.commit()
            results, changed = [], False
            for event in events:
                outcome, is_new = self._ingest_offline_event(conn, batch_row_id, batch_id, actor, event, now)
                results.append(outcome)
                changed = changed or is_new
            counts = recon_store.ledger_counts(conn, batch_row_id)
            status = "partial" if counts["failed"] else "merged"
            summary = {"accepted": counts["accepted"], "rejected": counts["failed"],
                       "failed": counts["failed"], "duplicates": counts["duplicate"],
                       "events": results}
            conn.execute("BEGIN IMMEDIATE")
            recon_store.finish_batch(conn, batch_row_id, status, summary, now)
            self._audit(conn, None, actor, "offline.batch_merged",
                        {"batch_id": batch_id, "status": status,
                         **{k: summary[k] for k in ("accepted", "rejected", "duplicates")}})
            conn.commit()
            return {"batch_id": batch_id, "idempotent": not changed, "status": status, "summary": summary}
        finally:
            conn.close()

    def _ingest_offline_event(self, conn: sqlite3.Connection, batch_row_id: int, client_batch_id: str,
                              actor: str, event: Any, now: str) -> tuple[dict[str, Any], bool]:
        """逐条入库：每条独立事务，部分失败不影响已接收记录，失败可单独重试。"""
        if not isinstance(event, dict):
            return {"client_event_id": "", "status": "failed", "error": "离线事件格式错误"}, True
        event_id = str(event.get("client_event_id", "")).strip()
        if not event_id:
            return {"client_event_id": "", "status": "failed", "error": "离线事件缺少 client_event_id"}, True
        entry = recon_store.get_ledger_entry(conn, batch_row_id, event_id)
        if entry and entry["status"] in ("accepted", "duplicate"):
            return {"client_event_id": event_id, "status": "duplicate", "idempotent": True,
                    "record_id": entry["record_id"], "recon_item_id": entry["recon_item_id"]}, False
        record_type = str(event.get("type", "")).strip()
        duplicate_of = self._find_duplicate(conn, record_type, event_id)
        if duplicate_of is not None:
            record_id, recon_item_id = duplicate_of
            conn.execute("BEGIN IMMEDIATE")
            recon_store.ledger_write(conn, batch_row_id, client_batch_id, event_id,
                                     record_type or "unknown", "duplicate", "", record_id, recon_item_id, now)
            conn.commit()
            return {"client_event_id": event_id, "status": "duplicate", "idempotent": True,
                    "record_id": record_id, "recon_item_id": recon_item_id}, False
        conn.execute("BEGIN IMMEDIATE")
        try:
            if record_type == "clue":
                record_id, recon_item_id, extra = self._merge_offline_clue(conn, actor, event, event_id), None, {}
            elif record_type == "sweep":
                record_id, recon_item_id, extra = self._merge_offline_sweep(conn, batch_row_id, actor, event, event_id, now)
            elif record_type == "timeline":
                self._merge_offline_timeline(conn, actor, event)
                record_id, recon_item_id, extra = None, None, {}
            else:
                raise DomainError("不支持的离线事件类型")
            recon_store.ledger_write(conn, batch_row_id, client_batch_id, event_id, record_type,
                                     "accepted", "", record_id, recon_item_id, now)
            conn.commit()
            return {"client_event_id": event_id, "status": "accepted", "record_id": record_id,
                    "recon_item_id": recon_item_id, **extra}, True
        except (DomainError, KeyError, TypeError, ValueError, sqlite3.Error) as exc:
            conn.rollback()
            conn.execute("BEGIN IMMEDIATE")
            recon_store.ledger_write(conn, batch_row_id, client_batch_id, event_id,
                                     record_type or "unknown", "failed", str(exc), None, None, now)
            conn.commit()
            return {"client_event_id": event_id, "status": "failed", "error": str(exc)}, True

    def _find_duplicate(self, conn: sqlite3.Connection, record_type: str,
                        event_id: str) -> tuple[int | None, int | None] | None:
        """同一记录只入一次：领域表与台账双重查重。"""
        if record_type == "clue":
            row = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            return (row["id"], None) if row else None
        if record_type == "sweep":
            row = recon_store.get_sweep_by_event_id(conn, event_id)
            if not row:
                return None
            item = recon_store.get_recon_item_by_sweep(conn, row["id"])
            return (row["id"], item["id"] if item else None)
        row = recon_store.find_accepted_event(conn, event_id)
        return (row["record_id"], row["recon_item_id"]) if row else None

    def _merge_offline_clue(self, conn: sqlite3.Connection, actor: str,
                            event: dict[str, Any], event_id: str) -> int:
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("已结束事件不能新增线索", 409)
        area_id = event.get("area_id")
        if area_id is not None and not conn.execute(
            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
        ).fetchone():
            raise DomainError("搜索区域不属于该事件", 409)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, area_id, event_id, lat, lon, confidence,
             str(event.get("source", "offline")).strip(), status, distance, actor,
             str(event.get("details", "")).strip(), utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
        return int(cur.lastrowid)

    def _merge_offline_sweep(self, conn: sqlite3.Connection, batch_row_id: int, actor: str,
                             event: dict[str, Any], event_id: str, now: str) -> tuple[int, int | None, dict[str, Any]]:
        try:
            incident_id = int(event["incident_id"])
            area_id = int(event["area_id"])
            area_version = int(event["area_version"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DomainError("扫测记录缺少必要字段或格式错误") from exc
        asset_id = event.get("asset_id")
        try:
            asset_id = int(asset_id) if asset_id is not None else None
        except (TypeError, ValueError) as exc:
            raise DomainError("扫测记录的资源编号无效") from exc
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone() if asset_id is not None else None
        record = {"incident_id": incident_id, "area_id": area_id, "area_version": area_version,
                  "asset_id": asset_id, "coverage_pct": event.get("coverage_pct")}
        decision = recon_rules.evaluate_sweep(
            record,
            incident=dict(incident) if incident else None,
            area=dict(area) if area else None,
            asset=dict(asset) if asset else None,
        )
        if decision["verdict"] == "reject":
            raise DomainError(decision["reasons"][0]["message"])
        coverage = float(event["coverage_pct"])
        merged = decision["verdict"] == "merge"
        sweep_id = recon_store.insert_sweep(
            conn, client_event_id=event_id, batch_id=batch_row_id, incident_id=incident_id,
            area_id=area_id, asset_id=asset_id, area_version=area_version, coverage_pct=coverage,
            swept_at=str(event.get("swept_at", "")).strip() or now,
            notes=str(event.get("notes", "")).strip(), actor=actor,
            recon_status="merged" if merged else "pending_review",
            applied=1 if merged else 0, now=now,
        )
        recon_item_id = None
        if merged:
            recon_store.apply_coverage(conn, area_id, coverage, now)
            self._audit(conn, incident_id, actor, "sweep.merged",
                        {"sweep_id": sweep_id, "area_id": area_id, "coverage_pct": coverage, "event_id": event_id})
        else:
            recon_item_id = recon_store.insert_recon_item(
                conn, sweep_record_id=sweep_id, client_event_id=event_id, incident_id=incident_id,
                area_id=area_id, area_version_recorded=area_version,
                area_version_checked=area["version"] if area else None,
                reasons=decision["reasons"], now=now,
            )
            self._audit(conn, incident_id, actor, "sweep.pending_review",
                        {"sweep_id": sweep_id, "recon_item_id": recon_item_id,
                         "reasons": [r["code"] for r in decision["reasons"]], "event_id": event_id})
        return sweep_id, recon_item_id, {"recon_status": "merged" if merged else "pending_review"}

    def _merge_offline_timeline(self, conn: sqlite3.Connection, actor: str, event: dict[str, Any]) -> None:
        incident_id = int(event["incident_id"])
        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
            raise DomainError("事件不存在", 404)
        self._audit(conn, incident_id, actor, event.get("action", "offline.note"), event.get("details", {}))

    def list_recon_items(self, actor: str = "", role: str = "viewer",
                         incident_id: Any = None, status: str | None = None) -> list[dict[str, Any]]:
        if incident_id in (None, ""):
            incident_id = None
        else:
            incident_id = int(incident_id)
        if not status:
            status = None
        with self.connect() as conn:
            return [self._recon_view(conn, item) for item in recon_store.list_recon_items(conn, incident_id, status)]

    def _recon_view(self, conn: sqlite3.Connection, item: dict[str, Any]) -> dict[str, Any]:
        """对账项视图：两端值、差异原因，并按当前状态重算待复核项的差异。"""
        view = dict(item)
        view["reasons"] = json.loads(item["reasons"])
        sweep = recon_store.get_sweep(conn, item["sweep_record_id"])
        view["sweep"] = sweep
        area = None
        if item["area_id"] is not None:
            row = conn.execute("SELECT * FROM search_areas WHERE id=?", (item["area_id"],)).fetchone()
            area = dict(row) if row else None
        view["area_version_current"] = area["version"] if area else None
        if item["status"] in ("pending_review", "stale") and sweep is not None:
            incident_row = conn.execute("SELECT * FROM incidents WHERE id=?", (item["incident_id"],)).fetchone()
            asset_row = None
            if sweep["asset_id"] is not None:
                asset_row = conn.execute("SELECT * FROM assets WHERE id=?", (sweep["asset_id"],)).fetchone()
            record = {"incident_id": item["incident_id"], "area_id": item["area_id"],
                      "area_version": sweep["area_version"], "asset_id": sweep["asset_id"],
                      "coverage_pct": sweep["coverage_pct"]}
            view["current_reasons"] = recon_rules.evaluate_sweep(
                record,
                incident=dict(incident_row) if incident_row else None,
                area=area,
                asset=dict(asset_row) if asset_row else None,
            )["reasons"]
        return view

    def resolve_recon_item(self, actor: str, role: str, item_id: int, decision: str,
                           note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "复核对账项")
        if decision not in ("confirmed", "dismissed"):
            raise DomainError("复核结论无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            item = recon_store.get_recon_item(conn, int(item_id))
            if not item:
                raise DomainError("对账项不存在", 404)
            if item["status"] not in ("pending_review", "stale"):
                raise DomainError("该对账项已有结论，区域版本变化后才能重新确认", 409)
            sweep = recon_store.get_sweep(conn, item["sweep_record_id"])
            area = None
            if item["area_id"] is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=?", (item["area_id"],)).fetchone()
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (item["incident_id"],)).fetchone()
            applied = bool(sweep["applied"])
            if decision == "confirmed":
                if not applied and area and area["status"] not in ("completed", "abandoned") \
                        and incident and incident["status"] in ACTIVE_INCIDENT:
                    recon_store.apply_coverage(conn, area["id"], sweep["coverage_pct"], now)
                    applied = True
            recon_store.set_sweep_recon_status(conn, sweep["id"], decision, 1 if applied else 0, now)
            recon_store.resolve_item(conn, item["id"], decision, actor, note.strip(),
                                     area["version"] if area else None, now)
            self._audit(conn, item["incident_id"], actor, "recon.resolved",
                        {"item_id": item["id"], "decision": decision,
                         "coverage_applied": applied, "note": note.strip()})
            return self._recon_view(conn, recon_store.get_recon_item(conn, item["id"]))

    @staticmethod
    def _batch_view(row: dict[str, Any]) -> dict[str, Any]:
        batch = dict(row)
        try:
            batch["summary"] = json.loads(batch["summary"])
        except (ValueError, TypeError):
            pass
        return batch

    def list_offline_batches(self, actor: str = "", role: str = "viewer") -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [self._batch_view(row) for row in recon_store.list_batches(conn)]

    def list_offline_records(self, actor: str = "", role: str = "viewer",
                             client_batch_id: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return recon_store.list_ledger(conn, client_batch_id)

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
            sweeps = recon_store.list_sweeps(conn)
            recon_items = [self._recon_view(conn, item) for item in recon_store.list_recon_items(conn)]
            batches = [self._batch_view(row) for row in recon_store.list_batches(conn)]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "timeline": timeline, "sweep_records": sweeps, "recon_items": recon_items,
                "offline_batches": batches}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            if path == "/api/recon":
                query = parse_qs(urlparse(self.path).query)
                self._send(200, {"items": self.service.list_recon_items(
                    *self._actor(),
                    incident_id=query.get("incident_id", [None])[0],
                    status=query.get("status", [None])[0],
                )})
                return
            if path == "/api/offline/batches":
                self._send(200, {"batches": self.service.list_offline_batches(*self._actor())})
                return
            if path == "/api/offline/records":
                query = parse_qs(urlparse(self.path).query)
                self._send(200, {"records": self.service.list_offline_records(
                    *self._actor(), client_batch_id=query.get("batch", [None])[0])})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/areas/reassign":
                result = self.service.reassign_area(actor, role, **data)
            elif path == "/api/recon/resolve":
                result = self.service.resolve_recon_item(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
