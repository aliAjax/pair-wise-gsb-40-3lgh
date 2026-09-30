"""回传对账数据层：表结构、版本迁移与 SQL 存取。

只依赖标准库，不认识规则层和接口层；所有函数接收调用方传入的连接，
便于并入调用方事务（例如区域版本变化时在同一个事务里让复核结论失效）。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

# 主程序原始表结构视为版本 1，回传对账结构为版本 2。
SCHEMA_VERSION = 2

RECON_TABLES = """
CREATE TABLE IF NOT EXISTS sweep_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_event_id TEXT NOT NULL UNIQUE,
    client_batch_id TEXT NOT NULL,
    incident_id INTEGER NOT NULL REFERENCES incidents(id),
    area_id INTEGER REFERENCES search_areas(id),
    area_version_seen INTEGER NOT NULL,
    asset_name TEXT NOT NULL DEFAULT '',
    swept_pct REAL NOT NULL,
    contacts INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL,
    recon_status TEXT NOT NULL,
    diff_reasons TEXT NOT NULL DEFAULT '[]',
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_batch_id TEXT NOT NULL UNIQUE,
    actor TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NOT NULL,
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_batch_id TEXT NOT NULL,
    client_event_id TEXT NOT NULL,
    status TEXT NOT NULL,
    record_id INTEGER REFERENCES sweep_records(id),
    error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    UNIQUE(client_batch_id, client_event_id)
);
CREATE TABLE IF NOT EXISTS recon_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id INTEGER NOT NULL REFERENCES sweep_records(id),
    area_id INTEGER,
    area_version_at_review INTEGER,
    conclusion TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    reviewer TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sweep_area ON sweep_records(area_id);
CREATE INDEX IF NOT EXISTS idx_receipts_batch ON recon_receipts(client_batch_id);
CREATE INDEX IF NOT EXISTS idx_reviews_record ON recon_reviews(record_id, status);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def migrate(conn: sqlite3.Connection) -> int:
    """把数据库升级到最新对账结构，可重复执行，返回升级后的版本。"""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            upgraded_at TEXT NOT NULL
        );
        """
    )
    row = conn.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    version = int(row["version"]) if row else 1
    if version < 2:
        conn.executescript(RECON_TABLES)
        version = 2
    now = utcnow()
    if row:
        conn.execute("UPDATE schema_meta SET version=?,upgraded_at=? WHERE id=1", (version, now))
    else:
        conn.execute("INSERT INTO schema_meta(id,version,upgraded_at) VALUES(1,?,?)", (version, now))
    return version


def schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    return int(row["version"]) if row else 1


# ---- 主流程表（只读，用于对账上下文与历史数据） ----

def load_context(conn: sqlite3.Connection, incident_id: int, area_id: int) -> dict[str, Any]:
    incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
    area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
    asset = None
    if area and area["assigned_asset_id"] is not None:
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (area["assigned_asset_id"],)).fetchone()
    return {"incident": incident, "area": area, "asset": asset}


def get_area(conn: sqlite3.Connection, area_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()


def legacy_clues(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id,client_event_id,incident_id,status,recorded_at FROM clues ORDER BY id DESC LIMIT 200"
    ).fetchall()


def legacy_batches(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT client_batch_id,status,received_at,merged_at,summary FROM offline_batches ORDER BY id DESC LIMIT 100"
    ).fetchall()


def audit(conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
        (incident_id, actor, action, json_dump(details), utcnow()),
    )


# ---- 批次与回执 ----

def get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM recon_batches WHERE client_batch_id=?", (batch_id,)).fetchone()


def upsert_batch(conn: sqlite3.Connection, batch_id: str, actor: str, status: str, summary: dict[str, Any]) -> None:
    now = utcnow()
    if get_batch(conn, batch_id):
        conn.execute(
            "UPDATE recon_batches SET actor=?,status=?,summary=?,updated_at=? WHERE client_batch_id=?",
            (actor, status, json_dump(summary), now, batch_id),
        )
    else:
        conn.execute(
            "INSERT INTO recon_batches(client_batch_id,actor,status,summary,received_at,updated_at) VALUES(?,?,?,?,?,?)",
            (batch_id, actor, status, json_dump(summary), now, now),
        )


def get_receipt(conn: sqlite3.Connection, batch_id: str, event_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM recon_receipts WHERE client_batch_id=? AND client_event_id=?", (batch_id, event_id)
    ).fetchone()


def upsert_receipt(conn: sqlite3.Connection, batch_id: str, event_id: str, status: str,
                   record_id: int | None, error: str) -> None:
    now = utcnow()
    if get_receipt(conn, batch_id, event_id):
        conn.execute(
            "UPDATE recon_receipts SET status=?,record_id=?,error=?,updated_at=? WHERE client_batch_id=? AND client_event_id=?",
            (status, record_id, error, now, batch_id, event_id),
        )
    else:
        conn.execute(
            "INSERT INTO recon_receipts(client_batch_id,client_event_id,status,record_id,error,updated_at) VALUES(?,?,?,?,?,?)",
            (batch_id, event_id, status, record_id, error, now),
        )


def receipts_for_batch(conn: sqlite3.Connection, batch_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM recon_receipts WHERE client_batch_id=? ORDER BY id", (batch_id,)
    ).fetchall()


def list_batches(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM recon_batches ORDER BY id DESC LIMIT 100").fetchall()


# ---- 扫测记录 ----

def get_record(conn: sqlite3.Connection, record_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM sweep_records WHERE id=?", (record_id,)).fetchone()


def get_record_by_event(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM sweep_records WHERE client_event_id=?", (event_id,)).fetchone()


def insert_record(conn: sqlite3.Connection, payload: dict[str, Any], batch_id: str,
                  recon_status: str, reasons: list[dict[str, Any]], actor: str) -> int:
    now = utcnow()
    cur = conn.execute(
        """INSERT INTO sweep_records(client_event_id,client_batch_id,incident_id,area_id,area_version_seen,
           asset_name,swept_pct,contacts,note,recorded_at,recon_status,diff_reasons,actor,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (payload["client_event_id"], batch_id, payload["incident_id"], payload["area_id"],
         payload["area_version_seen"], payload["asset_name"], payload["swept_pct"], payload["contacts"],
         payload["note"], payload["recorded_at"], recon_status, json_dump(reasons), actor, now, now),
    )
    return int(cur.lastrowid)


def update_record_status(conn: sqlite3.Connection, record_id: int, recon_status: str) -> None:
    conn.execute(
        "UPDATE sweep_records SET recon_status=?,updated_at=? WHERE id=?",
        (recon_status, utcnow(), record_id),
    )


def records_with_area(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT r.*, a.version AS current_area_version, a.status AS current_area_status, a.code AS area_code
           FROM sweep_records r LEFT JOIN search_areas a ON a.id=r.area_id ORDER BY r.id"""
    ).fetchall()


# ---- 复核结论 ----

def insert_review(conn: sqlite3.Connection, record_id: int, area_id: int | None,
                  area_version: int | None, conclusion: str, note: str, reviewer: str) -> int:
    cur = conn.execute(
        """INSERT INTO recon_reviews(record_id,area_id,area_version_at_review,conclusion,note,status,reviewer,created_at)
           VALUES(?,?,?,?,?,'active',?,?)""",
        (record_id, area_id, area_version, conclusion, note, reviewer, utcnow()),
    )
    return int(cur.lastrowid)


def get_review(conn: sqlite3.Connection, review_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM recon_reviews WHERE id=?", (review_id,)).fetchone()


def list_reviews(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM recon_reviews ORDER BY id").fetchall()


def active_reviews_for_area(conn: sqlite3.Connection, area_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT rv.id AS review_id, rv.record_id AS record_id, r.incident_id AS incident_id
           FROM recon_reviews rv JOIN sweep_records r ON r.id=rv.record_id
           WHERE r.area_id=? AND rv.status='active'""",
        (area_id,),
    ).fetchall()


def mark_review_stale(conn: sqlite3.Connection, review_id: int) -> None:
    conn.execute("UPDATE recon_reviews SET status='stale' WHERE id=?", (review_id,))


def mark_active_reviews_stale(conn: sqlite3.Connection, record_id: int) -> None:
    conn.execute("UPDATE recon_reviews SET status='stale' WHERE record_id=? AND status='active'", (record_id,))
