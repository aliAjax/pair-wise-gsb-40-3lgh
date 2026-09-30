"""回传对账的数据层：表结构迁移与存取函数。

只使用标准库 sqlite3。对账规则在 recon_rules.py，接口编排在 app.py。
库结构按 PRAGMA user_version 升级：旧库打开时自动迁移，
旧批次的逐条结果回填进台账，升级后对账状态照常可查。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

SCHEMA_VERSION = 1

# 对账项状态
PENDING_REVIEW = "pending_review"   # 停在待复核
STALE = "stale"                     # 区域版本已变，结论失效待重新确认
CONCLUDED = ("confirmed", "dismissed")

# 台账记录状态
ACCEPTED = "accepted"
FAILED = "failed"
DUPLICATE = "duplicate"


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def migrate(conn: sqlite3.Connection) -> None:
    """把库结构升级到当前版本；旧数据保留，旧批次回填台账。"""
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        _migrate_v1(conn)
    if version < SCHEMA_VERSION:
        conn.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)


def _migrate_v1(conn: sqlite3.Connection) -> None:
    columns = [row[1] for row in conn.execute("PRAGMA table_info(search_areas)").fetchall()]
    if "swept_pct" not in columns:
        conn.execute("ALTER TABLE search_areas ADD COLUMN swept_pct REAL NOT NULL DEFAULT 0")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sweep_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_event_id TEXT NOT NULL UNIQUE,
            batch_id INTEGER REFERENCES offline_batches(id),
            incident_id INTEGER NOT NULL REFERENCES incidents(id),
            area_id INTEGER REFERENCES search_areas(id),
            asset_id INTEGER REFERENCES assets(id),
            area_version INTEGER NOT NULL,
            coverage_pct REAL NOT NULL,
            swept_at TEXT NOT NULL,
            notes TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL,
            recon_status TEXT NOT NULL,
            applied INTEGER NOT NULL DEFAULT 0,
            received_at TEXT NOT NULL,
            merged_at TEXT
        );
        CREATE TABLE IF NOT EXISTS recon_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sweep_record_id INTEGER NOT NULL UNIQUE REFERENCES sweep_records(id),
            client_event_id TEXT NOT NULL,
            incident_id INTEGER NOT NULL REFERENCES incidents(id),
            area_id INTEGER REFERENCES search_areas(id),
            area_version_recorded INTEGER NOT NULL,
            area_version_checked INTEGER,
            status TEXT NOT NULL,
            reasons TEXT NOT NULL,
            decision_note TEXT NOT NULL DEFAULT '',
            decided_by TEXT,
            decided_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS offline_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES offline_batches(id),
            client_batch_id TEXT NOT NULL,
            client_event_id TEXT NOT NULL,
            record_type TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT NOT NULL DEFAULT '',
            record_id INTEGER,
            recon_item_id INTEGER,
            attempts INTEGER NOT NULL DEFAULT 1,
            received_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(client_batch_id, client_event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_sweep_incident ON sweep_records(incident_id, id);
        CREATE INDEX IF NOT EXISTS idx_recon_status ON recon_items(status, incident_id);
        CREATE INDEX IF NOT EXISTS idx_offline_records_batch ON offline_records(batch_id, id);
        CREATE INDEX IF NOT EXISTS idx_offline_records_event ON offline_records(client_event_id);
        """
    )
    _backfill_legacy_batches(conn)


def _backfill_legacy_batches(conn: sqlite3.Connection) -> None:
    """把升级前批次的逐条结果回填进台账，旧批次也能查出哪些没对上。"""
    for batch in conn.execute("SELECT * FROM offline_batches").fetchall():
        try:
            summary = json.loads(batch["summary"] or "{}")
        except ValueError:
            continue
        for event in summary.get("events", []):
            event_id = str(event.get("client_event_id", "")).strip()
            if not event_id:
                continue
            status = ACCEPTED if event.get("status") == "merged" else FAILED
            conn.execute(
                """INSERT OR IGNORE INTO offline_records(batch_id,client_batch_id,client_event_id,
                   record_type,status,error,record_id,received_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (batch["id"], batch["client_batch_id"], event_id, "legacy", status,
                 str(event.get("error", "")), event.get("record_id"),
                 batch["received_at"], batch["received_at"]),
            )


# ---- 批次 ----

def get_batch(conn: sqlite3.Connection, client_batch_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (client_batch_id,)).fetchone()
    return dict(row) if row else None


def create_batch(conn: sqlite3.Connection, client_batch_id: str, actor: str, now: str) -> int:
    try:
        cur = conn.execute(
            "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,summary) VALUES(?,?,?,?,?)",
            (client_batch_id, actor, "receiving", now, "{}"),
        )
        return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        existing = get_batch(conn, client_batch_id)
        if existing is None:
            raise
        return existing["id"]


def finish_batch(conn: sqlite3.Connection, batch_row_id: int, status: str,
                 summary: dict[str, Any], now: str) -> None:
    conn.execute(
        "UPDATE offline_batches SET status=?,merged_at=?,summary=? WHERE id=?",
        (status, now, _dump(summary), batch_row_id),
    )


def list_batches(conn: sqlite3.Connection, limit: int = 100) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM offline_batches ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


# ---- 逐条台账 ----

def get_ledger_entry(conn: sqlite3.Connection, batch_row_id: int,
                     client_event_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM offline_records WHERE batch_id=? AND client_event_id=?",
        (batch_row_id, client_event_id),
    ).fetchone()
    return dict(row) if row else None


def ledger_write(conn: sqlite3.Connection, batch_row_id: int, client_batch_id: str,
                 client_event_id: str, record_type: str, status: str, error: str,
                 record_id: int | None, recon_item_id: int | None, now: str) -> None:
    entry = get_ledger_entry(conn, batch_row_id, client_event_id)
    if entry is None:
        conn.execute(
            """INSERT INTO offline_records(batch_id,client_batch_id,client_event_id,record_type,
               status,error,record_id,recon_item_id,attempts,received_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,1,?,?)""",
            (batch_row_id, client_batch_id, client_event_id, record_type,
             status, error, record_id, recon_item_id, now, now),
        )
    else:
        conn.execute(
            """UPDATE offline_records SET record_type=?,status=?,error=?,record_id=?,
               recon_item_id=?,attempts=attempts+1,updated_at=? WHERE id=?""",
            (record_type, status, error, record_id, recon_item_id, now, entry["id"]),
        )


def ledger_counts(conn: sqlite3.Connection, batch_row_id: int) -> dict[str, int]:
    counts = {ACCEPTED: 0, FAILED: 0, DUPLICATE: 0}
    rows = conn.execute(
        "SELECT status,COUNT(*) AS c FROM offline_records WHERE batch_id=? GROUP BY status",
        (batch_row_id,),
    ).fetchall()
    for row in rows:
        counts[row["status"]] = row["c"]
    return counts


def find_accepted_event(conn: sqlite3.Connection, client_event_id: str) -> dict[str, Any] | None:
    """全局幂等：任一批次已接收过的同一记录。"""
    row = conn.execute(
        """SELECT * FROM offline_records WHERE client_event_id=? AND status IN ('accepted','duplicate')
           ORDER BY id LIMIT 1""",
        (client_event_id,),
    ).fetchone()
    return dict(row) if row else None


def list_ledger(conn: sqlite3.Connection, client_batch_id: str | None = None,
                limit: int = 500) -> list[dict[str, Any]]:
    if client_batch_id:
        rows = conn.execute(
            "SELECT * FROM offline_records WHERE client_batch_id=? ORDER BY id",
            (client_batch_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM offline_records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


# ---- 扫测记录 ----

def insert_sweep(conn: sqlite3.Connection, *, client_event_id: str, batch_id: int,
                 incident_id: int, area_id: int, asset_id: int | None, area_version: int,
                 coverage_pct: float, swept_at: str, notes: str, actor: str,
                 recon_status: str, applied: int, now: str) -> int:
    cur = conn.execute(
        """INSERT INTO sweep_records(client_event_id,batch_id,incident_id,area_id,asset_id,
           area_version,coverage_pct,swept_at,notes,actor,recon_status,applied,received_at,merged_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (client_event_id, batch_id, incident_id, area_id, asset_id, area_version, coverage_pct,
         swept_at, notes, actor, recon_status, applied, now, now if applied else None),
    )
    return int(cur.lastrowid)


def get_sweep(conn: sqlite3.Connection, sweep_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM sweep_records WHERE id=?", (sweep_id,)).fetchone()
    return dict(row) if row else None


def get_sweep_by_event_id(conn: sqlite3.Connection, client_event_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM sweep_records WHERE client_event_id=?", (client_event_id,)).fetchone()
    return dict(row) if row else None


def set_sweep_recon_status(conn: sqlite3.Connection, sweep_id: int, status: str,
                           applied: int, now: str) -> None:
    conn.execute(
        "UPDATE sweep_records SET recon_status=?,applied=?,merged_at=? WHERE id=?",
        (status, applied, now, sweep_id),
    )


def list_sweeps(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM sweep_records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


def apply_coverage(conn: sqlite3.Connection, area_id: int, coverage_pct: float, now: str) -> None:
    """合并扫测覆盖率；只累积进度，不动区域版本与资源分配。"""
    conn.execute(
        "UPDATE search_areas SET swept_pct=MIN(100.0, swept_pct+?),updated_at=? WHERE id=?",
        (coverage_pct, now, area_id),
    )


# ---- 对账项 ----

def insert_recon_item(conn: sqlite3.Connection, *, sweep_record_id: int, client_event_id: str,
                      incident_id: int, area_id: int | None, area_version_recorded: int,
                      area_version_checked: int | None, reasons: list[dict[str, Any]],
                      now: str) -> int:
    cur = conn.execute(
        """INSERT INTO recon_items(sweep_record_id,client_event_id,incident_id,area_id,
           area_version_recorded,area_version_checked,status,reasons,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (sweep_record_id, client_event_id, incident_id, area_id, area_version_recorded,
         area_version_checked, PENDING_REVIEW, _dump(reasons), now, now),
    )
    return int(cur.lastrowid)


def get_recon_item(conn: sqlite3.Connection, item_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM recon_items WHERE id=?", (item_id,)).fetchone()
    return dict(row) if row else None


def get_recon_item_by_sweep(conn: sqlite3.Connection, sweep_record_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM recon_items WHERE sweep_record_id=?", (sweep_record_id,)).fetchone()
    return dict(row) if row else None


def list_recon_items(conn: sqlite3.Connection, incident_id: int | None = None,
                     status: str | None = None) -> list[dict[str, Any]]:
    sql, clauses, params = "SELECT * FROM recon_items", [], []
    if incident_id is not None:
        clauses.append("incident_id=?")
        params.append(incident_id)
    if status is not None:
        clauses.append("status=?")
        params.append(status)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC"
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def resolve_item(conn: sqlite3.Connection, item_id: int, decision: str, actor: str,
                 note: str, area_version: int | None, now: str) -> None:
    conn.execute(
        """UPDATE recon_items SET status=?,decided_by=?,decided_at=?,decision_note=?,
           area_version_checked=?,updated_at=? WHERE id=?""",
        (decision, actor, now, note, area_version, now, item_id),
    )


def invalidate_area_conclusions(conn: sqlite3.Connection, area_id: int, now: str) -> list[int]:
    """区域版本变化后，已有复核结论失效，返回被失效的对账项编号。"""
    rows = conn.execute(
        "SELECT id FROM recon_items WHERE area_id=? AND status IN ('confirmed','dismissed')",
        (area_id,),
    ).fetchall()
    ids = [row["id"] for row in rows]
    if ids:
        conn.execute(
            "UPDATE recon_items SET status=?,updated_at=? WHERE area_id=? AND status IN ('confirmed','dismissed')",
            (STALE, now, area_id),
        )
    return ids
