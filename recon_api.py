"""回传对账接口层：参数校验、幂等合并、复核与状态查询。

只依赖 recon_store（数据层）和 recon_rules（规则层），不依赖 app.py；
HTTP 层（app.py）把 JSON 请求直接转交给这里的方法。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

import recon_rules as rules
import recon_store as store


class ReconError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


SUBMIT_ROLES = {"field", "operator", "coordinator"}
REVIEW_ROLES = {"coordinator"}


def _clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise ReconError("缺少操作人")
    return actor


def _require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise ReconError("角色无权执行：%s" % action, 403)


def _validate_record(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ReconError("扫测记录必须是对象")
    event_id = str(raw.get("client_event_id", "")).strip()
    if not event_id:
        raise ReconError("扫测记录缺少 client_event_id")
    try:
        incident_id = int(raw["incident_id"])
        area_id = int(raw["area_id"])
        area_version_seen = int(raw["area_version_seen"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ReconError("扫测记录缺少有效的 incident_id/area_id/area_version_seen") from exc
    if area_version_seen < 1:
        raise ReconError("area_version_seen 必须为正整数")
    try:
        swept_pct = float(raw.get("swept_pct", 0))
        contacts = int(raw.get("contacts", 0))
    except (TypeError, ValueError) as exc:
        raise ReconError("swept_pct 和 contacts 必须是数值") from exc
    if not 0 <= swept_pct <= 100:
        raise ReconError("swept_pct 应在 0 到 100 之间")
    if contacts < 0:
        raise ReconError("contacts 不能为负")
    recorded_at = str(raw.get("recorded_at") or "").strip() or store.utcnow()
    try:
        datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReconError("recorded_at 时间格式无效") from exc
    return {
        "client_event_id": event_id,
        "incident_id": incident_id,
        "area_id": area_id,
        "area_version_seen": area_version_seen,
        "asset_name": str(raw.get("asset_name", "")).strip(),
        "swept_pct": swept_pct,
        "contacts": contacts,
        "note": str(raw.get("note", "")).strip(),
        "recorded_at": recorded_at,
    }


class ReconAPI:
    """对账应用接口：被 HTTP 层和协调主流程调用。"""

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with store.connect(self.db_path) as conn:
            store.migrate(conn)

    # ---- 批次回传：幂等合并，部分失败只重试失败记录 ----

    def submit_batch(self, actor: str, role: str, client_batch_id: str, records: list[Any]) -> dict[str, Any]:
        actor = _clean_actor(actor)
        _require_role(role, SUBMIT_ROLES, "回传扫测记录")
        batch_id = str(client_batch_id or "").strip()
        if not batch_id or not isinstance(records, list):
            raise ReconError("批次编号和记录列表不能为空")
        with store.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existed_before = store.get_batch(conn, batch_id) is not None
            results = [self._process_record(conn, batch_id, actor, raw) for raw in records]
            processed_now = sum(1 for item in results if not item.get("idempotent"))
            summary = self._batch_summary(conn, batch_id)
            batch_status = "partial_failed" if summary["failed"] else "completed"
            store.upsert_batch(conn, batch_id, actor, batch_status, summary)
            store.audit(conn, None, actor, "recon.batch_processed", {"batch_id": batch_id, **summary})
            return {
                "batch_id": batch_id,
                "status": batch_status,
                "idempotent": existed_before and processed_now == 0,
                "summary": summary,
                "results": results,
            }

    def _process_record(self, conn: sqlite3.Connection, batch_id: str, actor: str, raw: Any) -> dict[str, Any]:
        event_id = str(raw.get("client_event_id", "")).strip() if isinstance(raw, dict) else ""
        if not event_id:
            return {"client_event_id": "", "status": "failed", "error": "扫测记录缺少 client_event_id", "idempotent": False}
        receipt = store.get_receipt(conn, batch_id, event_id)
        if receipt and receipt["status"] != "failed":
            # 已接收的记录直接返回原回执，不重复入库
            result: dict[str, Any] = {
                "client_event_id": event_id,
                "status": receipt["status"],
                "record_id": receipt["record_id"],
                "idempotent": True,
            }
            if receipt["record_id"]:
                record = store.get_record(conn, receipt["record_id"])
                if record:
                    result["recon_status"] = record["recon_status"]
                    result["reasons"] = json.loads(record["diff_reasons"])
            return result
        try:
            conn.execute("SAVEPOINT recon_item")
            try:
                payload = _validate_record(raw)
                existing = store.get_record_by_event(conn, event_id)
                if existing:
                    # 同一记录只入一次：其它批次已入库则本批次记为重复
                    store.upsert_receipt(conn, batch_id, event_id, "duplicate", existing["id"], "")
                    result = {"client_event_id": event_id, "status": "duplicate", "record_id": existing["id"],
                              "recon_status": existing["recon_status"], "idempotent": False}
                else:
                    context = store.load_context(conn, payload["incident_id"], payload["area_id"])
                    if not context["incident"]:
                        raise ReconError("事件不存在")
                    if not context["area"]:
                        raise ReconError("搜索区域不存在")
                    if context["area"]["incident_id"] != payload["incident_id"]:
                        raise ReconError("搜索区域不属于该事件")
                    decision = rules.evaluate_record(payload, context)
                    merged = decision["decision"] == "merge"
                    recon_status = "merged" if merged else "pending_review"
                    record_id = store.insert_record(conn, payload, batch_id, recon_status, decision["reasons"], actor)
                    store.upsert_receipt(conn, batch_id, event_id, "accepted" if merged else "pending", record_id, "")
                    store.audit(conn, payload["incident_id"], actor,
                                "sweep.merged" if merged else "sweep.parked",
                                {"record_id": record_id, "event_id": event_id, "reasons": decision["reasons"]})
                    result = {"client_event_id": event_id, "status": "accepted" if merged else "pending",
                              "record_id": record_id, "recon_status": recon_status,
                              "reasons": decision["reasons"], "idempotent": False}
                conn.execute("RELEASE recon_item")
                return result
            except Exception:
                conn.execute("ROLLBACK TO recon_item")
                conn.execute("RELEASE recon_item")
                raise
        except ReconError as exc:
            # 只把这一条记为失败，同批其它记录不受影响；重试时仅失败记录会被重新处理
            store.upsert_receipt(conn, batch_id, event_id, "failed", None, str(exc))
            return {"client_event_id": event_id, "status": "failed", "error": str(exc), "idempotent": False}
        except sqlite3.Error as exc:
            store.upsert_receipt(conn, batch_id, event_id, "failed", None, "数据库错误: %s" % exc)
            return {"client_event_id": event_id, "status": "failed", "error": "数据库错误: %s" % exc, "idempotent": False}

    def _batch_summary(self, conn: sqlite3.Connection, batch_id: str) -> dict[str, int]:
        receipts = store.receipts_for_batch(conn, batch_id)
        summary = {"accepted": 0, "pending": 0, "duplicate": 0, "failed": 0}
        for receipt in receipts:
            summary[receipt["status"]] = summary.get(receipt["status"], 0) + 1
        return summary

    # ---- 复核：登记结论；区域版本变化后结论失效需重新确认 ----

    def review(self, actor: str, role: str, record_id: int, conclusion: str, note: str = "") -> dict[str, Any]:
        actor = _clean_actor(actor)
        _require_role(role, REVIEW_ROLES, "复核扫测记录")
        conclusion = str(conclusion or "").strip()
        if conclusion not in rules.CONCLUSIONS:
            raise ReconError("复核结论无效")
        with store.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = store.get_record(conn, int(record_id))
            if not record:
                raise ReconError("扫测记录不存在", 404)
            if not rules.can_review(record["recon_status"]):
                raise ReconError("记录当前不在待复核状态", 409)
            area = store.get_area(conn, record["area_id"]) if record["area_id"] else None
            store.mark_active_reviews_stale(conn, record["id"])
            review_id = store.insert_review(conn, record["id"], record["area_id"],
                                            area["version"] if area else None,
                                            conclusion, str(note or "").strip(), actor)
            store.update_record_status(conn, record["id"], rules.conclusion_to_status(conclusion))
            store.audit(conn, record["incident_id"], actor, "recon.reviewed",
                        {"record_id": record["id"], "conclusion": conclusion,
                         "area_version": area["version"] if area else None})
            return {"record": dict(store.get_record(conn, record["id"])),
                    "review": dict(store.get_review(conn, review_id))}

    def on_area_version_changed(self, conn: sqlite3.Connection, area_id: int, actor: str) -> int:
        """区域版本变化时由协调主流程在同一事务内调用：已有复核结论失效，记录回到待复核。"""
        area = store.get_area(conn, area_id)
        if not area:
            return 0
        affected = store.active_reviews_for_area(conn, area_id)
        for item in affected:
            store.mark_review_stale(conn, item["review_id"])
            store.update_record_status(conn, item["record_id"], "pending_review")
            store.audit(conn, item["incident_id"], actor, "recon.conclusion_stale",
                        {"record_id": item["record_id"], "area_id": area_id, "area_version": area["version"]})
        return len(affected)

    # ---- 对账状态：含升级前的历史数据 ----

    def status(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with store.connect(self.db_path) as conn:
            records = store.records_with_area(conn)
            reviews = store.list_reviews(conn)
            batches = store.list_batches(conn)
            legacy_clues = store.legacy_clues(conn)
            legacy_batches = store.legacy_batches(conn)
            version = store.schema_version(conn)
        active_review: dict[int, sqlite3.Row] = {}
        stale_count: dict[int, int] = {}
        for review in reviews:
            if review["status"] == "active":
                active_review[review["record_id"]] = review
            else:
                stale_count[review["record_id"]] = stale_count.get(review["record_id"], 0) + 1
        counts = {"merged": 0, "pending_review": 0, "confirmed": 0, "dismissed": 0}
        items = []
        for record in records:
            counts[record["recon_status"]] = counts.get(record["recon_status"], 0) + 1
            review = active_review.get(record["id"])
            items.append({
                "id": record["id"],
                "client_event_id": record["client_event_id"],
                "client_batch_id": record["client_batch_id"],
                "incident_id": record["incident_id"],
                "area_id": record["area_id"],
                "area_code": record["area_code"],
                "area_version_seen": record["area_version_seen"],
                "current_area_version": record["current_area_version"],
                "asset_name": record["asset_name"],
                "swept_pct": record["swept_pct"],
                "contacts": record["contacts"],
                "note": record["note"],
                "recorded_at": record["recorded_at"],
                "recon_status": record["recon_status"],
                "diff_reasons": json.loads(record["diff_reasons"]),
                "active_review": dict(review) if review else None,
                "stale_reviews": stale_count.get(record["id"], 0),
            })
        return {
            "schema_version": version,
            "summary": {**counts, "stale_conclusions": sum(stale_count.values())},
            "records": items,
            "batches": [
                {"client_batch_id": b["client_batch_id"], "actor": b["actor"], "status": b["status"],
                 "summary": json.loads(b["summary"]), "received_at": b["received_at"], "updated_at": b["updated_at"]}
                for b in batches
            ],
            "legacy": {
                "clues": [
                    {"id": c["id"], "client_event_id": c["client_event_id"], "incident_id": c["incident_id"],
                     "clue_status": c["status"], "recorded_at": c["recorded_at"], "recon_status": "legacy_merged"}
                    for c in legacy_clues
                ],
                "offline_batches": [
                    {"client_batch_id": b["client_batch_id"], "status": b["status"],
                     "received_at": b["received_at"], "merged_at": b["merged_at"],
                     "summary": json.loads(b["summary"]), "recon_status": "legacy_merged"}
                    for b in legacy_batches
                ],
            },
        }
