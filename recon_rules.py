"""回传对账规则层：纯函数决策，不接触数据库与 HTTP。

输入现场记录和调度端当前状态，输出对账结论：
- merge：与实时调度一致，直接并入；
- park：存在差异，保留现场数据并停在待复核，reasons 列出两端值与差异原因。
"""
from __future__ import annotations

from typing import Any

CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
AREA_OPEN_FOR_SWEEP = {"planned", "assigned", "active"}

# 复核结论与记录状态
CONCLUSIONS = {"confirmed", "dismissed"}
REVIEWABLE_STATUS = {"pending_review"}
RECORD_STATUSES = {"merged", "pending_review", "confirmed", "dismissed"}


def evaluate_record(record: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """比对现场记录与调度端当前状态，返回 {"decision": "merge"|"park", "reasons": [...]}。

    record 至少包含 area_version_seen、asset_name、recorded_at；
    context 包含 incident、area、asset（区域当前分配的资源，可为 None）。
    """
    reasons: list[dict[str, Any]] = []
    incident = context.get("incident")
    area = context.get("area")
    asset = context.get("asset")

    if incident and incident["status"] in CLOSED_INCIDENT:
        reasons.append({
            "code": "incident_closed",
            "message": "事件已结束，现场数据保留待复核，不再并入实时调度",
            "field_value": {"recorded_at": record.get("recorded_at"), "expected": "在救事件"},
            "current_value": {"incident_status": incident["status"]},
        })

    if area is not None:
        current_asset = asset["name"] if asset else None
        if int(record["area_version_seen"]) != int(area["version"]):
            reasons.append({
                "code": "area_version_changed",
                "message": "区域版本已变化，可能已被改派，以调度端当前版本为准",
                "field_value": {"area_version": int(record["area_version_seen"]), "asset": record.get("asset_name") or None},
                "current_value": {"area_version": int(area["version"]), "asset": current_asset},
            })
        if area["status"] not in AREA_OPEN_FOR_SWEEP:
            reasons.append({
                "code": "area_finished",
                "message": "搜索区域已结束，迟到的扫测结果不再覆盖当前部署",
                "field_value": {"swept_pct": record.get("swept_pct"), "contacts": record.get("contacts")},
                "current_value": {"area_status": area["status"]},
            })
        if record.get("asset_name") and area["assigned_asset_id"] is None:
            reasons.append({
                "code": "resource_released",
                "message": "现场记录的资源已释放，区域当前未分配资源",
                "field_value": {"asset": record["asset_name"]},
                "current_value": {"asset": None},
            })

    return {"decision": "park" if reasons else "merge", "reasons": reasons}


def can_review(recon_status: str) -> bool:
    """只有停在待复核的记录可以登记复核结论。"""
    return recon_status in REVIEWABLE_STATUS


def conclusion_to_status(conclusion: str) -> str:
    if conclusion not in CONCLUSIONS:
        raise ValueError("未知复核结论: %s" % conclusion)
    return conclusion
