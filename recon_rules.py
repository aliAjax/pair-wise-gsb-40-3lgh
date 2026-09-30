"""离线扫测记录的回传对账规则。

纯函数模块：只依赖标准库，不接触数据库与 HTTP。
输入是字典（现场记录、当前事件/区域/资源状态），输出是裁决与差异原因。
数据存取见 recon_store.py，接口编排见 app.py，三层分开维护。
"""
from __future__ import annotations

from typing import Any

CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
CLOSED_AREA = {"completed", "abandoned"}

# 裁决结果
MERGE = "merge"      # 与当前调度一致，直接合并
REVIEW = "review"    # 存在差异，保留现场数据、停在待复核
REJECT = "reject"    # 记录本身有数据错误，修正后可重试

# 差异原因代码
AREA_VERSION_MISMATCH = "area_version_mismatch"
AREA_REASSIGNED = "area_reassigned"
ASSET_RELEASED = "asset_released"
INCIDENT_CLOSED = "incident_closed"
AREA_CLOSED = "area_closed"
INCIDENT_MISSING = "incident_not_found"
AREA_MISSING = "area_not_found"
AREA_INCIDENT_MISMATCH = "area_incident_mismatch"
INVALID_COVERAGE = "invalid_coverage"


def _reason(code: str, field: str, recorded: Any, current: Any, message: str) -> dict[str, Any]:
    """一条差异原因：字段、记录端值、当前端值和说明。"""
    return {"code": code, "field": field, "recorded": recorded, "current": current, "message": message}


def evaluate_sweep(record: dict[str, Any], *, incident: dict[str, Any] | None,
                   area: dict[str, Any] | None, asset: dict[str, Any] | None) -> dict[str, Any]:
    """评估一条扫测记录与当前调度状态是否一致。

    record 至少携带：incident_id、area_id、area_version（记录时的区域版本）、
    asset_id（扫测资源，可空）、coverage_pct。
    返回 {"verdict": merge|review|reject, "reasons": [差异...]}。
    """
    try:
        coverage = float(record.get("coverage_pct"))
    except (TypeError, ValueError):
        return {"verdict": REJECT, "reasons": [_reason(
            INVALID_COVERAGE, "coverage_pct", record.get("coverage_pct"), None,
            "扫测覆盖率必须是数值")]}
    if not 0 <= coverage <= 100:
        return {"verdict": REJECT, "reasons": [_reason(
            INVALID_COVERAGE, "coverage_pct", coverage, "0..100",
            "扫测覆盖率应在 0 到 100 之间")]}

    if incident is None:
        return {"verdict": REJECT, "reasons": [_reason(
            INCIDENT_MISSING, "incident_id", record.get("incident_id"), None,
            "事件不存在")]}
    if area is None:
        return {"verdict": REJECT, "reasons": [_reason(
            AREA_MISSING, "area_id", record.get("area_id"), None,
            "搜索区域不存在")]}
    if area["incident_id"] != incident["id"]:
        return {"verdict": REJECT, "reasons": [_reason(
            AREA_INCIDENT_MISMATCH, "area_id", record.get("area_id"), area["incident_id"],
            "搜索区域不属于该事件")]}

    reasons = []
    if incident["status"] in CLOSED_INCIDENT:
        reasons.append(_reason(
            INCIDENT_CLOSED, "incident_status", None, incident["status"],
            "事件已结束，现场数据保留待复核"))
    if area["status"] in CLOSED_AREA:
        reasons.append(_reason(
            AREA_CLOSED, "area_status", None, area["status"],
            "搜索区域已结束，现场数据保留待复核"))
    recorded_version = record.get("area_version")
    if recorded_version is not None and int(recorded_version) != area["version"]:
        reasons.append(_reason(
            AREA_VERSION_MISMATCH, "area_version", int(recorded_version), area["version"],
            "区域版本已变化：记录时 v%s，当前 v%s" % (recorded_version, area["version"])))
    recorded_asset = record.get("asset_id")
    if recorded_asset is not None and area["assigned_asset_id"] != recorded_asset:
        reasons.append(_reason(
            AREA_REASSIGNED, "assigned_asset_id", recorded_asset, area["assigned_asset_id"],
            "区域已改派：记录时资源 %s，当前资源 %s" % (recorded_asset, area["assigned_asset_id"])))
    if recorded_asset is not None:
        if asset is None:
            reasons.append(_reason(
                ASSET_RELEASED, "asset_status", "assigned", None,
                "扫测资源已不存在"))
        elif asset["status"] != "assigned":
            reasons.append(_reason(
                ASSET_RELEASED, "asset_status", "assigned", asset["status"],
                "扫测资源已释放"))
    return {"verdict": REVIEW if reasons else MERGE, "reasons": reasons}
