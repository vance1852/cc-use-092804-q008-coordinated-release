"""候选门禁、设备门禁与回退目标的确定性判定。"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

from .models import ARTIFACT_KINDS, CandidateDefinition


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def prototype_gate(
    required_devices: Sequence[str],
    acceptances: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """样机批次验收门禁：每台样机都必须收齐控制与 AI 两侧且结论为通过。"""
    devices: dict[str, dict[str, Any]] = {
        device_id: {"control": None, "ai": None} for device_id in required_devices
    }
    for row in acceptances:
        slot = devices.get(row["device_id"])
        if slot is not None and row["side"] in slot:
            slot[row["side"]] = {
                "result": row["result"],
                "summary": row["summary"],
                "recorded_by": row["recorded_by"],
                "recorded_at": row["recorded_at"],
            }
    blocked = any(
        side is not None and side["result"] == "fail"
        for slot in devices.values()
        for side in slot.values()
    )
    complete = bool(required_devices) and not blocked and all(
        slot["control"] is not None
        and slot["control"]["result"] == "pass"
        and slot["ai"] is not None
        and slot["ai"]["result"] == "pass"
        for slot in devices.values()
    )
    return {
        "required_devices": list(required_devices),
        "complete": complete,
        "blocked": blocked,
        "devices": [
            {"device_id": device_id, **slot} for device_id, slot in devices.items()
        ],
    }


def approval_gate(approvals: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """独立负责人门禁：控制与 AI 两侧负责人都签署才算完成。"""
    sides: dict[str, Any] = {"control": None, "ai": None}
    for row in approvals:
        sides[row["side"]] = {
            "approver_id": row["approver_id"],
            "note": row["note"],
            "created_at": row["created_at"],
        }
    return {
        "complete": sides["control"] is not None and sides["ai"] is not None,
        **sides,
    }


def rollback_violations(
    source: CandidateDefinition, target: CandidateDefinition
) -> list[str]:
    """逐类制品检查回退目标是否落在源候选声明的可回退版本内。"""
    violations: list[str] = []
    for kind in ARTIFACT_KINDS:
        wanted = target.artifacts[kind].version
        if wanted == source.artifacts[kind].version:
            continue
        if source.rollback_versions[kind] != wanted:
            violations.append(kind)
    return violations


def device_gate(
    *,
    candidate_state: str | None,
    prototype_complete: bool,
    migration_state: str | None,
    next_step: int | None,
    total_steps: int | None,
    pending_field_actions: int,
) -> str:
    """汇总一台设备当前卡在哪个门禁。"""
    if migration_state == "migrating":
        return f"migration_step_{next_step}_of_{total_steps}"
    if migration_state == "failed":
        return "rollback_required"
    if migration_state == "halted":
        return "candidate_superseded"
    if migration_state == "rolled_back":
        return "field_actions_pending" if pending_field_actions else "rolled_back"
    if migration_state == "completed":
        return "completed"
    if candidate_state == "open":
        return "independent_approval" if prototype_complete else "prototype_acceptance"
    if candidate_state == "approved":
        return "rollout_pending"
    if candidate_state is None:
        return "not_in_scope"
    return candidate_state
