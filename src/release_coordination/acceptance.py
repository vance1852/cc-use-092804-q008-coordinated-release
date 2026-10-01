"""协同发布全流程离线验收：基线、候选、样机双验、双负责人批准、扩大、失败回退。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import ReleaseCoordinator


def _artifact(kind: str, version: str, tag: str) -> dict[str, str]:
    return {
        "kind": kind,
        "artifact_id": f"art-{kind.replace('_', '-')}",
        "version": version,
        "sha256": f"{kind}-{tag}".encode().hex().ljust(64, "0")[:64],
        "summary": f"{kind} {version} 制品摘要",
    }


def _manifest(tag: str, *, rollback: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "artifacts": [
            _artifact("control_os", "3.4.0", tag),
            _artifact("network_config", "2.1.0", tag),
            _artifact("toolchain_output", "5.0.1", tag),
            _artifact("ai_model", "2026.09.2", tag),
        ],
        "target_models": ["RX-1", "RX-2"],
        "dependency_scope": [
            {"kind": "control_os", "constraint": ">=3.4,<4"},
            {"kind": "network_config", "constraint": "=2.1.0"},
            {"kind": "toolchain_output", "constraint": "=5.0.1（含标定参数集 2026-09）"},
            {"kind": "ai_model", "constraint": "=2026.09.2（与标定参数集 2026-09 配对）"},
        ],
        "migration_steps": [
            {"step_id": "flash-os", "side": "control", "description": "刷写控制操作系统 3.4.0",
             "rollback_field_action": "恢复出厂 OS 启动分区并重新上电自检"},
            {"step_id": "apply-net", "side": "control", "description": "下发实时总线网络配置 2.1.0",
             "rollback_field_action": "恢复出厂总线配置并确认总线周期"},
            {"step_id": "apply-params", "side": "control", "description": "导入工具链控制参数与标定表 5.0.1",
             "rollback_field_action": "现场重新执行关节零点标定"},
            {"step_id": "load-model", "side": "ai", "description": "装载 AI 模型 2026.09.2",
             "rollback_field_action": ""},
        ],
        "rollback_refs": rollback or [],
    }


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = ReleaseCoordinator(connection, FrozenClock(datetime(2026, 9, 28, 8, 0, tzinfo=timezone.utc)))

    for user_id, role in (
        ("releng", "release_engineer"),
        ("ctl", "control_owner"),
        ("ai", "ai_owner"),
        ("field", "field_tech"),
        ("audit", "auditor"),
    ):
        service.create_user(user_id, user_id, role)

    service.create_batch("releng", "pilot-batch", "RX-1", "首台全国产架构样机批次")
    service.create_batch("releng", "fleet-batch", "RX-2", "小批量交付批次")
    for serial in ("RX-1-001", "RX-1-002"):
        service.register_device("releng", serial, "pilot-batch")
    for serial in ("RX-2-101", "RX-2-102"):
        service.register_device("releng", serial, "fleet-batch")

    # 工厂已证明兼容组合（回退树根）。
    baseline = service.register_baseline("releng", "robot-combo", _manifest("factory"))
    # 小批量发布候选，只能回退到工厂基线整体组合。
    candidate = service.create_candidate(
        "releng", "robot-combo",
        _manifest("release", rollback=[{"candidate_id": "robot-combo", "revision": baseline["revision"]}]),
    )
    cid, rev = candidate["candidate_id"], candidate["revision"]

    # 指定样机批次先收集控制与 AI 两侧验收摘要。
    pilot = service.open_pilot_wave("releng", "wave-pilot", cid, rev, "pilot-batch", "idem-pilot")
    for serial in ("RX-1-001", "RX-1-002"):
        for step in ("flash-os", "apply-net", "apply-params"):
            service.record_receipt("field", pilot["wave_id"], serial, step, "ok",
                                   {"calibration_check": "pass"}, f"idem-{serial}-{step}")
        service.record_receipt("field", pilot["wave_id"], serial, "load-model", "ok",
                               {"motion_drift_mm": "0.4"})

    # 未完成两侧签署时扩大范围被门禁拒绝。
    blocked = None
    try:
        service.open_expansion_wave("releng", "wave-fleet", cid, rev, "fleet-batch", "idem-fleet")
    except Exception as exc:  # noqa: BLE001 - 验收脚本需要保留拒绝原因
        blocked = str(exc)

    # 两个相互独立的负责人分别批准。
    service.approve("ctl", cid, rev, "control", "控制侧验收通过")
    service.approve("ai", cid, rev, "ai", "AI 侧验收通过")
    expansion = service.open_expansion_wave("releng", "wave-fleet", cid, rev, "fleet-batch", "idem-fleet")

    # 扩大波次中一台设备在参数步骤失败（模拟只更模型未跟参数的偏差场景被检出）。
    good, bad = "RX-2-101", "RX-2-102"
    for step in ("flash-os", "apply-net", "apply-params", "load-model"):
        service.record_receipt("field", expansion["wave_id"], good, step, "ok", {"check": "pass"})
    for step in ("flash-os", "apply-net"):
        service.record_receipt("field", expansion["wave_id"], bad, step, "ok", {"check": "pass"})
    service.record_receipt("field", expansion["wave_id"], bad, "apply-params", "failed",
                           {"calibration_error": "关节 3 轨迹偏离标定 12mm"})

    # 只能回退到候选声明、已证明兼容的整体组合；回退后产生现场动作。
    rollback = service.rollback_device(
        "releng", bad, "robot-combo", baseline["revision"], "参数步骤验收失败", "idem-rollback-102")
    for action in rollback["pending_field_actions"]:
        row = service.connection.execute(
            "SELECT action_id FROM release_field_actions WHERE step_id=? AND device_serial=?",
            (action["step_id"], bad),
        ).fetchone()
        service.complete_field_action("field", row["action_id"])

    device_view = service.device_status(bad)
    result = {
        "status": "ok",
        "baseline": baseline,
        "candidate": service.candidate(cid, rev),
        "expansion_blocked_before_approvals": blocked,
        "pilot_report": service.wave_report("wave-pilot"),
        "expansion_report": service.wave_report("wave-fleet"),
        "failed_device": device_view,
        "audit": service.audit_chain("audit"),
        "workspace": workspace.name,
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行控制与 AI 协同发布离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
