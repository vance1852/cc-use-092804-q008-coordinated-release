"""控制软件与 AI 模型协同发布的事务用例。

发布候选不可变：四类制品摘要、目标机型、依赖范围、迁移步骤和可回退版本
在创建时固化为内容摘要。样机批次先收齐控制与 AI 两侧验收，再由相互独立
的负责人批准扩大范围；任一制品变化都会产生新候选并使旧候选未完成签署
失效。失败只能回退到已证明兼容的完整组合。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Mapping, Sequence

from .clock import SystemClock, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .gating import (
    approval_gate,
    canonical_json,
    device_gate,
    digest,
    prototype_gate,
    rollback_violations,
)
from .models import ARTIFACT_KINDS, ARTIFACT_LABELS, SIDES, CandidateDefinition, identifier, required_text
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"candidate.create", "device.register", "report.read"},
    "control_acceptor": {"acceptance.control"},
    "ai_acceptor": {"acceptance.ai"},
    "control_owner": {"approval.control"},
    "ai_owner": {"approval.ai"},
    "fleet_operator": {
        "rollout.start",
        "receipt.report",
        "rollback.execute",
        "field_action.complete",
        "report.read",
    },
    "auditor": {"report.read", "audit.read"},
}

NON_TERMINAL_MIGRATIONS = ("migrating", "failed", "halted")


class ReleaseService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM release_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO release_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def _idempotent(self, scope: str, key: str, request: Mapping[str, Any]) -> dict[str, Any] | None:
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency WHERE scope=? AND idempotency_key=?",
            (scope, key),
        ).fetchone()
        if stored is None:
            return None
        if stored["request_sha256"] != digest(request):
            raise Conflict("幂等键对应不同请求内容")
        return json.loads(stored["response_json"])

    def _store_idempotent(
        self, scope: str, key: str, request: Mapping[str, Any], response: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
            "VALUES(?,?,?,?,?)",
            (scope, key, digest(request), canonical_json(response), self._now()),
        )

    def _candidate_row(self, candidate_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_candidates WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if row is None:
            raise NotFound("发布候选不存在")
        return row

    @staticmethod
    def _definition(row: sqlite3.Row) -> CandidateDefinition:
        return CandidateDefinition.from_dict(json.loads(row["definition_json"]))

    def _prototype_devices(self, definition: CandidateDefinition) -> list[str]:
        placeholders = ",".join("?" for _ in definition.target_models)
        rows = self.connection.execute(
            f"SELECT device_id FROM devices WHERE batch_id=? AND active=1 "
            f"AND model IN ({placeholders}) ORDER BY device_id",
            (definition.prototype_batch, *definition.target_models),
        ).fetchall()
        return [row["device_id"] for row in rows]

    def _acceptance_rows(self, candidate_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM candidate_acceptances WHERE candidate_id=? ORDER BY device_id,side",
            (candidate_id,),
        ).fetchall()

    def _approval_rows(self, candidate_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM candidate_approvals WHERE candidate_id=? ORDER BY side",
            (candidate_id,),
        ).fetchall()

    def _incomplete_sign_offs(self, candidate_id: str) -> dict[str, Any]:
        row = self._candidate_row(candidate_id)
        definition = self._definition(row)
        gate = prototype_gate(self._prototype_devices(definition), self._acceptance_rows(candidate_id))
        missing_acceptances = [
            f"{device['device_id']}:{side}"
            for device in gate["devices"]
            for side in SIDES
            if device[side] is None
        ]
        approved_sides = {row["side"] for row in self._approval_rows(candidate_id)}
        return {
            "missing_acceptances": missing_acceptances,
            "missing_approvals": [side for side in SIDES if side not in approved_sides],
        }

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    def register_device(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "device.register")
        device_id = identifier(raw.get("device_id"), "device_id")
        model = required_text(raw.get("model"), "model", 64)
        batch_id = identifier(raw.get("batch_id"), "batch_id")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO devices(device_id,model,batch_id,created_by,created_at) VALUES(?,?,?,?,?)",
                    (device_id, model, batch_id, actor_id, self._now()),
                )
                self._audit("device", device_id, "device.registered", actor_id, {"model": model, "batch_id": batch_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("设备编号已经存在") from exc
        return {"device_id": device_id, "model": model, "batch_id": batch_id}

    def create_candidate(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "candidate.create")
        candidate_id = identifier(raw.get("candidate_id"), "candidate_id")
        release_line = identifier(raw.get("release_line"), "release_line")
        idem_key = required_text(raw.get("idempotency_key"), "idempotency_key", 128)
        definition = CandidateDefinition.from_dict(raw)
        stored = self._idempotent("candidate", idem_key, raw)
        if stored is not None:
            return stored
        content_sha256 = digest({"release_line": release_line, **definition.as_dict()})
        now = self._now()
        previous_rows = self.connection.execute(
            "SELECT candidate_id FROM release_candidates WHERE release_line=? AND state IN ('open','approved') "
            "ORDER BY created_at,rowid",
            (release_line,),
        ).fetchall()
        response = {
            "candidate_id": candidate_id,
            "release_line": release_line,
            "state": "open",
            "content_sha256": content_sha256,
            "superseded": [row["candidate_id"] for row in previous_rows],
        }
        try:
            with transaction(self.connection, immediate=True):
                for previous in previous_rows:
                    incomplete = self._incomplete_sign_offs(previous["candidate_id"])
                    self.connection.execute(
                        "UPDATE release_candidates SET state='superseded',superseded_by=?,revision=revision+1 "
                        "WHERE candidate_id=? AND state IN ('open','approved')",
                        (candidate_id, previous["candidate_id"]),
                    )
                    halted = self.connection.execute(
                        "UPDATE device_migrations SET state='halted',ended_at=? "
                        "WHERE candidate_id=? AND state='migrating'",
                        (now, previous["candidate_id"]),
                    ).rowcount
                    self._audit(
                        "candidate",
                        previous["candidate_id"],
                        "candidate.superseded",
                        actor_id,
                        {
                            "superseded_by": candidate_id,
                            "reason": "artifact_changed",
                            "incomplete_sign_offs": incomplete,
                            "halted_migrations": halted,
                        },
                    )
                self.connection.execute(
                    "INSERT INTO release_candidates(candidate_id,release_line,definition_json,content_sha256,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?)",
                    (candidate_id, release_line, canonical_json(definition.as_dict()), content_sha256, actor_id, now),
                )
                self._store_idempotent("candidate", idem_key, raw, response)
                self._audit(
                    "candidate",
                    candidate_id,
                    "candidate.created",
                    actor_id,
                    {"release_line": release_line, "content_sha256": content_sha256},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("候选编号或幂等键冲突") from exc
        return response

    def record_acceptance(self, actor_id: str, candidate_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        side = required_text(raw.get("side"), "side", 16)
        if side not in SIDES:
            raise ValidationFailed("side 必须是 control 或 ai")
        self._require(actor_id, f"acceptance.{side}")
        device_id = identifier(raw.get("device_id"), "device_id")
        result = required_text(raw.get("result"), "result", 8)
        if result not in ("pass", "fail"):
            raise ValidationFailed("result 必须是 pass 或 fail")
        summary = required_text(raw.get("summary"), "summary", 512)
        idem_key = required_text(raw.get("idempotency_key"), "idempotency_key", 128)
        stored = self._idempotent("acceptance", idem_key, raw)
        if stored is not None:
            return stored
        row = self._candidate_row(candidate_id)
        if row["state"] != "open":
            raise InvalidState("候选不在样机验收阶段，未完成签署已失效")
        definition = self._definition(row)
        device = self.connection.execute(
            "SELECT * FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        if device is None or not device["active"]:
            raise NotFound("设备不存在")
        if device["batch_id"] != definition.prototype_batch or device["model"] not in definition.target_models:
            raise Conflict("设备不在候选指定的样机批次或目标机型范围")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO candidate_acceptances(candidate_id,device_id,side,result,summary,content_sha256,"
                    "idempotency_key,recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        candidate_id,
                        device_id,
                        side,
                        result,
                        summary,
                        row["content_sha256"],
                        idem_key,
                        actor_id,
                        self._now(),
                    ),
                )
                gate = prototype_gate(self._prototype_devices(definition), self._acceptance_rows(candidate_id))
                response = {
                    "candidate_id": candidate_id,
                    "device_id": device_id,
                    "side": side,
                    "result": result,
                    "prototype_gate_complete": gate["complete"],
                }
                self._store_idempotent("acceptance", idem_key, raw, response)
                self._audit(
                    "candidate",
                    candidate_id,
                    "acceptance.recorded",
                    actor_id,
                    {"device_id": device_id, "side": side, "result": result},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该设备该侧验收摘要已归并") from exc
        return response

    def approve_candidate(self, actor_id: str, candidate_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        side = required_text(raw.get("side"), "side", 16)
        if side not in SIDES:
            raise ValidationFailed("side 必须是 control 或 ai")
        self._require(actor_id, f"approval.{side}")
        note = required_text(raw.get("note"), "note", 512)
        row = self._candidate_row(candidate_id)
        if row["state"] != "open":
            raise InvalidState("候选不在待批准状态，未完成签署已失效")
        if row["created_by"] == actor_id:
            raise Forbidden("负责人不能批准自己创建的候选")
        definition = self._definition(row)
        gate = prototype_gate(self._prototype_devices(definition), self._acceptance_rows(candidate_id))
        if not gate["complete"]:
            raise InvalidState("样机批次两侧验收未全部通过，不能批准扩大范围")
        other = self.connection.execute(
            "SELECT approver_id FROM candidate_approvals WHERE candidate_id=? AND side<>?",
            (candidate_id, side),
        ).fetchone()
        if other is not None and other["approver_id"] == actor_id:
            raise Forbidden("两侧负责人必须相互独立")
        approved = False
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO candidate_approvals(candidate_id,side,approver_id,note,content_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (candidate_id, side, actor_id, note, row["content_sha256"], self._now()),
                )
                count = self.connection.execute(
                    "SELECT count(*) AS total FROM candidate_approvals WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()["total"]
                if count == 2:
                    self.connection.execute(
                        "UPDATE release_candidates SET state='approved',revision=revision+1 "
                        "WHERE candidate_id=? AND state='open'",
                        (candidate_id,),
                    )
                    approved = True
                    self._audit(
                        "candidate",
                        candidate_id,
                        "candidate.approved",
                        actor_id,
                        {"content_sha256": row["content_sha256"]},
                    )
                self._audit("candidate", candidate_id, "approval.recorded", actor_id, {"side": side})
        except sqlite3.IntegrityError as exc:
            raise Conflict("该侧负责人已批准") from exc
        return {
            "candidate_id": candidate_id,
            "side": side,
            "state": "approved" if approved else "open",
        }

    def start_rollout(self, actor_id: str, candidate_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "rollout.start")
        raw_ids = raw.get("device_ids")
        if not isinstance(raw_ids, list) or not raw_ids:
            raise ValidationFailed("device_ids 必须是非空列表")
        device_ids = [identifier(item, "device_ids") for item in raw_ids]
        if len(set(device_ids)) != len(device_ids):
            raise ValidationFailed("device_ids 存在重复设备")
        idem_key = required_text(raw.get("idempotency_key"), "idempotency_key", 128)
        stored = self._idempotent("rollout", idem_key, raw)
        if stored is not None:
            return stored
        row = self._candidate_row(candidate_id)
        if row["state"] != "approved":
            raise InvalidState("候选尚未通过独立负责人批准，不能扩大范围")
        definition = self._definition(row)
        for device_id in device_ids:
            device = self.connection.execute(
                "SELECT * FROM devices WHERE device_id=?", (device_id,)
            ).fetchone()
            if device is None or not device["active"]:
                raise NotFound(f"设备不存在: {device_id}")
            if device["model"] not in definition.target_models:
                raise Conflict(f"设备 {device_id} 机型不在目标范围")
            busy = self.connection.execute(
                "SELECT migration_id FROM device_migrations WHERE device_id=? AND state IN ('migrating','failed','halted')",
                (device_id,),
            ).fetchone()
            if busy is not None:
                raise Conflict(f"设备 {device_id} 存在未闭环迁移")
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                for device_id in device_ids:
                    cursor = self.connection.execute(
                        "INSERT INTO device_migrations(device_id,candidate_id,started_by,started_at) VALUES(?,?,?,?)",
                        (device_id, candidate_id, actor_id, now),
                    )
                    self._audit(
                        "migration",
                        str(int(cursor.lastrowid)),
                        "rollout.started",
                        actor_id,
                        {"device_id": device_id, "candidate_id": candidate_id},
                    )
                response = {
                    "candidate_id": candidate_id,
                    "started": list(device_ids),
                    "total_steps": definition.total_steps,
                }
                self._store_idempotent("rollout", idem_key, raw, response)
        except sqlite3.IntegrityError as exc:
            raise Conflict("设备已在该候选的推广记录中") from exc
        return response

    def report_receipt(
        self, actor_id: str, candidate_id: str, device_id: str, raw: Mapping[str, Any]
    ) -> dict[str, Any]:
        self._require(actor_id, "receipt.report")
        step = raw.get("step")
        if isinstance(step, bool) or not isinstance(step, int) or step < 1:
            raise ValidationFailed("step 必须是正整数")
        status = required_text(raw.get("status"), "status", 8)
        if status not in ("done", "failed"):
            raise ValidationFailed("status 必须是 done 或 failed")
        detail = required_text(raw.get("detail"), "detail", 512)
        idem_key = required_text(raw.get("idempotency_key"), "idempotency_key", 128)
        stored = self._idempotent("receipt", idem_key, raw)
        if stored is not None:
            return stored
        row = self._candidate_row(candidate_id)
        definition = self._definition(row)
        migration = self.connection.execute(
            "SELECT * FROM device_migrations WHERE device_id=? AND candidate_id=?",
            (device_id, candidate_id),
        ).fetchone()
        if migration is None:
            raise NotFound("设备没有该候选的迁移记录")
        if migration["state"] != "migrating":
            raise InvalidState("迁移不在进行中，回执无法归并")
        merged = self.connection.execute(
            "SELECT 1 FROM step_receipts WHERE candidate_id=? AND device_id=? AND step=?",
            (candidate_id, device_id, step),
        ).fetchone()
        if merged is not None:
            raise Conflict("该设备该步骤回执已归并")
        expected = migration["current_step"] + 1
        if step != expected or step > definition.total_steps:
            raise InvalidState(f"必须按顺序上报第 {expected} 步回执")
        now = self._now()
        completed = status == "done" and step == definition.total_steps
        migration_state = "migrating" if status == "done" else "failed"
        if completed:
            migration_state = "completed"
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO step_receipts(candidate_id,device_id,step,status,detail,idempotency_key,"
                    "reported_by,reported_at) VALUES(?,?,?,?,?,?,?,?)",
                    (candidate_id, device_id, step, status, detail, idem_key, actor_id, now),
                )
                if status == "done":
                    if completed:
                        self.connection.execute(
                            "UPDATE device_migrations SET state='completed',current_step=?,ended_at=? "
                            "WHERE migration_id=?",
                            (step, now, migration["migration_id"]),
                        )
                        self.connection.execute(
                            "INSERT INTO device_combinations(device_id,candidate_id,reason,created_at) "
                            "VALUES(?,?,?,?)",
                            (device_id, candidate_id, "migration_completed", now),
                        )
                        self._audit(
                            "migration",
                            str(migration["migration_id"]),
                            "migration.completed",
                            actor_id,
                            {"device_id": device_id, "candidate_id": candidate_id},
                        )
                    else:
                        self.connection.execute(
                            "UPDATE device_migrations SET current_step=? WHERE migration_id=?",
                            (step, migration["migration_id"]),
                        )
                else:
                    self.connection.execute(
                        "UPDATE device_migrations SET state='failed',failed_step=?,failure_detail=?,ended_at=? "
                        "WHERE migration_id=?",
                        (step, detail, now, migration["migration_id"]),
                    )
                    self._audit(
                        "migration",
                        str(migration["migration_id"]),
                        "migration.step_failed",
                        actor_id,
                        {"device_id": device_id, "step": step, "detail": detail},
                    )
                response = {
                    "candidate_id": candidate_id,
                    "device_id": device_id,
                    "step": step,
                    "status": status,
                    "migration_state": migration_state,
                    "current_step": step if status == "done" else migration["current_step"],
                    "total_steps": definition.total_steps,
                }
                self._store_idempotent("receipt", idem_key, raw, response)
                self._audit(
                    "migration",
                    str(migration["migration_id"]),
                    "receipt.recorded",
                    actor_id,
                    {"device_id": device_id, "step": step, "status": status},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该设备该步骤回执已归并") from exc
        return response

    def rollback_device(
        self, actor_id: str, candidate_id: str, device_id: str, raw: Mapping[str, Any]
    ) -> dict[str, Any]:
        self._require(actor_id, "rollback.execute")
        target_id = identifier(raw.get("target_candidate_id"), "target_candidate_id")
        idem_key = required_text(raw.get("idempotency_key"), "idempotency_key", 128)
        stored = self._idempotent("rollback", idem_key, raw)
        if stored is not None:
            return stored
        source_row = self._candidate_row(candidate_id)
        migration = self.connection.execute(
            "SELECT * FROM device_migrations WHERE device_id=? AND candidate_id=?",
            (device_id, candidate_id),
        ).fetchone()
        if migration is None:
            raise NotFound("设备没有该候选的迁移记录")
        if migration["state"] not in ("failed", "halted"):
            raise InvalidState("只有失败或被取代的迁移可以回退")
        if target_id == candidate_id:
            raise ValidationFailed("回退目标不能是源候选")
        target_row = self._candidate_row(target_id)
        if target_row["release_line"] != source_row["release_line"]:
            raise Conflict("回退目标必须在同一发布线内")
        approvals = self.connection.execute(
            "SELECT count(*) AS total FROM candidate_approvals WHERE candidate_id=?",
            (target_id,),
        ).fetchone()["total"]
        if approvals != 2:
            raise Conflict("回退目标不是已证明兼容的组合")
        source_def = self._definition(source_row)
        target_def = self._definition(target_row)
        violations = rollback_violations(source_def, target_def)
        if violations:
            labels = "、".join(ARTIFACT_LABELS[kind] for kind in violations)
            raise Conflict(f"回退目标超出源候选声明的可回退版本: {labels}")
        if migration["state"] == "failed" and migration["failed_step"]:
            upto = int(migration["failed_step"])
        else:
            upto = int(migration["current_step"])
        actions = [
            action
            for step in reversed(source_def.migration_steps[:upto])
            for action in step.rollback_actions
        ]
        now = self._now()
        with transaction(self.connection, immediate=True):
            action_ids: list[int] = []
            for description in actions:
                cursor = self.connection.execute(
                    "INSERT INTO field_actions(device_id,migration_id,description,created_at) VALUES(?,?,?,?)",
                    (device_id, migration["migration_id"], description, now),
                )
                action_ids.append(int(cursor.lastrowid))
            self.connection.execute(
                "UPDATE device_migrations SET state='rolled_back',ended_at=? WHERE migration_id=?",
                (now, migration["migration_id"]),
            )
            self.connection.execute(
                "INSERT INTO device_combinations(device_id,candidate_id,reason,created_at) VALUES(?,?,?,?)",
                (device_id, target_id, "rollback", now),
            )
            response = {
                "device_id": device_id,
                "from_candidate": candidate_id,
                "to_candidate": target_id,
                "combination": {kind: target_def.artifacts[kind].as_dict() for kind in ARTIFACT_KINDS},
                "field_actions": [
                    {"action_id": action_id, "description": description, "state": "pending"}
                    for action_id, description in zip(action_ids, actions)
                ],
            }
            self._store_idempotent("rollback", idem_key, raw, response)
            self._audit(
                "migration",
                str(migration["migration_id"]),
                "migration.rolled_back",
                actor_id,
                {
                    "device_id": device_id,
                    "from_candidate": candidate_id,
                    "to_candidate": target_id,
                    "field_actions": len(actions),
                },
            )
        return response

    def complete_field_action(self, actor_id: str, action_id: int, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "field_action.complete")
        note = raw.get("note")
        if note is not None:
            note = required_text(note, "note", 512)
        row = self.connection.execute(
            "SELECT * FROM field_actions WHERE action_id=?", (action_id,)
        ).fetchone()
        if row is None:
            raise NotFound("现场动作不存在")
        if row["state"] == "done":
            return {
                "action_id": action_id,
                "state": "done",
                "completed_by": row["completed_by"],
                "completed_at": row["completed_at"],
            }
        now = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE field_actions SET state='done',completed_by=?,completed_at=? "
                "WHERE action_id=? AND state='pending'",
                (actor_id, now, action_id),
            )
            self._audit(
                "field_action",
                str(action_id),
                "field_action.completed",
                actor_id,
                {"device_id": row["device_id"], "note": note},
            )
        return {"action_id": action_id, "state": "done", "completed_by": actor_id, "completed_at": now}

    def candidate_status(self, actor_id: str, candidate_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        row = self._candidate_row(candidate_id)
        definition = self._definition(row)
        gate = prototype_gate(self._prototype_devices(definition), self._acceptance_rows(candidate_id))
        approvals = approval_gate(self._approval_rows(candidate_id))
        migrations = self.connection.execute(
            "SELECT * FROM device_migrations WHERE candidate_id=? ORDER BY migration_id",
            (candidate_id,),
        ).fetchall()
        devices = []
        for migration in migrations:
            pending = self.connection.execute(
                "SELECT count(*) AS total FROM field_actions WHERE migration_id=? AND state='pending'",
                (migration["migration_id"],),
            ).fetchone()["total"]
            devices.append(
                {
                    "device_id": migration["device_id"],
                    "state": migration["state"],
                    "current_step": migration["current_step"],
                    "failure_detail": migration["failure_detail"],
                    "gate": device_gate(
                        candidate_state=row["state"],
                        prototype_complete=gate["complete"],
                        migration_state=migration["state"],
                        next_step=migration["current_step"] + 1,
                        total_steps=definition.total_steps,
                        pending_field_actions=pending,
                    ),
                    "started_at": migration["started_at"],
                    "ended_at": migration["ended_at"],
                }
            )
        return {
            "candidate_id": candidate_id,
            "release_line": row["release_line"],
            "state": row["state"],
            "superseded_by": row["superseded_by"],
            "content_sha256": row["content_sha256"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "definition": definition.as_dict(),
            "gates": {
                "prototype_acceptance": gate,
                "independent_approval": approvals,
                "rollout": {"total_steps": definition.total_steps, "devices": devices},
            },
        }

    def _focus_candidate(
        self,
        device: sqlite3.Row,
        migrations: Sequence[sqlite3.Row],
        combination_row: sqlite3.Row | None,
    ) -> sqlite3.Row | None:
        lines: list[str] = []
        if migrations:
            lines.append(self._candidate_row(migrations[0]["candidate_id"])["release_line"])
        if combination_row is not None:
            lines.append(self._candidate_row(combination_row["candidate_id"])["release_line"])
        if lines:
            placeholders = ",".join("?" for _ in lines)
            return self.connection.execute(
                f"SELECT * FROM release_candidates WHERE release_line IN ({placeholders}) "
                "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                tuple(lines),
            ).fetchone()
        rows = self.connection.execute(
            "SELECT * FROM release_candidates WHERE state IN ('open','approved') "
            "ORDER BY created_at DESC,rowid DESC"
        ).fetchall()
        for row in rows:
            if device["model"] in self._definition(row).target_models:
                return row
        return None

    def _device_view(self, device: sqlite3.Row) -> dict[str, Any]:
        device_id = device["device_id"]
        migrations = self.connection.execute(
            "SELECT * FROM device_migrations WHERE device_id=? ORDER BY migration_id DESC",
            (device_id,),
        ).fetchall()
        combination_row = self.connection.execute(
            "SELECT * FROM device_combinations WHERE device_id=? ORDER BY combination_id DESC LIMIT 1",
            (device_id,),
        ).fetchone()
        combination = None
        if combination_row is not None:
            source = self._candidate_row(combination_row["candidate_id"])
            source_def = self._definition(source)
            combination = {
                "candidate_id": combination_row["candidate_id"],
                "reason": combination_row["reason"],
                "since": combination_row["created_at"],
                "artifacts": {kind: source_def.artifacts[kind].as_dict() for kind in ARTIFACT_KINDS},
            }
        pending_rows = self.connection.execute(
            "SELECT * FROM field_actions WHERE device_id=? AND state='pending' ORDER BY action_id",
            (device_id,),
        ).fetchall()
        active = next(
            (item for item in migrations if item["state"] in NON_TERMINAL_MIGRATIONS), None
        )
        if active is not None:
            focus = self._candidate_row(active["candidate_id"])
            focus_migration: sqlite3.Row | None = active
        else:
            focus = self._focus_candidate(device, migrations, combination_row)
            focus_migration = None
            if focus is not None:
                focus_migration = next(
                    (item for item in migrations if item["candidate_id"] == focus["candidate_id"]),
                    None,
                )
        gate = "not_in_scope"
        focus_summary = None
        migration_summary = None
        if focus is not None:
            definition = self._definition(focus)
            prototype = prototype_gate(
                self._prototype_devices(definition), self._acceptance_rows(focus["candidate_id"])
            )
            gate = device_gate(
                candidate_state=focus["state"],
                prototype_complete=prototype["complete"],
                migration_state=None if focus_migration is None else focus_migration["state"],
                next_step=None if focus_migration is None else focus_migration["current_step"] + 1,
                total_steps=definition.total_steps,
                pending_field_actions=len(pending_rows),
            )
            focus_summary = {"candidate_id": focus["candidate_id"], "state": focus["state"]}
        if focus_migration is not None:
            migration_summary = {
                "migration_id": focus_migration["migration_id"],
                "candidate_id": focus_migration["candidate_id"],
                "state": focus_migration["state"],
                "current_step": focus_migration["current_step"],
                "failure_detail": focus_migration["failure_detail"],
            }
        return {
            "device_id": device_id,
            "model": device["model"],
            "batch_id": device["batch_id"],
            "combination": combination,
            "gate": gate,
            "focus_candidate": focus_summary,
            "migration": migration_summary,
            "pending_field_actions": [
                {
                    "action_id": row["action_id"],
                    "description": row["description"],
                    "created_at": row["created_at"],
                }
                for row in pending_rows
            ],
        }

    def device_status(self, actor_id: str, device_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        device = self.connection.execute(
            "SELECT * FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        if device is None:
            raise NotFound("设备不存在")
        return self._device_view(device)

    def fleet_status(self, actor_id: str, release_line: str | None = None) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        devices = self.connection.execute(
            "SELECT * FROM devices WHERE active=1 ORDER BY device_id"
        ).fetchall()
        result: dict[str, Any] = {"devices": [self._device_view(device) for device in devices]}
        if release_line is not None:
            candidates = self.connection.execute(
                "SELECT * FROM release_candidates WHERE release_line=? ORDER BY created_at,rowid",
                (release_line,),
            ).fetchall()
            result["release_line"] = release_line
            result["candidates"] = [
                {
                    "candidate_id": row["candidate_id"],
                    "state": row["state"],
                    "content_sha256": row["content_sha256"],
                    "superseded_by": row["superseded_by"],
                }
                for row in candidates
            ]
        return result

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM release_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
