"""协同发布的事务用例。

门禁顺序：

1. 发布候选封存（不可变清单 + 仅可回退到已证明兼容组合的校验）；
2. 指定样机批次（wave 0）收集控制侧与 AI 侧分步骤验收回执；
3. 控制负责人、AI 负责人两个相互独立角色分别签署；
4. 扩大范围（expansion 波次），候选转为 released；
5. 任一设备步骤失败，只能整体回退到候选清单声明、且已发布或
   工厂基线认证的完整组合，不能跨版本拼合，回退后遗留现场动作。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Iterable, Mapping

from .clock import SystemClock, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, digest
from .models import CandidateManifest
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "release_engineer": {
        "baseline.register", "candidate.write", "batch.write", "device.register",
        "wave.pilot", "wave.expansion", "rollback.execute",
    },
    "control_owner": {"approval.control"},
    "ai_owner": {"approval.ai"},
    "field_tech": {"receipt.record", "field_action.complete"},
    "auditor": {"report.read", "audit.read"},
}

SIDE_PERMISSION = {"control": "approval.control", "ai": "approval.ai"}
APPROVAL_ROLE = {"control": "control_owner", "ai": "ai_owner"}


class ReleaseCoordinator:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    # ------------------------------------------------------------------ 基础

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

    def _idempotency(self, scope: str, key: str, raw: Mapping[str, Any]) -> dict[str, Any] | None:
        if not key:
            return None
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency WHERE scope=? AND idempotency_key=?",
            (scope, key),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同请求内容")
            return json.loads(stored["response_json"])
        return None

    def _store_idempotency(self, scope: str, key: str, raw: Mapping[str, Any], response: Mapping[str, Any]) -> None:
        if key:
            self.connection.execute(
                "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (scope, key, digest(raw), canonical_json(response), self._now()),
            )

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

    # ------------------------------------------------------------- 批次/设备

    def create_batch(self, actor_id: str, batch_id: str, machine_model: str, note: str = "") -> dict[str, Any]:
        self._require(actor_id, "batch.write")
        batch_id = batch_id.strip()
        machine_model = machine_model.strip()
        if not batch_id or not machine_model:
            raise ValidationFailed("批次编号和目标机型不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_device_batches(batch_id,machine_model,note,created_by,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (batch_id, machine_model, note.strip(), actor_id, self._now()),
                )
                self._audit("batch", batch_id, "batch.created", actor_id, {"machine_model": machine_model})
        except sqlite3.IntegrityError as exc:
            raise Conflict("批次编号已经存在") from exc
        return {"batch_id": batch_id, "machine_model": machine_model}

    def register_device(
        self,
        actor_id: str,
        device_serial: str,
        batch_id: str,
        installed_ref: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require(actor_id, "device.register")
        batch = self.connection.execute(
            "SELECT * FROM release_device_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise NotFound("样机批次不存在")
        device_serial = device_serial.strip()
        if not device_serial:
            raise ValidationFailed("设备序列号不能为空")
        installed_id: str | None = None
        installed_revision: int | None = None
        if installed_ref is not None:
            installed_id = str(installed_ref.get("candidate_id", "")).strip()
            try:
                installed_revision = int(installed_ref["revision"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValidationFailed("出厂组合 revision 必须是正整数") from exc
            combo = self.connection.execute(
                "SELECT * FROM release_candidates WHERE candidate_id=? AND revision=?",
                (installed_id, installed_revision),
            ).fetchone()
            if combo is None:
                raise NotFound("出厂组合不存在")
            if combo["state"] != "released" and not combo["baseline_certified"]:
                raise InvalidState("出厂组合必须已发布或为工厂基线")
            if batch["machine_model"] not in json.loads(combo["target_models_json"]):
                raise InvalidState("出厂组合不覆盖该设备机型")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_devices(device_serial,batch_id,machine_model,"
                    "installed_candidate_id,installed_revision,registered_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (device_serial, batch_id, batch["machine_model"], installed_id, installed_revision, self._now()),
                )
                self._audit("device", device_serial, "device.registered", actor_id, {"batch_id": batch_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("设备序列号已经存在") from exc
        result = {"device_serial": device_serial, "batch_id": batch_id, "machine_model": batch["machine_model"]}
        if installed_id is not None:
            result["installed_combination"] = {"candidate_id": installed_id, "revision": installed_revision}
        return result

    # --------------------------------------------------------------- 候选单

    def _candidate_row(self, candidate_id: str, revision: int) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_candidates WHERE candidate_id=? AND revision=?",
            (candidate_id, revision),
        ).fetchone()
        if row is None:
            raise NotFound("发布候选修订不存在")
        return row

    def _manifest(self, row: sqlite3.Row) -> CandidateManifest:
        body = {
            "artifacts": json.loads(row["artifacts_json"]),
            "target_models": json.loads(row["target_models_json"]),
            "dependency_scope": json.loads(row["dependency_scope_json"]),
            "migration_steps": json.loads(row["migration_steps_json"]),
            "rollback_refs": json.loads(row["rollback_refs_json"]),
        }
        return CandidateManifest.from_dict(body)

    def _seal(
        self,
        actor_id: str,
        candidate_id: str,
        manifest: CandidateManifest,
        *,
        baseline: bool,
    ) -> dict[str, Any]:
        candidate_id = candidate_id.strip()
        if not candidate_id:
            raise ValidationFailed("候选编号不能为空")
        body = manifest.as_dict()
        # 制品摘要是清单的一部分；manifest_sha256 绑定全部字段，
        # content_sha256 仅绑定制品（kind,id,version,sha），任一制品变化即变。
        manifest_sha256 = digest(body)
        content_sha256 = digest(
            [
                {"kind": item.kind, "artifact_id": item.artifact_id, "version": item.version, "sha256": item.sha256}
                for item in manifest.artifacts
            ]
        )
        with transaction(self.connection, immediate=True):
            previous = self.connection.execute(
                "SELECT revision FROM release_candidates WHERE candidate_id=? ORDER BY revision DESC LIMIT 1",
                (candidate_id,),
            ).fetchone()
            revision = 1 if previous is None else previous["revision"] + 1

            # 回退目标必须是整体已证明兼容的组合：已发布修订或工厂基线，
            # 且必须覆盖本候选的全部目标机型。
            for ref in manifest.rollback_refs:
                target = self.connection.execute(
                    "SELECT * FROM release_candidates WHERE candidate_id=? AND revision=?",
                    (ref.candidate_id, ref.revision),
                ).fetchone()
                if target is None:
                    raise ValidationFailed(
                        f"回退目标 {ref.candidate_id}@{ref.revision} 不存在，不能拼出未批准组合"
                    )
                if target["state"] != "released" and not target["baseline_certified"]:
                    raise InvalidState(
                        f"回退目标 {ref.candidate_id}@{ref.revision} 尚未证明兼容（未发布且非工厂基线）"
                    )
                target_models = set(json.loads(target["target_models_json"]))
                if not set(manifest.target_models).issubset(target_models):
                    raise InvalidState(
                        f"回退目标 {ref.candidate_id}@{ref.revision} 未覆盖本候选全部目标机型"
                    )
                if ref.candidate_id == candidate_id and ref.revision >= revision:
                    raise ValidationFailed("回退目标不能指向候选自身或更新修订")

            self.connection.execute(
                "INSERT INTO release_candidates(candidate_id,revision,state,superseded,baseline_certified,"
                "artifacts_json,target_models_json,dependency_scope_json,migration_steps_json,rollback_refs_json,"
                "manifest_sha256,content_sha256,created_by,created_at,sealed_at) "
                "VALUES(?,?,'sealed',0,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    candidate_id,
                    revision,
                    1 if baseline else 0,
                    canonical_json(body["artifacts"]),
                    canonical_json(body["target_models"]),
                    canonical_json(body["dependency_scope"]),
                    canonical_json(body["migration_steps"]),
                    canonical_json(body["rollback_refs"]),
                    manifest_sha256,
                    content_sha256,
                    actor_id,
                    self._now(),
                    self._now(),
                ),
            )
            # 任一制品变化只能形成新修订；旧修订上尚未完成的签署随之失效：
            # 未走到 released 的旧候选修订标记 superseded，禁止补签。
            # 工厂基线是回退根，不参与发布门禁，不作废。
            self.connection.execute(
                "UPDATE release_candidates SET superseded=1 "
                "WHERE candidate_id=? AND revision<? AND state='sealed' AND baseline_certified=0",
                (candidate_id, revision),
            )
            self._audit(
                "candidate",
                f"{candidate_id}@{revision}",
                "candidate.baseline_registered" if baseline else "candidate.sealed",
                actor_id,
                {"manifest_sha256": manifest_sha256, "content_sha256": content_sha256},
            )
        return {
            "candidate_id": candidate_id,
            "revision": revision,
            "state": "sealed",
            "manifest_sha256": manifest_sha256,
            "content_sha256": content_sha256,
        }

    def register_baseline(self, actor_id: str, candidate_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """登记工厂/试制阶段已证明兼容的组合（回退树根，自身无需回退目标）。"""
        self._require(actor_id, "baseline.register")
        manifest = CandidateManifest.from_dict(raw)
        if manifest.rollback_refs:
            raise ValidationFailed("工厂基线组合不能再声明回退目标")
        result = self._seal(actor_id, candidate_id, manifest, baseline=True)
        result["baseline_certified"] = True
        return result

    def create_candidate(self, actor_id: str, candidate_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "candidate.write")
        manifest = CandidateManifest.from_dict(raw)
        if not manifest.rollback_refs:
            raise ValidationFailed("发布候选必须声明至少一个已证明兼容的回退版本")
        return self._seal(actor_id, candidate_id, manifest, baseline=False)

    def candidate(self, candidate_id: str, revision: int) -> dict[str, Any]:
        row = self._candidate_row(candidate_id, revision)
        manifest = self._manifest(row)
        approvals = self.connection.execute(
            "SELECT scope,approver_id,approved_at FROM release_approvals WHERE candidate_id=? AND revision=?",
            (candidate_id, revision),
        ).fetchall()
        return {
            "candidate_id": candidate_id,
            "revision": revision,
            "state": row["state"],
            "superseded": bool(row["superseded"]),
            "baseline_certified": bool(row["baseline_certified"]),
            "manifest": manifest.as_dict(),
            "manifest_sha256": row["manifest_sha256"],
            "content_sha256": row["content_sha256"],
            "approvals": [dict(item) for item in approvals],
            "gates": self._gates(candidate_id, revision, row=row, manifest=manifest),
            "created_at": row["created_at"],
            "sealed_at": row["sealed_at"],
        }

    # ----------------------------------------------------------------- 波次

    def _latest_wave_sequence(self, candidate_id: str, revision: int) -> int:
        row = self.connection.execute(
            "SELECT max(sequence) AS sequence FROM release_waves WHERE candidate_id=? AND revision=?",
            (candidate_id, revision),
        ).fetchone()
        return -1 if row["sequence"] is None else int(row["sequence"])

    def _wave(self, wave_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM release_waves WHERE wave_id=?", (wave_id,)).fetchone()
        if row is None:
            raise NotFound("发布波次不存在")
        return row

    def _wave_devices(self, wave_id: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT d.*, wd.sequence AS wave_sequence FROM release_wave_devices wd "
                "JOIN release_devices d ON d.device_serial=wd.device_serial "
                "WHERE wd.wave_id=? ORDER BY wd.sequence",
                (wave_id,),
            ).fetchall()
        )

    def _resolve_wave_devices(
        self, candidate_id: str, revision: int, batch_id: str, serials: Iterable[str] | None
    ) -> list[str]:
        batch = self.connection.execute(
            "SELECT * FROM release_device_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise NotFound("样机批次不存在")
        rows = self.connection.execute(
            "SELECT * FROM release_devices WHERE batch_id=? ORDER BY device_serial", (batch_id,)
        ).fetchall()
        by_serial = {row["device_serial"]: row for row in rows}
        if not by_serial:
            raise ValidationFailed("批次下没有注册设备")
        if serials is None:
            chosen = list(by_serial.values())
        else:
            wanted = [item.strip() for item in serials if item and item.strip()]
            if not wanted or len(set(wanted)) != len(wanted):
                raise ValidationFailed("设备列表为空或存在重复")
            missing = [item for item in wanted if item not in by_serial]
            if missing:
                raise NotFound(f"设备不属于该批次: {', '.join(missing)}")
            chosen = [by_serial[item] for item in wanted]
        for device in chosen:
            if device["state"] in ("scheduled", "in_progress", "failed"):
                raise InvalidState(f"设备 {device['device_serial']} 仍处于未结束的发布波次中")
            if device["state"] == "accepted" and device["scheduled_candidate_id"] is None:
                installed = (device["installed_candidate_id"], device["installed_revision"])
                if installed == (candidate_id, revision):
                    raise InvalidState(
                        f"设备 {device['device_serial']} 已在该修订的波次中验收通过，不能重复加入"
                    )
        return [item["device_serial"] for item in chosen]

    def _open_wave(
        self,
        actor_id: str,
        permission: str,
        kind: str,
        scope: str,
        wave_id: str,
        candidate_id: str,
        revision: int,
        batch_id: str,
        serials: Iterable[str] | None,
        idempotency_key: str,
        raw: Mapping[str, Any],
    ) -> dict[str, Any]:
        self._require(actor_id, permission)
        replay = self._idempotency(scope, idempotency_key, raw)
        if replay is not None:
            return replay
        row = self._candidate_row(candidate_id, revision)
        manifest = self._manifest(row)
        if row["superseded"]:
            raise InvalidState("候选修订已被新修订取代，未完成签署已失效")
        if batch_id and batch_id.strip():
            batch = self.connection.execute(
                "SELECT * FROM release_device_batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise NotFound("样机批次不存在")
            if batch["machine_model"] not in manifest.target_models:
                raise ValidationFailed(
                    f"批次机型 {batch['machine_model']} 不在候选目标机型 {manifest.target_models} 内"
                )
        if kind == "expansion":
            if not self._pilot_passed(candidate_id, revision):
                raise InvalidState("样机批次两侧验收未全部通过，不能扩大范围")
            approvals = {
                item["scope"]
                for item in self.connection.execute(
                    "SELECT scope FROM release_approvals WHERE candidate_id=? AND revision=?",
                    (candidate_id, revision),
                ).fetchall()
            }
            missing = [side for side in ("control", "ai") if side not in approvals]
            if missing:
                raise InvalidState(f"{missing} 侧负责人尚未批准扩大范围")

        with transaction(self.connection, immediate=True):
            sequence = self._latest_wave_sequence(candidate_id, revision) + 1
            if kind == "pilot" and sequence != 0:
                raise InvalidState("每个候选修订只能有一个样机批次波次")
            resolved = self._resolve_wave_devices(candidate_id, revision, batch_id, serials)
            try:
                self.connection.execute(
                    "INSERT INTO release_waves(wave_id,candidate_id,revision,sequence,kind,batch_id,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (wave_id, candidate_id, revision, sequence, kind, batch_id, actor_id, self._now()),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("波次编号冲突") from exc
            for index, serial in enumerate(resolved):
                self.connection.execute(
                    "INSERT INTO release_wave_devices(wave_id,candidate_id,revision,device_serial,sequence) "
                    "VALUES(?,?,?,?,?)",
                    (wave_id, candidate_id, revision, serial, index),
                )
                self.connection.execute(
                    "UPDATE release_devices SET scheduled_candidate_id=?,scheduled_revision=?,state='scheduled' "
                    "WHERE device_serial=?",
                    (candidate_id, revision, serial),
                )
            if kind == "expansion":
                self.connection.execute(
                    "UPDATE release_candidates SET state='released' WHERE candidate_id=? AND revision=?",
                    (candidate_id, revision),
                )
            response = {
                "wave_id": wave_id,
                "candidate_id": candidate_id,
                "revision": revision,
                "sequence": sequence,
                "kind": kind,
                "batch_id": batch_id,
                "devices": resolved,
                "state": "open",
            }
            self._store_idempotency(scope, idempotency_key, raw, response)
            self._audit(
                "wave", wave_id, f"wave.{kind}_opened", actor_id,
                {"candidate_id": candidate_id, "revision": revision, "devices": resolved},
            )
        return response

    def open_pilot_wave(
        self,
        actor_id: str,
        wave_id: str,
        candidate_id: str,
        revision: int,
        batch_id: str,
        idempotency_key: str,
        devices: list[str] | None = None,
    ) -> dict[str, Any]:
        raw = {
            "wave_id": wave_id, "candidate_id": candidate_id, "revision": revision,
            "batch_id": batch_id, "devices": devices,
        }
        return self._open_wave(
            actor_id, "wave.pilot", "pilot", "wave.pilot", wave_id,
            candidate_id, revision, batch_id, devices, idempotency_key, raw,
        )

    def open_expansion_wave(
        self,
        actor_id: str,
        wave_id: str,
        candidate_id: str,
        revision: int,
        batch_id: str,
        idempotency_key: str,
        devices: list[str] | None = None,
    ) -> dict[str, Any]:
        raw = {
            "wave_id": wave_id, "candidate_id": candidate_id, "revision": revision,
            "batch_id": batch_id, "devices": devices,
        }
        return self._open_wave(
            actor_id, "wave.expansion", "expansion", f"wave.expansion:{wave_id}", wave_id,
            candidate_id, revision, batch_id, devices, idempotency_key, raw,
        )

    # ----------------------------------------------------------------- 回执

    def record_receipt(
        self,
        actor_id: str,
        wave_id: str,
        device_serial: str,
        step_id: str,
        status: str,
        summary: Mapping[str, Any],
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        self._require(actor_id, "receipt.record")
        if status not in ("ok", "failed"):
            raise ValidationFailed("回执状态只能是 ok 或 failed")
        if not isinstance(summary, Mapping):
            raise ValidationFailed("验收摘要必须是 JSON 对象")
        wave = self._wave(wave_id)
        candidate_id, revision = wave["candidate_id"], wave["revision"]
        row = self._candidate_row(candidate_id, revision)
        manifest = self._manifest(row)
        steps = {step.step_id: step for step in manifest.migration_steps}
        if step_id not in steps:
            raise ValidationFailed(f"步骤 {step_id} 不在候选迁移步骤中")
        membership = self.connection.execute(
            "SELECT 1 FROM release_wave_devices WHERE wave_id=? AND device_serial=?",
            (wave_id, device_serial),
        ).fetchone()
        if membership is None:
            raise NotFound("设备不在该发布波次中")
        device = self.connection.execute(
            "SELECT * FROM release_devices WHERE device_serial=?", (device_serial,)
        ).fetchone()

        scope = f"receipt:{candidate_id}:{revision}:{device_serial}:{step_id}"
        replay = self._idempotency(scope, idempotency_key, {"status": status, "summary": dict(summary)})
        if replay is not None:
            return replay

        if device["state"] in ("failed", "rolled_back", "recovered"):
            raise InvalidState("设备已失败或已回退，不能继续记录回执，请先按回退流程处理")

        summary_json = canonical_json(dict(summary))
        summary_sha256 = digest(
            {
                "candidate_id": candidate_id,
                "revision": revision,
                "device_serial": device_serial,
                "step_id": step_id,
                "status": status,
                "summary": dict(summary),
            }
        )
        side = steps[step_id].side
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT status,summary_sha256 FROM release_receipts "
                "WHERE candidate_id=? AND revision=? AND device_serial=? AND step_id=?",
                (candidate_id, revision, device_serial, step_id),
            ).fetchone()
            if existing is not None:
                # 按设备×步骤幂等归并：同内容重放返回原回执，内容变化冲突。
                if existing["summary_sha256"] != summary_sha256:
                    raise Conflict("同一设备同一步骤已有不同验收回执")
                stored = self.connection.execute(
                    "SELECT * FROM release_receipts WHERE candidate_id=? AND revision=? "
                    "AND device_serial=? AND step_id=?",
                    (candidate_id, revision, device_serial, step_id),
                ).fetchone()
                return self._receipt_dict(stored)
            cursor = self.connection.execute(
                "INSERT INTO release_receipts(candidate_id,revision,wave_id,device_serial,step_id,side,"
                "status,summary_json,summary_sha256,recorded_by,recorded_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    candidate_id, revision, wave_id, device_serial, step_id, side, status,
                    summary_json, summary_sha256, actor_id, self._now(),
                ),
            )
            receipt_id = int(cursor.lastrowid)
            stored = self.connection.execute(
                "SELECT * FROM release_receipts WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
            response = self._receipt_dict(stored)
            self._store_idempotency(
                scope, idempotency_key, {"status": status, "summary": dict(summary)},
                response,
            )
            self._refresh_device_state(self.connection, device_serial, candidate_id, revision, manifest)
            self._audit(
                "receipt", str(receipt_id), "receipt.recorded", actor_id,
                {"wave_id": wave_id, "device_serial": device_serial, "step_id": step_id,
                 "side": side, "status": status},
            )
            return response

    def _receipts_for(self, candidate_id: str, revision: int, device_serial: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT * FROM release_receipts WHERE candidate_id=? AND revision=? AND device_serial=? "
                "ORDER BY receipt_id",
                (candidate_id, revision, device_serial),
            ).fetchall()
        )

    def _refresh_device_state(
        self,
        conn: sqlite3.Connection,
        device_serial: str,
        candidate_id: str,
        revision: int,
        manifest: CandidateManifest,
    ) -> None:
        receipts = list(
            conn.execute(
                "SELECT * FROM release_receipts WHERE candidate_id=? AND revision=? AND device_serial=?",
                (candidate_id, revision, device_serial),
            ).fetchall()
        )
        by_step = {item["step_id"]: item for item in receipts}
        if any(item["status"] == "failed" for item in receipts):
            conn.execute(
                "UPDATE release_devices SET state='failed' WHERE device_serial=?", (device_serial,)
            )
        elif all(step.step_id in by_step and by_step[step.step_id]["status"] == "ok"
               for step in manifest.migration_steps):
            conn.execute(
                "UPDATE release_devices SET state='accepted',installed_candidate_id=?,"
                "installed_revision=?,scheduled_candidate_id=NULL,scheduled_revision=NULL "
                "WHERE device_serial=?",
                (candidate_id, revision, device_serial),
            )
        elif receipts:
            conn.execute(
                "UPDATE release_devices SET state='in_progress' WHERE device_serial=? AND state='scheduled'",
                (device_serial,),
            )
        # 波次内全部设备进入终态时自动收尾（含失败待回退设备，由回退流程继续处理）。
        wave_row = conn.execute(
            "SELECT w.wave_id FROM release_wave_devices wd JOIN release_waves w ON w.wave_id=wd.wave_id "
            "WHERE wd.candidate_id=? AND wd.revision=? AND wd.device_serial=?",
            (candidate_id, revision, device_serial),
        ).fetchone()
        if wave_row is not None:
            pending = conn.execute(
                "SELECT count(*) AS n FROM release_wave_devices wd JOIN release_devices d "
                "ON d.device_serial=wd.device_serial WHERE wd.wave_id=? "
                "AND d.state IN ('scheduled','in_progress')",
                (wave_row["wave_id"],),
            ).fetchone()["n"]
            if pending == 0:
                conn.execute(
                    "UPDATE release_waves SET state='completed' WHERE wave_id=? AND state='open'",
                    (wave_row["wave_id"],),
                )

    @staticmethod
    def _receipt_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "receipt_id": row["receipt_id"],
            "candidate_id": row["candidate_id"],
            "revision": row["revision"],
            "wave_id": row["wave_id"],
            "device_serial": row["device_serial"],
            "step_id": row["step_id"],
            "side": row["side"],
            "status": row["status"],
            "summary": json.loads(row["summary_json"]),
            "recorded_by": row["recorded_by"],
            "recorded_at": row["recorded_at"],
        }

    # ------------------------------------------------------------- 门禁/验收

    def _pilot_wave(self, candidate_id: str, revision: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM release_waves WHERE candidate_id=? AND revision=? AND kind='pilot'",
            (candidate_id, revision),
        ).fetchone()

    def _side_acceptance(self, wave_id: str, side: str) -> dict[str, Any]:
        devices = self._wave_devices(wave_id)
        wave = self._wave(wave_id)
        required_steps = {
            step.step_id
            for step in self._manifest(self._candidate_row(wave["candidate_id"], wave["revision"])).migration_steps
            if step.side == side
        }
        accepted = 0
        failed: list[str] = []
        pending: list[str] = []
        summaries: list[dict[str, Any]] = []
        for device in devices:
            rows = self.connection.execute(
                "SELECT * FROM release_receipts WHERE wave_id=? AND device_serial=? AND side=? ORDER BY step_id",
                (wave_id, device["device_serial"], side),
            ).fetchall()
            by_step = {item["step_id"]: item for item in rows}
            summaries.extend(self._receipt_dict(item) for item in rows)
            if any(item["status"] == "failed" for item in rows):
                failed.append(device["device_serial"])
            elif required_steps and all(
                step in by_step and by_step[step]["status"] == "ok" for step in required_steps
            ):
                accepted += 1
            else:
                pending.append(device["device_serial"])
        return {
            "side": side,
            "devices_total": len(devices),
            "devices_accepted": accepted,
            "devices_failed": failed,
            "devices_pending": pending,
            "passed": not failed and not pending and bool(devices),
            "receipts": summaries,
        }

    def _pilot_passed(self, candidate_id: str, revision: int) -> bool:
        pilot = self._pilot_wave(candidate_id, revision)
        if pilot is None:
            return False
        return self._side_acceptance(pilot["wave_id"], "control")["passed"] and \
            self._side_acceptance(pilot["wave_id"], "ai")["passed"]

    def approve(
        self, actor_id: str, candidate_id: str, revision: int, side: str, note: str = ""
    ) -> dict[str, Any]:
        if side not in SIDE_PERMISSION:
            raise ValidationFailed("签署范围只能是 control 或 ai")
        self._require(actor_id, SIDE_PERMISSION[side])
        row = self._candidate_row(candidate_id, revision)
        if row["baseline_certified"]:
            raise InvalidState("工厂基线组合无需签署")
        if row["superseded"]:
            raise InvalidState("候选已被新修订取代，旧修订未完成的签署已失效")
        pilot = self._pilot_wave(candidate_id, revision)
        if pilot is None:
            raise InvalidState("样机批次波次尚未开始，不能签署")
        acceptance = self._side_acceptance(pilot["wave_id"], side)
        if not acceptance["passed"]:
            raise InvalidState(
                f"{side} 侧样机验收未全部通过（待处理设备 {acceptance['devices_failed'] + acceptance['devices_pending']}）"
            )
        # 相互独立的负责人：control/ai 是不同角色，数据库层也禁止同一人拥有两角色。
        other = "ai" if side == "control" else "control"
        approver = self._user(actor_id)
        if approver["role"] != APPROVAL_ROLE[side]:
            raise Forbidden(f"{side} 侧签署必须由 {APPROVAL_ROLE[side]} 完成")
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT approver_id FROM release_approvals WHERE candidate_id=? AND revision=? AND scope=?",
                (candidate_id, revision, side),
            ).fetchone()
            if existing is not None:
                raise Conflict(f"{side} 侧已经签署")
            other_row = self.connection.execute(
                "SELECT approver_id FROM release_approvals WHERE candidate_id=? AND revision=? AND scope=?",
                (candidate_id, revision, other),
            ).fetchone()
            if other_row is not None and other_row["approver_id"] == actor_id:
                raise Forbidden("两侧签署必须由相互独立的负责人完成，不能同一人连签")
            self.connection.execute(
                "INSERT INTO release_approvals(candidate_id,revision,scope,approver_id,content_sha256,"
                "approved_at) VALUES(?,?,?,?,?,?)",
                (candidate_id, revision, side, actor_id, row["content_sha256"], self._now()),
            )
            self._audit(
                "candidate", f"{candidate_id}@{revision}", "candidate.approved", actor_id,
                {"scope": side, "content_sha256": row["content_sha256"], "note": note},
            )
        return {"candidate_id": candidate_id, "revision": revision, "scope": side, "approver_id": actor_id}

    # ----------------------------------------------------------------- 回退

    def rollback_device(
        self,
        actor_id: str,
        device_serial: str,
        target_candidate_id: str,
        target_revision: int,
        reason: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "rollback.execute")
        raw = {
            "device_serial": device_serial,
            "target_candidate_id": target_candidate_id,
            "target_revision": target_revision,
            "reason": reason,
        }
        replay = self._idempotency("rollback", idempotency_key, raw)
        if replay is not None:
            return replay
        device = self.connection.execute(
            "SELECT * FROM release_devices WHERE device_serial=?", (device_serial,)
        ).fetchone()
        if device is None:
            raise NotFound("设备不存在")
        from_id, from_revision = device["scheduled_candidate_id"], device["scheduled_revision"]
        if from_id is None:
            raise InvalidState("设备没有进行中的发布波次，无需回退")
        failure = self.connection.execute(
            "SELECT 1 FROM release_receipts WHERE candidate_id=? AND revision=? AND device_serial=? AND status='failed'",
            (from_id, from_revision, device_serial),
        ).fetchone()
        if failure is None:
            raise InvalidState("设备没有失败回执，不能执行回退")
        from_row = self._candidate_row(from_id, from_revision)
        from_manifest = self._manifest(from_row)
        refs = {(ref.candidate_id, ref.revision) for ref in from_manifest.rollback_refs}
        if (target_candidate_id, target_revision) not in refs:
            raise InvalidState(
                f"目标 {target_candidate_id}@{target_revision} 不在当前候选声明的回退组合内，"
                "禁止拼出从未批准的版本"
            )
        target = self._candidate_row(target_candidate_id, target_revision)
        if target["state"] != "released" and not target["baseline_certified"]:
            raise InvalidState("目标组合未证明兼容，禁止回退")
        if device["machine_model"] not in json.loads(target["target_models_json"]):
            raise InvalidState("目标组合不覆盖该设备机型")
        if (target_candidate_id, target_revision) == (from_id, from_revision):
            raise ValidationFailed("回退目标不能是当前组合本身")

        with transaction(self.connection, immediate=True):
            attempted_steps = {
                item["step_id"]
                for item in self.connection.execute(
                    "SELECT step_id FROM release_receipts WHERE candidate_id=? AND revision=? AND device_serial=?",
                    (from_id, from_revision, device_serial),
                ).fetchall()
            }
            actions: list[dict[str, Any]] = []
            for step in from_manifest.migration_steps:
                if step.step_id in attempted_steps and step.rollback_field_action:
                    self.connection.execute(
                        "INSERT OR IGNORE INTO release_field_actions(device_serial,from_candidate_id,"
                        "from_revision,to_candidate_id,to_revision,step_id,description,reason,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            device_serial, from_id, from_revision, target_candidate_id, target_revision,
                            step.step_id, step.rollback_field_action, reason, self._now(),
                        ),
                    )
                    actions.append({"step_id": step.step_id, "description": step.rollback_field_action})
            self.connection.execute(
                "UPDATE release_devices SET state='rolled_back',installed_candidate_id=?,"
                "installed_revision=?,scheduled_candidate_id=NULL,scheduled_revision=NULL "
                "WHERE device_serial=?",
                (target_candidate_id, target_revision, device_serial),
            )
            response = {
                "device_serial": device_serial,
                "rolled_back_from": {"candidate_id": from_id, "revision": from_revision},
                "rolled_back_to": {"candidate_id": target_candidate_id, "revision": target_revision},
                "target_combination": self._combination(target),
                "pending_field_actions": actions,
                "state": "rolled_back",
            }
            self._store_idempotency("rollback", idempotency_key, raw, response)
            self._audit(
                "device", device_serial, "device.rolled_back", actor_id,
                {"from": f"{from_id}@{from_revision}", "to": f"{target_candidate_id}@{target_revision}",
                 "reason": reason, "field_actions": len(actions)},
            )
        return response

    def complete_field_action(self, actor_id: str, action_id: int) -> dict[str, Any]:
        self._require(actor_id, "field_action.complete")
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT * FROM release_field_actions WHERE action_id=?", (action_id,)
            ).fetchone()
            if row is None:
                raise NotFound("现场动作不存在")
            if row["state"] != "pending":
                raise Conflict("现场动作已经完成")
            self.connection.execute(
                "UPDATE release_field_actions SET state='done',completed_by=?,completed_at=? WHERE action_id=?",
                (actor_id, self._now(), action_id),
            )
            remaining = self.connection.execute(
                "SELECT count(*) AS n FROM release_field_actions WHERE device_serial=? AND state='pending'",
                (row["device_serial"],),
            ).fetchone()["n"]
            if remaining == 0:
                self.connection.execute(
                    "UPDATE release_devices SET state='recovered' WHERE device_serial=? AND state='rolled_back'",
                    (row["device_serial"],),
                )
            self._audit(
                "field_action", str(action_id), "field_action.completed", actor_id,
                {"device_serial": row["device_serial"]},
            )
        return {"action_id": action_id, "state": "done", "pending_remaining": remaining}

    # ------------------------------------------------------------------ 视图

    def _combination(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "candidate_id": row["candidate_id"],
            "revision": row["revision"],
            "state": row["state"],
            "baseline_certified": bool(row["baseline_certified"]),
            "content_sha256": row["content_sha256"],
            "artifacts": json.loads(row["artifacts_json"]),
        }

    def _gates(
        self,
        candidate_id: str,
        revision: int,
        *,
        row: sqlite3.Row | None = None,
        manifest: CandidateManifest | None = None,
    ) -> list[dict[str, Any]]:
        row = row or self._candidate_row(candidate_id, revision)
        manifest = manifest or self._manifest(row)
        pilot = self._pilot_wave(candidate_id, revision)
        control = self._side_acceptance(pilot["wave_id"], "control") if pilot is not None else None
        ai = self._side_acceptance(pilot["wave_id"], "ai") if pilot is not None else None
        approvals = {
            item["scope"]: item
            for item in self.connection.execute(
                "SELECT scope,approver_id,approved_at FROM release_approvals WHERE candidate_id=? AND revision=?",
                (candidate_id, revision),
            ).fetchall()
        }
        gates = [
            {"gate": "sealed", "state": "ok", "detail": {"content_sha256": row["content_sha256"]}},
            {
                "gate": "pilot_opened",
                "state": "ok" if pilot is not None else "blocked",
                "detail": {} if pilot is None else {"wave_id": pilot["wave_id"], "batch_id": pilot["batch_id"]},
            },
            {
                "gate": "pilot_control_acceptance",
                "state": self._acceptance_gate_state(control),
                "detail": {} if control is None else self._acceptance_detail(control),
            },
            {
                "gate": "pilot_ai_acceptance",
                "state": self._acceptance_gate_state(ai),
                "detail": {} if ai is None else self._acceptance_detail(ai),
            },
            {
                "gate": "control_approval",
                "state": "ok" if "control" in approvals else "blocked",
                "detail": {} if "control" not in approvals else {"approver_id": approvals["control"]["approver_id"]},
            },
            {
                "gate": "ai_approval",
                "state": "ok" if "ai" in approvals else "blocked",
                "detail": {} if "ai" not in approvals else {"approver_id": approvals["ai"]["approver_id"]},
            },
            {
                "gate": "released",
                "state": "ok" if row["state"] == "released" else "blocked",
                "detail": {},
            },
        ]
        current = next((item["gate"] for item in gates if item["state"] != "ok"), None)
        for item in gates:
            item["current_gate"] = item["gate"] == current if current else False
        return gates

    @staticmethod
    def _acceptance_gate_state(acceptance: dict[str, Any] | None) -> str:
        if acceptance is None:
            return "blocked"
        if acceptance["devices_failed"]:
            return "failed"
        if acceptance["passed"]:
            return "ok"
        return "in_progress"

    @staticmethod
    def _acceptance_detail(acceptance: dict[str, Any]) -> dict[str, Any]:
        return {
            "devices_total": acceptance["devices_total"],
            "devices_accepted": acceptance["devices_accepted"],
            "devices_failed": acceptance["devices_failed"],
            "devices_pending": acceptance["devices_pending"],
        }

    def _device_gate(self, device: sqlite3.Row) -> dict[str, Any] | None:
        candidate_id, revision = device["scheduled_candidate_id"], device["scheduled_revision"]
        if candidate_id is None:
            return None
        manifest = self._manifest(self._candidate_row(candidate_id, revision))
        receipts = {item["step_id"]: item for item in self._receipts_for(candidate_id, revision, device["device_serial"])}
        steps = []
        blocking_step: str | None = None
        for step in manifest.migration_steps:
            recorded = receipts.get(step.step_id)
            status = "missing" if recorded is None else recorded["status"]
            steps.append({"step_id": step.step_id, "side": step.side, "status": status})
            if blocking_step is None and status != "ok":
                blocking_step = step.step_id
        return {
            "wave": self._active_wave(candidate_id, revision, device["device_serial"]),
            "steps": steps,
            "blocking_step": blocking_step,
        }

    def _active_wave(self, candidate_id: str, revision: int, device_serial: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT w.wave_id,w.kind,w.sequence,w.batch_id FROM release_wave_devices wd "
            "JOIN release_waves w ON w.wave_id=wd.wave_id "
            "WHERE wd.candidate_id=? AND wd.revision=? AND wd.device_serial=? "
            "ORDER BY w.sequence DESC LIMIT 1",
            (candidate_id, revision, device_serial),
        ).fetchone()
        return None if row is None else dict(row)

    def device_status(self, device_serial: str) -> dict[str, Any]:
        device = self.connection.execute(
            "SELECT * FROM release_devices WHERE device_serial=?", (device_serial,)
        ).fetchone()
        if device is None:
            raise NotFound("设备不存在")
        installed = None
        if device["installed_candidate_id"] is not None:
            installed_row = self._candidate_row(device["installed_candidate_id"], device["installed_revision"])
            installed = self._combination(installed_row)
        scheduled = None
        if device["scheduled_candidate_id"] is not None:
            scheduled_row = self._candidate_row(device["scheduled_candidate_id"], device["scheduled_revision"])
            scheduled = self._combination(scheduled_row)
        pending_actions = [
            dict(item)
            for item in self.connection.execute(
                "SELECT action_id,from_candidate_id,from_revision,to_candidate_id,to_revision,step_id,"
                "description,reason,created_at FROM release_field_actions WHERE device_serial=? AND state='pending' "
                "ORDER BY action_id",
                (device_serial,),
            ).fetchall()
        ]
        for action in pending_actions:
            action["from_revision"] = int(action["from_revision"])
            action["to_revision"] = int(action["to_revision"])
            action["action_id"] = int(action["action_id"])
        gate = self._device_gate(device)
        return {
            "device_serial": device_serial,
            "batch_id": device["batch_id"],
            "machine_model": device["machine_model"],
            "state": device["state"],
            "installed_combination": installed,
            "scheduled_combination": scheduled,
            "gate": gate,
            "pending_field_actions": pending_actions,
        }

    def wave_report(self, wave_id: str) -> dict[str, Any]:
        wave = self._wave(wave_id)
        control = self._side_acceptance(wave_id, "control")
        ai = self._side_acceptance(wave_id, "ai")
        devices = []
        for device in self._wave_devices(wave_id):
            status = self.device_status(device["device_serial"])
            devices.append(
                {
                    "device_serial": device["device_serial"],
                    "machine_model": device["machine_model"],
                    "state": device["state"],
                    "gate": status["gate"],
                    "pending_field_actions": status["pending_field_actions"],
                }
            )
        return {
            "wave_id": wave_id,
            "candidate_id": wave["candidate_id"],
            "revision": wave["revision"],
            "sequence": wave["sequence"],
            "kind": wave["kind"],
            "batch_id": wave["batch_id"],
            "state": wave["state"],
            "control_acceptance": {k: v for k, v in control.items() if k != "receipts"},
            "ai_acceptance": {k: v for k, v in ai.items() if k != "receipts"},
            "control_summaries": control["receipts"],
            "ai_summaries": ai["receipts"],
            "devices": devices,
        }

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
