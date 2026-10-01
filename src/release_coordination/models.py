"""协同发布候选的输入契约与规范化。

发布候选把一次整机升级必须同时生效的四类制品绑定为一个整体：

- ``control_os``：控制操作系统镜像；
- ``network_config``：实时总线等网络配置；
- ``toolchain_output``：工具链生成物（控制参数、标定表等）；
- ``ai_model``：AI 模型。

前三类属于控制侧，``ai_model`` 属于 AI 侧。现场曾出现只更新模型、
遗漏控制参数的事故，因此候选必须同时包含两侧制品，且按整体封存、
整体签署、整体回退。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")

CONTROL_KINDS = ("control_os", "network_config", "toolchain_output")
ARTIFACT_KINDS = CONTROL_KINDS + ("ai_model",)
SIDES = ("control", "ai")


def _required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def _identifier(value: object, field: str) -> str:
    result = _required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value.strip().lower()):
        raise ValidationFailed(f"{field} 必须是 64 位小写十六进制 SHA-256")
    return value.strip().lower()


@dataclass(frozen=True, slots=True)
class Artifact:
    kind: str
    artifact_id: str
    version: str
    sha256: str
    summary: str

    @property
    def side(self) -> str:
        return "ai" if self.kind == "ai_model" else "control"

    def as_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "artifact_id": self.artifact_id,
            "version": self.version,
            "sha256": self.sha256,
            "summary": self.summary,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Artifact":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("制品必须是对象")
        kind = _required_text(raw.get("kind"), "制品 kind", 32)
        if kind not in ARTIFACT_KINDS:
            raise ValidationFailed(f"未知制品类型 {kind}")
        return cls(
            kind=kind,
            artifact_id=_identifier(raw.get("artifact_id"), "制品 artifact_id"),
            version=_required_text(raw.get("version"), "制品 version", 128),
            sha256=_sha256(raw.get("sha256"), "制品 sha256"),
            summary=_required_text(raw.get("summary", ""), "制品摘要", 1024),
        )


@dataclass(frozen=True, slots=True)
class MigrationStep:
    step_id: str
    side: str
    description: str
    # 回退之后仍需人工在现场完成的动作（如恢复标定参数、重新归零），
    # 没有则表示该步骤回退无需现场处置。
    rollback_field_action: str = ""

    def as_dict(self) -> dict[str, str]:
        body = {
            "step_id": self.step_id,
            "side": self.side,
            "description": self.description,
        }
        if self.rollback_field_action:
            body["rollback_field_action"] = self.rollback_field_action
        return body

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MigrationStep":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("迁移步骤必须是对象")
        side = _required_text(raw.get("side"), "步骤 side", 16)
        if side not in SIDES:
            raise ValidationFailed("步骤 side 只能是 control 或 ai")
        action = str(raw.get("rollback_field_action", "") or "").strip()
        if len(action) > 1024:
            raise ValidationFailed("现场动作说明不能超过 1024 个字符")
        return cls(
            step_id=_identifier(raw.get("step_id"), "步骤 step_id"),
            side=side,
            description=_required_text(raw.get("description"), "步骤说明", 1024),
            rollback_field_action=action,
        )


@dataclass(frozen=True, slots=True)
class RollbackRef:
    candidate_id: str
    revision: int

    def as_dict(self) -> dict[str, object]:
        return {"candidate_id": self.candidate_id, "revision": self.revision}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "RollbackRef":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("可回退版本必须是对象")
        try:
            revision = int(raw["revision"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationFailed("可回退版本 revision 必须是正整数") from exc
        if revision <= 0:
            raise ValidationFailed("可回退版本 revision 必须是正整数")
        return cls(candidate_id=_identifier(raw.get("candidate_id"), "可回退候选 candidate_id"), revision=revision)


@dataclass(frozen=True, slots=True)
class CandidateManifest:
    artifacts: tuple[Artifact, ...]
    target_models: tuple[str, ...]
    dependency_scope: tuple[dict[str, str], ...]
    migration_steps: tuple[MigrationStep, ...]
    rollback_refs: tuple[RollbackRef, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifacts": [item.as_dict() for item in self.artifacts],
            "target_models": list(self.target_models),
            "dependency_scope": [dict(item) for item in self.dependency_scope],
            "migration_steps": [item.as_dict() for item in self.migration_steps],
            "rollback_refs": [item.as_dict() for item in self.rollback_refs],
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CandidateManifest":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("候选清单必须是 JSON 对象")

        artifact_rows = raw.get("artifacts")
        if not isinstance(artifact_rows, list) or not artifact_rows:
            raise ValidationFailed("制品清单不能为空")
        artifacts = tuple(Artifact.from_dict(item) for item in artifact_rows)
        keys = {(item.kind, item.artifact_id) for item in artifacts}
        if len(keys) != len(artifacts):
            raise ValidationFailed("制品类型与编号不能重复")
        sides = {item.side for item in artifacts}
        if "control" not in sides or "ai" not in sides:
            raise ValidationFailed("候选必须同时包含控制侧制品和 AI 模型")

        models_raw = raw.get("target_models")
        if not isinstance(models_raw, list) or not models_raw:
            raise ValidationFailed("目标机型不能为空")
        target_models = tuple(_required_text(item, "目标机型", 64) for item in models_raw)
        if len(set(target_models)) != len(target_models):
            raise ValidationFailed("目标机型不能重复")

        scope_raw = raw.get("dependency_scope", [])
        if not isinstance(scope_raw, list) or not scope_raw:
            raise ValidationFailed("依赖范围不能为空")
        dependency_scope: list[dict[str, str]] = []
        seen_scope: set[str] = set()
        for index, item in enumerate(scope_raw):
            if not isinstance(item, Mapping):
                raise ValidationFailed("依赖范围条目必须是对象")
            kind = _required_text(item.get("kind"), f"依赖范围[{index}].kind", 32)
            if kind not in ARTIFACT_KINDS:
                raise ValidationFailed(f"依赖范围存在未知制品类型 {kind}")
            constraint = _required_text(item.get("constraint"), f"依赖范围[{index}].constraint", 256)
            if kind in seen_scope:
                raise ValidationFailed(f"依赖范围中 {kind} 重复")
            seen_scope.add(kind)
            dependency_scope.append({"kind": kind, "constraint": constraint})

        steps_raw = raw.get("migration_steps")
        if not isinstance(steps_raw, list) or not steps_raw:
            raise ValidationFailed("迁移步骤不能为空")
        migration_steps = tuple(MigrationStep.from_dict(item) for item in steps_raw)
        step_ids = {step.step_id for step in migration_steps}
        if len(step_ids) != len(migration_steps):
            raise ValidationFailed("迁移步骤 step_id 不能重复")
        step_sides = {step.side for step in migration_steps}
        if "control" not in step_sides or "ai" not in step_sides:
            raise ValidationFailed("迁移步骤必须覆盖控制与 AI 两侧")

        refs_raw = raw.get("rollback_refs", [])
        if not isinstance(refs_raw, list):
            raise ValidationFailed("可回退版本必须是列表")
        rollback_refs = tuple(RollbackRef.from_dict(item) for item in refs_raw)
        ref_keys = {(ref.candidate_id, ref.revision) for ref in rollback_refs}
        if len(ref_keys) != len(rollback_refs):
            raise ValidationFailed("可回退版本不能重复")

        return cls(
            artifacts=artifacts,
            target_models=target_models,
            dependency_scope=tuple(dependency_scope),
            migration_steps=migration_steps,
            rollback_refs=rollback_refs,
        )
