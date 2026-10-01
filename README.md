# 治理控制软件与 AI 模型协同发布基础平台

本项目是一套可离线运行的 Python 服务端平台，服务于具身智能机器人控制域、AI 计算域、国产电子部件质量域和整机协同发布域。平台管理控制计算节点、实时控制总线、资源时隙、机器人构建、验证测量、分析租约、部件批次与质量决定，并把控制操作系统、网络配置、工具链生成物与 AI 模型绑定为不可变发布候选，经样机双验、双负责人批准后分阶段放行；业务状态、幂等结果和审计事件保存在 SQLite 中。

## 目录

- `src/robot_control/`：控制节点、实时总线、资源批次、时隙申请、容量分配和架构情景；
- `src/embodied_ai/`：机器人构建、验证协议、测量导入、排除复核、分析任务和准入决定；
- `src/component_qualification/`：国产电子部件批次、信号测量、统计分析、账号权限和质量审批；
- `src/release_coordination/`：不可变发布候选、样机批次控制/AI 双侧验收、独立负责人批准、分阶段波次、幂等回执、整体回退与现场动作；
- `fixtures/`：离线验收使用的验证协议与结构化测量；
- `tests/`：领域规则、事务、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m robot_control.acceptance --workspace .
PYTHONPATH=src python3 -m embodied_ai.acceptance --workspace .
PYTHONPATH=src python3 -m component_qualification.acceptance
PYTHONPATH=src python3 -m release_coordination.acceptance --workspace .
```

四条命令会在临时 SQLite 数据库中完成控制资源分配、AI 验证分析、国产电子部件质量流程，以及控制与 AI 协同发布（不可变候选、样机双验、双负责人批准、扩大放行、失败整体回退与现场动作），不访问外部网络。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_control.api --database robot-control.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m embodied_ai.api --database embodied-ai.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m component_qualification.api --database component.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m release_coordination.api --database release.sqlite3 --host 127.0.0.1 --port 8083
```

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。

## 协同发布门禁

`release_coordination` 把一次整机升级必须同时生效的四类制品——控制操作系统、网络配置、工具链生成物（含控制参数与标定表）、AI 模型——绑定为按修订不可变的发布候选：

1. **工厂基线**（`POST /candidates/{id}/baseline`）：试制阶段已证明兼容的整体组合，作为回退树根；
2. **发布候选**（`POST /candidates/{id}/revisions`）：封存制品摘要、目标机型、依赖范围、迁移步骤和可回退版本，计算 `manifest_sha256` / `content_sha256`；回退目标只能是已发布修订或工厂基线，且必须覆盖全部目标机型，封存时即拒绝拼出未批准组合；
3. **样机波次**（wave 0）：在指定样机批次按设备×步骤收集控制侧与 AI 侧验收回执，回执按设备和步骤幂等归并；
4. **独立批准**：控制负责人、AI 负责人两个互斥角色在各自一侧验收全过后分别签署，同一人不能连签两侧；
5. **扩大范围**（expansion 波次）：双签齐全且样机双验通过方可放行，候选转为 `released`；
6. **任一制品变化**只能产生新修订，旧修订未完成的签署立即作废（标记 `superseded`，禁止补签），签署不跨修订迁移；
7. **失败回退**：任一设备步骤失败后，只能整体回退到候选清单声明、且已证明兼容的完整组合，已尝试步骤声明的现场动作进入待处理清单，全部完成后设备才恢复；
8. **发布视图**：`GET /devices/{serial}` 显示设备采用的完整四制品组合、当前卡住的门禁步骤、以及回退后仍需处理的现场动作；`GET /waves/{id}` 汇总波次内两侧验收摘要。
