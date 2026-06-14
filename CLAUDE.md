# natsx

`natsx` 是 FoundationX 的 **L2 NATS adapter 基础库**，为 ZoneCNH 服务提供 Core NATS（pub/sub/request/reply）、JetStream 持久化、Subject 规范构造和连接生命周期管理。

本仓库遵循 [xlib-standard](https://github.com/ZoneCNH/xlib-standard) 治理协议。

## 项目概述

natsx 1.0 合约是 NATS 的小型显式封装，标准化以下职责：

- **Core NATS**：publish、subscribe、queue subscribe、request/reply，支持 context 取消和超时
- **JetStream**：stream/consumer 管理、publish ack、durable consume、ack/nack/term、redelivery
- **Envelope**：bidirectional NATS header 映射（traceId、messageId、schemaVersion）
- **Subject builder**：`domain.resource.action.v{version}` 规范构造
- **Lifecycle**：connect、flush、drain、close、idempotent shutdown
- **Health / Metrics**：readiness/liveness、connection state、operation counters、敏感值脱敏

公开包：`pkg/natsx`。`pkg/templatex` 是旧模板遗留，不属于 natsx 1.0 API，不得作为模块身份文档化。

## 硬性约束

- **禁止将 `pkg/templatex` 作为 natsx 身份**——旧模板遗留代码，1.0 API 在 `pkg/natsx`
- **禁止依赖 `x.go`** 或任何业务 topic/schema
- **不在公开 API 中泄露 NATS client 具体类型**
- **生产凭证通过 Config 显式注入**，不在源码、日志或 artifact 中泄露
- NATSX_LIVE_INTEGRATION=1 控制实况集成测试，默认不连接外部 broker

## 编辑前基线确认

> 同步自 ZoneCNH/CLAUDE.md 工作流规则。编辑文件前必须先确认当前状态。

- **编辑前先 `git log --oneline -5`，然后 `Read` 确认目标文件当前内容**——禁止假设文件仍是自己记忆中的状态。
- **对文档中的代码事实声称，核对源码后再提交**——用 `grep` 确认字段存在，用 `head` 确认文档不是占位符。
- **先列验证清单，再列变更清单**——先确定需要查什么，验证完再按变更清单编辑。

## 语言规则（全局强制）

1. **回答语言**：所有对话回复默认使用中文，除非用户明确要求使用其他语言。
2. **文档语言**：所有仓库文档默认使用中文叙述。
3. **代码注释**：Go 源码注释默认使用中文。导出符号的 godoc 注释可保留英文，内部代码一律中文。
4. **保留原文的例外**：代码标识符、命令、路径、包名、Go module 路径、外部专有名词、协议固定短语和 git 提交标题保留原文。
5. **提交信息**：正文和 trailer 使用中文；标题保留英文以兼容工具链。
