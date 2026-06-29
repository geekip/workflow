# 更新日志

本项目遵循语义化版本。`v0.x` 阶段 API 仍可能调整，但会尽量保持向后兼容，并在本文档记录公开行为变化。

## Unreleased

- 将模块路径规范为 `github.com/geekip/workflow`。
- 为 `ParallelBatchNode` 和 `ParallelBatchFlow` 补充返回具体类型的链式方法。
- 为 `WorkflowError` 增加轻量错误码。
- 扩展 `RetryPolicy`，支持固定退避、指数退避、最大等待和 jitter。
- 增加流程级 timeout。
- 增加 strict routing 模式，未匹配 action 可返回错误。
- 增加返回 error 的构造函数，减少库使用方对 panic 构造路径的依赖。
- `ParallelBatchNode` 和 `ParallelBatchFlow` 改为固定 worker pool，避免大批量任务创建过多 goroutine。
- 增加轻量 `AsyncEventSink`。
- 增加 CI、benchmark 和设计边界文档。

## v0.1.0

- 初始轻量工作流执行内核。
- 支持 `Flow`、`FuncNode`、`BatchNode`、`ParallelBatchNode`、`BatchFlow`、`ParallelBatchFlow`。
- 支持参数合并、共享状态、重试、fallback、事件、结构化错误、panic 防护、节点级 timeout 和静态校验。
