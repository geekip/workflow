# 生产环境接入建议

本文档面向平台、后端和 SRE 团队，说明如何把 `workflow` 作为服务内工作流编排库接入生产环境。

## 适用边界

适合：

- 服务内部的短生命周期流程编排。
- 可重试、可重跑、幂等性可由业务方保障的任务。
- LLM/Agent 调用链、数据处理流水线、批量 API 调用、自动化业务步骤。
- 不希望引入外部调度平台的轻量场景。

不适合直接承担：

- 需要进程崩溃后自动恢复的长事务。
- 必须持久化每一步执行状态的关键任务。
- 需要跨服务调度、人工审批、定时调度、复杂补偿事务的流程平台。

这类场景应考虑 Temporal、Argo Workflows、Airflow 等平台级系统，或在本库外层补充持久化和调度能力。

## 启动期校验

所有生产流程都建议在服务启动时执行：

```go
if err := flow.Validate(); err != nil {
	return fmt.Errorf("invalid workflow: %w", err)
}
```

校验失败应阻止服务启动或阻止该流程注册，避免错误图配置进入运行期。

## 超时策略

建议同时配置三层超时：

- 请求级或任务级 `context.WithTimeout`。
- 关键节点 `CoreNode.SetTimeout`。
- 外部依赖客户端自身的 timeout。

示例：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

node.Core().SetTimeout(3 * time.Second)
```

节点实现必须监听 `ctx.Done()`。Go 无法安全杀死忽略 context 的 goroutine。

## 幂等与重试

库提供重试机制，但不提供业务幂等保障。生产写操作必须由业务方提供：

- 幂等 key，例如 request id、order id、task id。
- 外部 API 的去重 token。
- 数据库唯一索引或状态机保护。
- 重试前后的状态检查。

建议把幂等 key 放入 `Params` 或 `Shared`：

```go
rc := workflow.NewRunContext(ctx, shared, workflow.Params{
	"request_id": requestID,
})
```

## 事件与可观测性

建议把 `EventSink` 接入日志、指标或 tracing：

```go
rc.Events = workflow.EventSinkFunc(func(event workflow.Event) {
	metrics.Count("workflow.event", 1,
		"type", string(event.Type),
		"flow", event.FlowID,
		"node", event.NodeID,
	)
})
```

注意事项：

- `EventSink` 当前是同步调用，应避免执行慢操作。
- 如需写网络或磁盘，建议在 sink 内投递到异步队列。
- sink 内 panic 会被恢复并记录，不会中断主流程。
- 自定义 sink 如果维护内部状态，需要自行保证并发安全。

## 错误分类

生产日志应记录：

- `WorkflowError.Stage`
- `WorkflowError.NodeID`
- `WorkflowError.Msg`
- `WorkflowError.Cause`
- `WorkflowError.Stack`，仅 panic 场景有值。
- `RunContext.FlowID`
- `RunContext.RunID`

推荐处理方式：

```go
var wfErr *workflow.WorkflowError
if errors.As(err, &wfErr) {
	logger.Printf("workflow failed flow=%s run=%s stage=%s node=%s msg=%s err=%v",
		rc.FlowID, rc.RunID, wfErr.Stage, wfErr.NodeID, wfErr.Msg, wfErr.Cause)
}
```

## 并发安全

已内置保护：

- `Shared` 的 map 读写。
- `CoreNode` 的参数、重试策略、超时和路由配置读写。
- 并行批处理的错误记录和并发限流。

业务方仍需负责：

- `Shared` 中可变对象的内部并发安全。
- 自定义 `EventSink` 的并发安全。
- 自定义 `Logger` 的并发安全。
- 多个并行 item 写同一个 key 时的覆盖策略。

## panic 处理

节点、批处理节点和批处理流程中的 panic 会被转换为：

```go
WorkflowError{
	Stage: StagePanic,
	NodeID: "...",
	Msg: "...",
	Cause: error("panic: ..."),
	Stack: "...",
}
```

建议生产日志在 panic 场景记录 `Stack`，但避免把调用栈直接返回给终端用户。

## 发布前检查清单

每个流程上线前建议完成：

- `flow.Validate()` 通过。
- 所有外部写操作具备幂等保护。
- 流程级 context timeout 已配置。
- 关键节点 `SetTimeout` 已配置。
- 重试次数和等待时间符合外部依赖限流要求。
- `EventSink` 已接入日志/指标/tracing。
- 单元测试覆盖成功路径、失败路径、fallback、超时和取消。
- `go test ./...` 通过。
- `go test -race ./...` 通过。
- `go vet ./...` 通过。

## 当前限制

- 不持久化执行状态。
- 不支持崩溃恢复。
- 不内置分布式锁、调度器或任务队列。
- 不内置业务补偿事务。
- 不内置异步事件队列。
- 不强制幂等。

这些限制是轻量内嵌式设计的取舍。关键任务可以在外层组合数据库、队列、幂等表和调度系统来补齐。
