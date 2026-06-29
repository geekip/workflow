# 设计边界

`workflow` 的定位是纯 Go、轻量、可嵌入、可测试的工作流执行内核。它负责把一组节点按 action 路由串起来，并提供执行期需要的基础保护。

它不是完整工作流平台，也不尝试取代 Temporal、Argo Workflows、Airflow 等平台级系统。

## 库负责什么

核心职责：

- 节点编排。
- action 路由。
- 参数合并。
- context 取消。
- 流程级和节点级 timeout。
- 重试与 fallback。
- 串行和并行批处理。
- 生命周期事件。
- 可选异步事件包装。
- 结构化错误。
- panic 防护。
- 静态流程图校验。
- 并发安全的共享状态容器。

这些能力都是执行内核的一部分，适合直接放在库内。

## 库不负责什么

以下能力应放在业务平台或上层系统中实现：

- Web UI。
- REST API。
- CLI 平台工具。
- 多租户和权限。
- 人工审批。
- 定时调度。
- 分布式 worker。
- 数据库持久化实现。
- 队列实现。
- 审计后台。
- 业务补偿事务平台。
- 业务幂等记录。
- 全局限流和熔断。

这些能力通常和业务域、组织治理、基础设施选型强相关，放进内核会让库变重，并削弱可嵌入性。

## 扩展方式

业务平台可以通过以下方式接入：

- 使用 `EventSink` 把事件接入日志、指标、trace 或异步队列。
- 使用 `RunContext.Params` 传入 request id、tenant id、trace id 等业务上下文。
- 使用 `Shared` 在节点间传递运行期状态。
- 使用外部数据库记录运行结果、幂等 key 和审计日志。
- 在节点内部接入限流、熔断、权限、补偿等业务能力。

## 超时语义

`Flow.SetTimeout` 和 `CoreNode.SetTimeout` 都基于 `context.WithTimeout`。

Go 无法安全强杀正在运行的 goroutine，因此节点必须尊重 `ctx.Done()`：

```go
select {
case <-ctx.Done():
	return nil, ctx.Err()
case result := <-work:
	return result, nil
}
```

如果节点忽略 context，库只能在节点返回后识别超时，不能强制中断执行。

## 重试语义

重试只负责重新调用 `Exec` 或 `ExecItem`。库不保证业务幂等。

外部写操作必须由业务方提供：

- 幂等 key。
- 唯一索引。
- 外部 API 去重 token。
- 状态机保护。
- 重试前后的状态检查。

## 事件语义

`EventSink` 默认是同步调用。sink panic 会被恢复并记录，不会中断主流程。

库提供轻量 `AsyncEventSink`，用于把慢观测逻辑和主流程隔离。它使用内存缓冲，缓冲区满时保留 backpressure。

如果需要持久化事件、丢弃策略、批量发送、跨进程队列或复杂 backpressure，建议业务平台在 `EventSink` 外层实现。

## 错误语义

关键阶段错误会被包装为 `WorkflowError`：

```go
type WorkflowError struct {
	Code   ErrorCode
	Stage  Stage
	NodeID string
	Msg    string
	Cause  error
	Stack  string
}
```

panic 会转换为 `StagePanic` 和 `ErrCodePanic`，并保留调用栈。

## 发布前建议

使用本库的项目上线前建议检查：

- `flow.Validate()` 通过。
- 流程级 context timeout 已配置。
- 关键节点 `SetTimeout` 已配置。
- 外部写操作具备幂等保护。
- 重试策略符合外部依赖承载能力。
- `EventSink` 已接入业务侧观测系统。
- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过。

这些建议属于使用方接入规范，不代表库内要实现完整平台能力。
