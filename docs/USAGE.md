# workflow 使用文档

本文档面向业务开发者，说明如何用 `workflow` 编写、运行和测试工作流。

## 基本概念

### Flow

`Flow` 表示一个工作流。它从 `Start` 节点开始执行，每个节点返回一个 action，流程根据 action 选择下一个节点。

```go
flow := workflow.NewFlow("demo", startNode)
```

### Node

`Node` 是最小执行单元。内置节点包括：

- `FuncNode`：通用函数节点。
- `BatchNode`：在一个节点内串行处理多个 item。
- `ParallelBatchNode`：在一个节点内并发处理多个 item。

### RunContext

`RunContext` 在一次运行中传递：

- `context.Context`：取消和超时控制。
- `FlowID`、`RunID`、节点元数据。
- `Params`：当前节点生效参数。
- `Shared`：节点间共享状态。
- `Logger`：可选日志器。
- `Events`：可选事件接收器。

`NewRunContext` 会为 nil 输入提供默认值，并自动生成 `RunID`。

## 编写普通节点

`FuncNode` 的执行顺序是：

```text
Prep -> Exec(可重试) -> Fallback(可选) -> Post -> action
```

示例：

```go
start := workflow.NewFuncNode(workflow.NodeMeta{ID: "start"}).
	SetPrep(func(ctx *workflow.RunContext) (any, error) {
		return ctx.Params["name"], nil
	}).
	SetExec(func(ctx *workflow.RunContext, prepResult any) (any, error) {
		return fmt.Sprintf("hello %s", prepResult), nil
	}).
	SetPost(func(ctx *workflow.RunContext, prepResult any, execResult any) (string, error) {
		ctx.Shared.Set("message", execResult)
		return workflow.DefaultAction, nil
	})
```

## 连接节点

默认路由：

```go
start.Core().Next(next)
```

条件分支：

```go
start.Core().NextAction("approved", approvedNode)
start.Core().NextAction("rejected", rejectedNode)
```

如果节点返回空 action，会被转换为 `workflow.DefaultAction`。

## 参数优先级

节点运行前会合并参数，优先级为：

```text
flow 参数 < run 参数 < batch item 参数 < node 参数
```

示例：

```go
flow.SetParams(workflow.Params{"region": "global"})
node.Core().SetParams(workflow.Params{"region": "cn"})
```

节点内读取：

```go
region := ctx.Params["region"]
```

## 共享状态

`Shared` 是并发安全的 key/value 容器，用于节点间传递运行时状态：

```go
ctx.Shared.Set("order_id", "o_001")
orderID, ok := ctx.Shared.Get("order_id")
```

注意：`Shared` 只保护容器本身。如果 value 是 map、slice、指针等可变对象，其内部并发安全由业务方负责。

## 重试和 fallback

```go
node.Core().SetRetry(3, 500*time.Millisecond)
node.SetFallback(func(ctx *workflow.RunContext, prepResult any, lastErr error) (any, error) {
	return "fallback-value", nil
})
```

语义：

- `Exec` 成功后进入 `Post`。
- `Exec` 全部失败且配置了 fallback，则 fallback 结果作为 `execResult` 进入 `Post`。
- `Exec` 全部失败且未配置 fallback，则返回 `WorkflowError`。

需要指数退避时使用完整重试策略：

```go
node.Core().SetRetryPolicy(workflow.RetryPolicy{
	MaxRetries: 3,
	Wait:       200 * time.Millisecond,
	MaxWait:    2 * time.Second,
	Backoff:    workflow.BackoffExponential,
	Jitter:     100 * time.Millisecond,
})
```

## 超时

流程级超时：

```go
flow.SetTimeout(30 * time.Second)
```

节点级超时：

```go
node.Core().SetTimeout(3 * time.Second)
```

节点级超时会把节点执行上下文替换为带 deadline 的 context。节点代码需要主动监听：

```go
select {
case <-ctx.Done():
	return nil, ctx.Err()
case result := <-work:
	return result, nil
}
```

Go 无法安全强杀正在运行的 goroutine，因此如果节点忽略 `ctx.Done()`，超时只能在节点返回后被识别。

## 批处理节点

串行批处理：

```go
batch := workflow.NewBatchNode(workflow.NodeMeta{ID: "batch"}).
	SetPrep(func(ctx *workflow.RunContext) ([]any, error) {
		return []any{1, 2, 3}, nil
	}).
	SetExecItem(func(ctx *workflow.RunContext, item any, index int) (any, error) {
		return item.(int) * 2, nil
	}).
	SetPost(func(ctx *workflow.RunContext, items []any, results []any) (string, error) {
		ctx.Shared.Set("results", results)
		return workflow.DefaultAction, nil
	})
```

并行批处理：

```go
parallel := workflow.NewParallelBatchNode(workflow.NodeMeta{ID: "parallel"}, 8)
parallel.FailFast = true
```

并行批处理会按输入索引保存结果。`FailFast` 为 true 时，首个错误会取消尚未完成的工作。

## 批处理流程

`BatchFlow` 会用多组参数重复执行同一张流程图：

```go
bf := workflow.NewBatchFlow("batch-flow", start)
bf.SetPrepBatch(func(ctx *workflow.RunContext) ([]workflow.Params, error) {
	return []workflow.Params{
		{"id": 1},
		{"id": 2},
	}, nil
})
bf.SetPostBatch(func(ctx *workflow.RunContext, batchParams []workflow.Params) (string, error) {
	return workflow.DefaultAction, nil
})
```

并行批处理流程：

```go
pf := workflow.NewParallelBatchFlow("parallel-flow", start, 4)
pf.FailFast = true
```

## 静态校验

生产环境建议在服务启动时校验流程图：

```go
if err := flow.Validate(); err != nil {
	return err
}
```

当前校验范围：

- flow 是否为 nil。
- start node 是否为空。
- node core 是否为空。
- node ID 是否为空。
- 是否存在重复 node ID。
- 是否存在 nil successor。
- 是否存在环路。

如果业务确实需要循环流程，应跳过 `Validate` 或在上层实现最大循环次数、退出条件和超时保护。

## 事件监听

```go
rc := workflow.NewRunContext(context.Background(), workflow.NewShared(nil), nil)
rc.Events = workflow.EventSinkFunc(func(event workflow.Event) {
	fmt.Printf("%s flow=%s run=%s node=%s action=%s err=%s\n",
		event.Type, event.FlowID, event.RunID, event.NodeID, event.Action, event.Error)
})

_, err := flow.RunWithContext(rc)
```

事件接收器中的 panic 会被恢复，并通过 `RunContext.Logger` 记录，不会打断主流程。

## 错误处理

```go
var wfErr *workflow.WorkflowError
if errors.As(err, &wfErr) {
	fmt.Println(wfErr.Code, wfErr.Stage, wfErr.NodeID, wfErr.Msg)
}
```

常见错误码：

- `prep_failed`
- `exec_failed`
- `post_failed`
- `fallback_failed`
- `flow_failed`
- `batch_failed`
- `timeout`
- `cancelled`
- `panic`
- `validation_failed`

常见阶段：

- `prep`
- `exec`
- `fallback`
- `post`
- `flow`
- `batch`
- `panic`

panic 会被转换为 `WorkflowError`，并在 `Stack` 字段中保留调用栈。

## 推荐运行方式

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

shared := workflow.NewShared(nil)
rc := workflow.NewRunContext(ctx, shared, workflow.Params{"request_id": "req_001"})
rc.Logger = logger
rc.Events = eventSink

if err := flow.Validate(); err != nil {
	return err
}

action, err := flow.RunWithContext(rc)
```
