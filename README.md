# workflow

`workflow` 是一个轻量级 Go 工作流编排库，用于把一组可复用节点串成可观测、可重试、可分支的执行流程。它适合构建数据处理流水线、LLM/Agent 编排、任务自动化、批量作业、ETL 小流程，以及需要在业务代码中内嵌流程控制的场景。

## 核心优势

- **轻量无外部依赖**：当前库只依赖 Go 标准库，易于集成和部署。
- **节点模型简单**：普通节点由 `Prep`、`Exec`、`Post`、`Fallback` 组成，职责清晰。
- **Action 分支路由**：节点返回 action，`CoreNode` 按 action 选择下一个节点，天然支持条件分支。
- **内置重试和降级**：`Exec`/`ExecItem` 支持重试；失败后可通过 fallback 产出兜底结果。
- **串行/并行批处理**：同时提供 `BatchNode`、`ParallelBatchNode`、`BatchFlow`、`ParallelBatchFlow`。
- **运行期共享状态**：`Shared` 是并发安全的 key/value 存储，可在节点间传递中间状态。
- **生命周期事件**：通过 `EventSink` 订阅流程和节点开始、结束、失败事件，便于日志、指标和追踪。
- **错误上下文清晰**：`WorkflowError` 带有 code、stage、nodeID、msg 和原始 cause，支持 `errors.As`/`errors.Is`。
- **运行防护增强**：支持静态图校验、流程级/节点级超时、panic 转错误、默认 RunID、事件接收器 panic 隔离。

## 文档导航

- [完整使用文档](docs/USAGE.md)
- [设计边界](docs/DESIGN.md)

## 执行原理

一个 `Flow` 从 `Start` 节点开始执行。每个节点运行后返回一个 action 字符串，流程用这个 action 到当前节点的 successor 映射里查找下一个节点：

1. 构造 `RunContext`，携带 `context.Context`、运行参数、共享状态、日志器和事件接收器。
2. 进入起始节点。
3. 合并参数，优先级为：`Flow.Params < RunContext.Params < batchParams < Node.Params`。
4. 调用节点 `Run`。
5. 节点返回 action；空 action 会被规范化为 `"default"`。
6. 根据 action 查找下一个节点。
7. 找不到下一个节点时，流程结束并返回最后一个 action。

普通 `FuncNode` 的内部执行顺序是：

```text
Prep -> Exec(可重试) -> Fallback(可选，仅 Exec 全部失败后) -> Post -> action
```

批处理节点的内部执行顺序是：

```text
Prep -> 对每个 item 执行 ExecItem(可重试/FallbackItem) -> Post -> action
```

## 安装

当前模块路径是 `github.com/geekip/workflow`：

```bash
go get github.com/geekip/workflow
```

如果你在本地多模块项目中使用，可以在调用方 `go.mod` 中通过 `replace` 指向本目录：

```go
replace github.com/geekip/workflow => /data/llm/workflow
```

## 快速开始

```go
package main

import (
	"context"
	"fmt"

	"github.com/geekip/workflow"
)

func main() {
	start := workflow.NewFuncNode(workflow.NodeMeta{
		ID:   "start",
		Name: "开始",
	}).SetPrep(func(ctx *workflow.RunContext) (any, error) {
		name, _ := ctx.Params["name"].(string)
		return name, nil
	}).SetExec(func(ctx *workflow.RunContext, prepResult any) (any, error) {
		return fmt.Sprintf("hello %s", prepResult), nil
	}).SetPost(func(ctx *workflow.RunContext, prepResult any, execResult any) (string, error) {
		ctx.Shared.Set("message", execResult)
		return workflow.DefaultAction, nil
	})

	end := workflow.NewFuncNode(workflow.NodeMeta{ID: "end"}).
		SetExec(func(ctx *workflow.RunContext, prepResult any) (any, error) {
			msg, _ := ctx.Shared.Get("message")
			fmt.Println(msg)
			return nil, nil
		})

	start.Core().Next(end)

	flow := workflow.NewFlow("demo", start)
	action, err := flow.Run(context.Background(), workflow.NewShared(nil), workflow.Params{
		"name": "workflow",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("finished with action:", action)
}
```

## 条件分支

节点通过 `PostFunc` 返回 action，然后用 `NextAction` 注册不同后继节点：

```go
check := workflow.NewFuncNode(workflow.NodeMeta{ID: "check"}).
	SetPost(func(ctx *workflow.RunContext, prepResult any, execResult any) (string, error) {
		if ctx.Params["enabled"] == true {
			return "enabled", nil
		}
		return "disabled", nil
	})

enabledNode := workflow.NewFuncNode(workflow.NodeMeta{ID: "enabled"})
disabledNode := workflow.NewFuncNode(workflow.NodeMeta{ID: "disabled"})

check.Core().NextAction("enabled", enabledNode)
check.Core().NextAction("disabled", disabledNode)
```

如果 action 找不到对应 successor，流程会在当前分支结束。

## 参数与共享状态

参数用于配置当前节点运行，`Shared` 用于传递运行中产生的状态。

```go
flow := workflow.NewFlow("params-demo", start).
	SetParams(workflow.Params{"timeout": 3})

start.Core().SetParams(workflow.Params{"timeout": 5})
```

参数合并时后者覆盖前者，最终优先级为：

```text
flow 参数 < run 参数 < batch item 参数 < node 参数
```

`Shared` 内部使用读写锁保护，可以被并行节点安全访问：

```go
ctx.Shared.Set("user_id", "u_001")
value, ok := ctx.Shared.Get("user_id")
snapshot := ctx.Shared.Snapshot()
```

注意：`Shared` 只保证 map 读写本身并发安全。如果存进去的是指针、slice、map 等可变对象，对象内部并发安全需要调用方自己保证。

## 重试与降级

`CoreNode.SetRetry` 配置 `Exec` 的最大尝试次数和固定间隔：

```go
node := workflow.NewFuncNode(workflow.NodeMeta{ID: "call-api"})
node.Core().SetRetry(3, time.Second)
node.SetFallback(func(ctx *workflow.RunContext, prepResult any, lastErr error) (any, error) {
	return "fallback result", nil
})
```

当 `Exec` 全部失败后：

- 如果配置了 `FallbackFunc`，使用 fallback 结果继续进入 `Post`。
- 如果没有配置 fallback，返回 `WorkflowError`，stage 为 `exec`。

需要指数退避时使用 `SetRetryPolicy`：

```go
node.Core().SetRetryPolicy(workflow.RetryPolicy{
	MaxRetries: 3,
	Wait:       200 * time.Millisecond,
	MaxWait:    2 * time.Second,
	Backoff:    workflow.BackoffExponential,
	Jitter:     100 * time.Millisecond,
})
```

## 静态校验与超时

服务启动时建议先执行 `Validate`，提前发现空起点、空后继、重复节点 ID 和环路：

```go
if err := flow.Validate(); err != nil {
	return err
}
```

流程级超时通过 `Flow.SetTimeout` 设置，节点级超时通过 `CoreNode.SetTimeout` 设置。超时依赖节点代码尊重 `ctx.Done()`：

```go
flow.SetTimeout(30 * time.Second)
node.Core().SetTimeout(3 * time.Second)
```

节点、批处理节点和批处理流程中的 panic 会被转换为 `WorkflowError`，stage 为 `panic`，并保留 `Stack` 便于诊断。

## 批处理节点

`BatchNode` 在一个节点内部处理多个 item，默认串行执行：

```go
batch := workflow.NewBatchNode(workflow.NodeMeta{ID: "batch"}).
	SetPrep(func(ctx *workflow.RunContext) ([]any, error) {
		return []any{"a", "b", "c"}, nil
	}).
	SetExecItem(func(ctx *workflow.RunContext, item any, index int) (any, error) {
		return fmt.Sprintf("%d:%s", index, item), nil
	}).
	SetPost(func(ctx *workflow.RunContext, items []any, results []any) (string, error) {
		ctx.Shared.Set("results", results)
		return workflow.DefaultAction, nil
	})
```

需要并发处理 item 时使用 `ParallelBatchNode`：

```go
parallel := workflow.NewParallelBatchNode(workflow.NodeMeta{ID: "parallel"}, 8)
parallel.FailFast = true
```

`ParallelBatchNode` 会保持 `results` 与输入 item 的索引一致。`FailFast` 为 `true` 时，首个错误会取消尚未开始或正在等待的任务。

## 批处理流程

`BatchFlow` 会用多组参数重复执行同一个流程图：

```go
bf := workflow.NewBatchFlow("batch-flow", start).
	SetPrepBatch(func(ctx *workflow.RunContext) ([]workflow.Params, error) {
		return []workflow.Params{
			{"id": 1},
			{"id": 2},
		}, nil
	}).
	SetPostBatch(func(ctx *workflow.RunContext, batchParams []workflow.Params) (string, error) {
		return workflow.DefaultAction, nil
	})
```

需要并行执行多组参数时使用：

```go
pf := workflow.NewParallelBatchFlow("parallel-flow", start, 4)
pf.FailFast = true
```

## 事件监听

通过 `RunContext.Events` 可以监听生命周期事件：

```go
rc := workflow.NewRunContext(context.Background(), workflow.NewShared(nil), nil)
rc.FlowID = "demo"
rc.Events = workflow.EventSinkFunc(func(event workflow.Event) {
	fmt.Printf("%s flow=%s node=%s action=%s err=%s\n",
		event.Type, event.FlowID, event.NodeID, event.Action, event.Error)
})

_, err := flow.RunWithContext(rc)
```

事件类型包括：

- `flow_started`
- `flow_finished`
- `node_started`
- `node_finished`
- `node_failed`

## 错误处理

库内部会把关键阶段错误包装成 `WorkflowError`：

```go
var wfErr *workflow.WorkflowError
if errors.As(err, &wfErr) {
	fmt.Println(wfErr.Code, wfErr.Stage, wfErr.NodeID, wfErr.Msg)
}
```

常见 stage：

- `prep`
- `exec`
- `fallback`
- `post`
- `flow`
- `batch`
- `panic`

## 并发注意事项

- `CoreNode` 的参数、重试策略、后继路由读写是并发安全的。
- `Shared` 的 key/value 容器读写是并发安全的。
- 并行节点会复用同一个 `Shared`，适合汇总状态，但写同一个 key 时需要调用方设计覆盖策略。
- 并行节点的 `RunContext` 会共享 logger 和 event sink；如果自定义实现有内部状态，需要保证它们自身并发安全。
- `context.Context` 取消会中断重试等待、并行调度和后续流程执行。

## 适用场景

- 需要把业务步骤拆成可测试节点的自动化流程。
- 需要根据节点结果进行条件分支的任务编排。
- 需要对外部 API 调用增加重试、降级和观测事件。
- 需要处理批量数据，并控制并发度。
- 需要构建轻量级 LLM/Agent 工作流，但不想引入重量级编排系统。

## 当前限制

- 没有持久化执行状态，进程退出后运行状态不会恢复。
- 没有 DAG 静态校验，错误 action 或环路需要调用方自行设计和测试。
- 没有内置超时策略，应通过传入的 `context.Context` 控制。
- `Params` 和 `Shared` 都是浅拷贝语义，复杂对象的并发安全由调用方负责。
