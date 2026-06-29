# LLM/Agent 编排规划

本文档说明如何在业务侧基于 `workflow` 实现类似 LangGraph 的 LLM/Agent 编排能力，同时保持 `workflow` 核心库精简、通用和 Go-native。

## 定位

`workflow` 继续定位为轻量工作流执行内核，负责：

- 节点执行。
- action 路由。
- 参数合并。
- context 取消和 timeout。
- 重试、fallback、panic 防护。
- 生命周期事件。
- 静态流程图校验。
- 串行/并行批处理。

`agentgraph` 建议作为业务侧或上层模块实现，负责：

- Agent state 建模。
- LLM 节点封装。
- Tool 节点封装。
- 条件路由。
- Agent 循环。
- step limit。
- memory/session/thread 管理。
- checkpoint 适配。
- streaming 适配。
- human-in-the-loop 适配。

核心原则：

```text
workflow 做执行内核
agentgraph 做 LLM/Agent 语义
业务平台做持久化、审计、UI、权限和分布式治理
```

## 与 LangGraph 的关系

可以借鉴 LangGraph 的状态图思想，但不把 LangGraph 的平台生态能力塞进 `workflow`。

| 能力 | workflow 当前能力 | agentgraph 业务侧封装 |
|---|---|---|
| StateGraph | `Flow` + `Shared` | typed `AgentState` |
| Node | `FuncNode` | `StateNode`、`LLMNode`、`ToolNode` |
| Conditional Edge | `PostFunc` 返回 action | router function |
| Agent Loop | action 回到上游节点 | 显式允许循环并设置 max steps |
| Tool Calling | 业务节点内部实现 | tool registry / tool executor |
| Checkpoint | 不内置 | EventSink 或 node wrapper 持久化 |
| Streaming | EventSink | SSE / WebSocket / channel adapter |
| Human-in-the-loop | 不内置 | 平台侧暂停、审批、恢复 |

## 推荐业务侧模型

### AgentState

业务侧定义强类型状态，不建议直接把所有内容散落在 `Shared` 的多个 key 中。

```go
type AgentState struct {
	ThreadID string
	Step     int
	MaxSteps int
	Messages []Message
	ToolCalls []ToolCall
	ToolResults map[string]ToolResult
	FinalAnswer string
}

type Message struct {
	Role    string
	Content string
}
```

将状态放入 `Shared`：

```go
shared := workflow.NewShared(map[string]any{
	"state": state,
})
```

### StateNodeFunc

业务侧节点函数只关心 typed state：

```go
type StateNodeFunc func(ctx context.Context, state *AgentState) (string, error)
```

返回值是下一条路由 action。

### StateNode 适配器

把 `StateNodeFunc` 适配成 `workflow.FuncNode`：

```go
func NewStateNode(id string, fn StateNodeFunc) *workflow.FuncNode {
	return workflow.NewFuncNode(workflow.NodeMeta{ID: id}).
		SetExec(func(ctx *workflow.RunContext, prep any) (any, error) {
			value := ctx.Shared.MustGet("state")
			state, ok := value.(*AgentState)
			if !ok || state == nil {
				return nil, errors.New("agent state is missing")
			}

			if state.MaxSteps > 0 && state.Step >= state.MaxSteps {
				return "stop", nil
			}
			state.Step++

			return fn(ctx.Context, state)
		}).
		SetPost(func(ctx *workflow.RunContext, prep any, exec any) (string, error) {
			action, _ := exec.(string)
			return action, nil
		})
}
```

## 典型 Agent 图

一个基础 ReAct 风格 Agent 可以拆成：

- `planner`：调用 LLM，决定回答或调用工具。
- `tool`：执行工具调用，把结果写入 state。
- `answer`：生成最终回答。
- `stop`：结束。

路由关系：

```go
planner.Core().NextAction("tool", toolNode)
planner.Core().NextAction("answer", answerNode)
planner.Core().NextAction("stop", stopNode)

toolNode.Core().NextAction("planner", planner)
answerNode.Core().NextAction("stop", stopNode)
```

如果图中存在显式循环，默认 `Validate` 会报错。业务侧可以选择：

- 不对包含循环的 Agent 图执行 `Validate`。
- 或者在业务侧实现 `ValidateAgentGraph`，允许特定 action 回边，并强制校验 `MaxSteps`。

## ToolNode 规划

业务侧可以定义工具接口：

```go
type Tool interface {
	Name() string
	Call(ctx context.Context, input any) (any, error)
}

type ToolRegistry map[string]Tool
```

`ToolNode` 从 state 中读取待执行工具：

```go
func NewToolNode(id string, tools ToolRegistry) *workflow.FuncNode {
	return NewStateNode(id, func(ctx context.Context, state *AgentState) (string, error) {
		for _, call := range state.ToolCalls {
			tool := tools[call.Name]
			if tool == nil {
				return "answer", fmt.Errorf("tool not found: %s", call.Name)
			}

			result, err := tool.Call(ctx, call.Input)
			if err != nil {
				return "answer", err
			}
			state.ToolResults[call.ID] = ToolResult{Value: result}
		}

		return "planner", nil
	})
}
```

## LLMNode 规划

LLM provider 不放入 `workflow`。业务侧定义最小接口：

```go
type ChatModel interface {
	Chat(ctx context.Context, messages []Message) (Message, error)
}
```

`planner` 节点调用模型后，根据模型输出决定 action：

```go
func NewPlannerNode(model ChatModel) *workflow.FuncNode {
	return NewStateNode("planner", func(ctx context.Context, state *AgentState) (string, error) {
		msg, err := model.Chat(ctx, state.Messages)
		if err != nil {
			return "", err
		}
		state.Messages = append(state.Messages, msg)

		if len(state.ToolCalls) > 0 {
			return "tool", nil
		}
		if state.FinalAnswer != "" {
			return "answer", nil
		}
		return "stop", nil
	})
}
```

## Checkpoint 规划

checkpoint 不进入 `workflow` 核心库。建议业务侧实现：

```go
type Checkpointer interface {
	Save(ctx context.Context, threadID string, state *AgentState) error
	Load(ctx context.Context, threadID string) (*AgentState, error)
}
```

接入方式：

- 在每个 StateNode 的 `Post` 后保存 state。
- 或用 `EventSink` 在节点结束事件中保存 state snapshot。
- 对外部写操作，仍需业务幂等 key 保证重复执行安全。

## Streaming 规划

`workflow.EventSink` 可转成 SSE、WebSocket 或 channel：

```go
type StreamEvent struct {
	RunID  string
	NodeID string
	Type   string
	Data   any
}
```

建议拆分两类流：

- workflow lifecycle event：节点开始、结束、失败。
- LLM token event：由业务侧 LLM adapter 直接推送。

不要把 token streaming 放进 `workflow` 核心库；它属于 LLM provider adapter 的职责。

## Human-in-the-loop 规划

Human-in-the-loop 建议在业务平台实现：

- 节点返回 action：`"wait_human"`。
- 平台记录当前 thread/run/state。
- UI 展示审批任务。
- 审批完成后重新构造 state 并从指定节点继续。

当前 `workflow` 不内置暂停/恢复，因为这依赖持久化、权限、UI 和业务审批模型。

## 可靠性建议

业务侧实现 agentgraph 时建议强制：

- 每个 Agent run 设置 `MaxSteps`。
- 所有外部工具调用设置 timeout。
- 所有写操作设计幂等 key。
- LLM 调用设置重试上限和总 timeout。
- checkpoint 保存失败时明确策略：失败流程、降级继续或告警。
- 事件流和 token 流分离。

## 不建议放入 workflow 的能力

以下能力不应进入 `workflow` 核心库：

- prompt 模板。
- 模型 provider SDK。
- tool calling 协议。
- agent memory 实现。
- vector store。
- checkpoint 数据库实现。
- human approval UI。
- session/thread API。
- token streaming 协议。

这些能力应由 `agentgraph`、业务平台或应用层实现。

## 演进路线

建议分三步演进：

1. **业务侧 adapter**
   - `AgentState`
   - `NewStateNode`
   - `ToolRegistry`
   - `ChatModel`
   - max steps

2. **示例项目**
   - `examples/agent_graph`
   - planner/tool/answer/stop
   - mock LLM
   - mock tool
   - in-memory checkpoint

3. **平台接入**
   - checkpoint store
   - SSE/WebSocket streaming
   - human approval
   - audit log
   - trace/metrics

这样可以在业务侧获得类似 LangGraph 的核心体验，同时保持 `workflow` 本身小而稳。
