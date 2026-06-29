package workflow

import (
	"time"
)

// EventType 标识工作流生命周期事件类型。
type EventType string

const (
	EventFlowStarted  EventType = "flow_started"
	EventFlowFinished EventType = "flow_finished"
	EventNodeStarted  EventType = "node_started"
	EventNodeFinished EventType = "node_finished"
	EventNodeFailed   EventType = "node_failed"
)

// Event 描述流程或节点的生命周期通知。
//
// 使用方可以在 RunContext 上挂载 EventSink，用于收集指标、日志或链路追踪，
// 避免将这些观测逻辑耦合进节点业务代码。
type Event struct {
	Type      EventType
	FlowID    string
	RunID     string
	NodeID    string
	NodeName  string
	NodeType  string
	Action    string
	Error     string
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
}

// EventSink 接收工作流生命周期事件。
type EventSink interface {
	Emit(event Event)
}

// EventSinkFunc 将普通函数适配为 EventSink 接口。
type EventSinkFunc func(event Event)

// Emit 调用 f(event)。
func (f EventSinkFunc) Emit(event Event) {
	f(event)
}
