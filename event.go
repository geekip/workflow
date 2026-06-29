package workflow

import (
	"context"
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

// AsyncEventSink 使用内存缓冲异步转发事件。
//
// 它适合隔离较慢的观测逻辑。缓冲区满时 Emit 会阻塞，从而保留 backpressure；
// 如果需要丢弃策略，调用方可以在下游 EventSink 中自行实现。
type AsyncEventSink struct {
	events chan Event
	done   chan struct{}
	stop   context.CancelFunc
}

// NewAsyncEventSink 创建异步事件接收器并启动后台转发 goroutine。
func NewAsyncEventSink(ctx context.Context, buffer int, sink EventSink) *AsyncEventSink {
	if buffer < 0 {
		buffer = 0
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)

	a := &AsyncEventSink{
		events: make(chan Event, buffer),
		done:   make(chan struct{}),
		stop:   cancel,
	}

	go func() {
		defer close(a.done)
		for {
			select {
			case <-runCtx.Done():
				a.drain(sink)
				return
			case event, ok := <-a.events:
				if !ok {
					a.drain(sink)
					return
				}
				emitEvent(sink, event)
			}
		}
	}()

	return a
}

// Emit 将事件写入异步缓冲区。Close 后或构造时传入的 context 取消后，Emit 会直接丢弃事件。
func (a *AsyncEventSink) Emit(event Event) {
	defer func() {
		_ = recover()
	}()

	select {
	case <-a.done:
		return
	case a.events <- event:
	}
}

// Close 停止接收新事件，尽力转发已进入缓冲区的事件，并等待后台 goroutine 退出。
// Close 可重复调用。
func (a *AsyncEventSink) Close() {
	defer func() {
		_ = recover()
		<-a.done
	}()

	a.stop()
	close(a.events)
	<-a.done
}

func (a *AsyncEventSink) drain(sink EventSink) {
	for {
		select {
		case event, ok := <-a.events:
			if !ok {
				return
			}
			emitEvent(sink, event)
		default:
			return
		}
	}
}

func emitEvent(sink EventSink, event Event) {
	if sink == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	sink.Emit(event)
}
