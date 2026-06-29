# 生产接入模板

本文档提供生产环境常见接入方式。`workflow` 核心库保持零外部依赖，OpenTelemetry、数据库、审计、幂等和告警都建议放在业务侧适配层。

## 接入原则

- 服务启动时构建流程图，并执行 `flow.Validate()`。
- 生产关键流程开启 `flow.SetStrictRouting(true)`。
- 设置流程级 timeout，并为关键外部调用节点设置节点级 timeout。
- 对外部写操作设计幂等 key，重试只重试可安全重放的操作。
- 通过 `EventSink` 接入 trace、metrics、日志、审计和运行记录。
- 慢事件处理使用 `AsyncEventSink`，缓冲大小按峰值事件量和可接受 backpressure 设定。

## OpenTelemetry 适配示例

OpenTelemetry 官方建议应用侧初始化 SDK，库侧只依赖 API。这里的适配器放在业务项目中，不放进 `workflow` 模块。

```go
package observability

import (
	"context"

	"github.com/geekip/workflow"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OTelEventSink struct {
	tracer trace.Tracer
	spans  *spanStore
}

func NewOTelEventSink() *OTelEventSink {
	return &OTelEventSink{
		tracer: otel.Tracer("github.com/geekip/workflow"),
		spans:  newSpanStore(),
	}
}

func (s *OTelEventSink) Emit(event workflow.Event) {
	key := event.RunID + ":" + event.NodeID

	switch event.Type {
	case workflow.EventFlowStarted:
		_, span := s.tracer.Start(context.Background(), "workflow "+event.FlowID,
			trace.WithAttributes(
				attribute.String("workflow.flow_id", event.FlowID),
				attribute.String("workflow.run_id", event.RunID),
			),
		)
		s.spans.Set(event.RunID, span)

	case workflow.EventFlowFinished:
		if span, ok := s.spans.Delete(event.RunID); ok {
			if event.Error != "" {
				span.RecordError(errorString(event.Error))
				span.SetStatus(codes.Error, event.Error)
			}
			span.SetAttributes(attribute.String("workflow.action", event.Action))
			span.End()
		}

	case workflow.EventNodeStarted:
		parentCtx := context.Background()
		if parent, ok := s.spans.Get(event.RunID); ok {
			parentCtx = trace.ContextWithSpan(parentCtx, parent)
		}
		_, span := s.tracer.Start(parentCtx, "workflow node "+event.NodeID,
			trace.WithAttributes(
				attribute.String("workflow.flow_id", event.FlowID),
				attribute.String("workflow.run_id", event.RunID),
				attribute.String("workflow.node_id", event.NodeID),
				attribute.String("workflow.node_name", event.NodeName),
				attribute.String("workflow.node_type", event.NodeType),
			),
		)
		s.spans.Set(key, span)

	case workflow.EventNodeFinished, workflow.EventNodeFailed:
		if span, ok := s.spans.Delete(key); ok {
			span.SetAttributes(attribute.String("workflow.action", event.Action))
			if event.Error != "" {
				span.RecordError(errorString(event.Error))
				span.SetStatus(codes.Error, event.Error)
			}
			span.End()
		}
	}
}
```

`spanStore` 可以用一个带 `sync.Mutex` 的 map 实现：

```go
type spanStore struct {
	mu    sync.Mutex
	spans map[string]trace.Span
}

func newSpanStore() *spanStore {
	return &spanStore{spans: map[string]trace.Span{}}
}

func (s *spanStore) Set(key string, span trace.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans[key] = span
}

func (s *spanStore) Get(key string) (trace.Span, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	span, ok := s.spans[key]
	return span, ok
}

func (s *spanStore) Delete(key string) (trace.Span, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	span, ok := s.spans[key]
	delete(s.spans, key)
	return span, ok
}

type errorString string

func (e errorString) Error() string {
	return string(e)
}
```

生产项目通常还会在 HTTP/RPC 入口创建 root span，然后把入口 `context.Context` 传给 `workflow.NewRunContext`，这样节点内外部调用可以继续沿用同一条 trace。

## 持久化和审计适配示例

核心库不持久化运行状态。业务侧可以把事件写入数据库，用于运行记录、审计、排障和离线统计。

建议最小表结构：

```sql
create table workflow_runs (
	run_id text primary key,
	flow_id text not null,
	status text not null,
	action text not null default '',
	error text not null default '',
	started_at timestamp not null,
	ended_at timestamp null,
	created_at timestamp not null default current_timestamp,
	updated_at timestamp not null default current_timestamp
);

create table workflow_node_events (
	id bigserial primary key,
	run_id text not null,
	flow_id text not null,
	node_id text not null default '',
	node_name text not null default '',
	node_type text not null default '',
	event_type text not null,
	action text not null default '',
	error text not null default '',
	started_at timestamp null,
	ended_at timestamp null,
	duration_ms bigint not null default 0,
	created_at timestamp not null default current_timestamp
);
```

业务侧 `EventSink` 示例：

```go
type AuditStore interface {
	UpsertRunStarted(ctx context.Context, event workflow.Event) error
	MarkRunFinished(ctx context.Context, event workflow.Event) error
	AppendNodeEvent(ctx context.Context, event workflow.Event) error
}

type AuditEventSink struct {
	ctx   context.Context
	store AuditStore
	logf  func(string, ...any)
}

func NewAuditEventSink(ctx context.Context, store AuditStore, logf func(string, ...any)) *AuditEventSink {
	if ctx == nil {
		ctx = context.Background()
	}
	return &AuditEventSink{ctx: ctx, store: store, logf: logf}
}

func (s *AuditEventSink) Emit(event workflow.Event) {
	var err error

	switch event.Type {
	case workflow.EventFlowStarted:
		err = s.store.UpsertRunStarted(s.ctx, event)
	case workflow.EventFlowFinished:
		err = s.store.MarkRunFinished(s.ctx, event)
	case workflow.EventNodeStarted, workflow.EventNodeFinished, workflow.EventNodeFailed:
		err = s.store.AppendNodeEvent(s.ctx, event)
	}

	if err != nil && s.logf != nil {
		s.logf("workflow audit event failed run=%s flow=%s node=%s type=%s err=%v",
			event.RunID, event.FlowID, event.NodeID, event.Type, err)
	}
}
```

生产建议：

- `workflow_runs.run_id` 使用唯一约束，重复事件用 upsert 保证幂等。
- 节点事件按 append-only 写入，避免覆盖诊断信息。
- 审计写入失败不应打断主流程；如果必须强审计，应在业务节点内显式写事务日志。
- 对审计 sink 外层包一层 `AsyncEventSink`，避免数据库抖动拖慢流程执行。

## 生产 Runner 模板

下面的 Runner 把校验、超时、事件、审计、日志和错误归一化集中在一处，业务 handler 只负责传参。

```go
type Runner struct {
	Flow    *workflow.Flow
	Events  workflow.EventSink
	Logger  *log.Logger
	Timeout time.Duration
}

func NewRunner(flow *workflow.Flow, events workflow.EventSink, logger *log.Logger) (*Runner, error) {
	if flow == nil {
		return nil, errors.New("workflow flow is nil")
	}

	flow.SetStrictRouting(true)
	if flow.Timeout() == 0 {
		flow.SetTimeout(30 * time.Second)
	}
	if err := flow.Validate(); err != nil {
		return nil, err
	}

	return &Runner{
		Flow:    flow,
		Events:  events,
		Logger:  logger,
		Timeout: flow.Timeout(),
	}, nil
}

func (r *Runner) Run(ctx context.Context, params workflow.Params) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok && r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}

	shared := workflow.NewShared(nil)
	rc := workflow.NewRunContext(ctx, shared, params)
	rc.Logger = r.Logger
	rc.Events = r.Events

	action, err := r.Flow.RunWithContext(rc)
	if err != nil {
		var wfErr *workflow.WorkflowError
		if errors.As(err, &wfErr) && r.Logger != nil {
			r.Logger.Printf(
				"workflow failed flow=%s run=%s code=%s stage=%s node=%s err=%v",
				rc.FlowID,
				rc.RunID,
				wfErr.Code,
				wfErr.Stage,
				wfErr.NodeID,
				err,
			)
		}
		return "", err
	}

	return action, nil
}
```

组合多个 sink：

```go
type MultiEventSink []workflow.EventSink

func (m MultiEventSink) Emit(event workflow.Event) {
	for _, sink := range m {
		if sink != nil {
			sink.Emit(event)
		}
	}
}

func NewProductionEvents(ctx context.Context, audit workflow.EventSink, otel workflow.EventSink) *workflow.AsyncEventSink {
	return workflow.NewAsyncEventSink(ctx, 4096, MultiEventSink{audit, otel})
}
```

## 上线检查清单

- 流程图：`flow.Validate()` 在启动期执行并阻断错误配置上线。
- 路由：生产关键流程开启 `SetStrictRouting(true)`。
- 超时：入口 context、流程级 timeout、关键节点 timeout 均已配置。
- 重试：外部写操作有幂等 key；外部读操作有合理退避和最大等待。
- 观测：接入 OpenTelemetry trace，事件包含 `flow_id`、`run_id`、`node_id`。
- 审计：运行记录和节点事件可按 `run_id` 查询。
- 异步事件：`AsyncEventSink.Close()` 在服务退出时调用。
- 压测：并行批处理使用符合下游承载能力的 `MaxConcurrency`。
- CI：`go test ./...`、`go test -race ./...`、`go vet ./...` 必须通过。

## 参考

- OpenTelemetry Go instrumentation: https://opentelemetry.io/docs/languages/go/instrumentation/
