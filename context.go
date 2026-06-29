package workflow

import (
	"context"
	"log"
	"sync"
)

// Params 保存传入流程和节点的运行时参数。
//
// 参数合并采用后写覆盖规则，后面的 Params 会覆盖前面同名 key 的值。
type Params map[string]any

// CopyParams 返回 src 的浅拷贝。
func CopyParams(src Params) Params {
	dst := make(Params, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// MergeParams 按顺序合并多组 Params。
//
// key 冲突时，后面 map 中的值会覆盖前面 map 中的值。
func MergeParams(items ...Params) Params {
	out := Params{}
	for _, item := range items {
		for k, v := range item {
			out[k] = v
		}
	}
	return out
}

// Shared 是一次运行中所有节点共享的并发安全 key/value 存储。
type Shared struct {
	mu   sync.RWMutex
	data map[string]any
}

// NewShared 创建 Shared，并复制 initial 中的初始值。
func NewShared(initial map[string]any) *Shared {
	s := &Shared{
		data: map[string]any{},
	}

	for k, v := range initial {
		s.data[k] = v
	}

	return s
}

// Get 从共享存储中读取值。
func (s *Shared) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	return v, ok
}

// MustGet 返回值并忽略该 key 是否存在。
func (s *Shared) MustGet(key string) any {
	v, _ := s.Get(key)
	return v
}

// Set 向共享存储写入值。
func (s *Shared) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete 从共享存储中删除值。
func (s *Shared) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}

// Snapshot 返回当前共享存储中所有值的浅拷贝。
func (s *Shared) Snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp := make(map[string]any, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}

	return cp
}

// RunContext 在单次工作流运行中传递取消信号、参数、共享状态、日志和事件。
type RunContext struct {
	context.Context

	FlowID string
	RunID  string

	NodeID   string
	NodeName string
	NodeType string

	Params Params
	Shared *Shared

	Logger *log.Logger
	Events EventSink
}

// NewRunContext 创建运行上下文，并为 nil 输入提供安全默认值。
func NewRunContext(ctx context.Context, shared *Shared, params Params) *RunContext {
	if ctx == nil {
		ctx = context.Background()
	}
	if shared == nil {
		shared = NewShared(nil)
	}
	if params == nil {
		params = Params{}
	}

	return &RunContext{
		Context: ctx,
		Params:  CopyParams(params),
		Shared:  shared,
	}
}

// WithNode 返回一个节点作用域的运行上下文副本。
//
// 返回的上下文会保留取消信号、日志器、事件接收器和共享状态，
// 同时替换节点元数据和当前生效参数。
func (rc *RunContext) WithNode(meta NodeMeta, params Params) *RunContext {
	cp := *rc
	cp.NodeID = meta.ID
	cp.NodeName = meta.Name
	cp.NodeType = meta.Type
	cp.Params = CopyParams(params)

	return &cp
}

// Logf 在配置了 Logger 时写入日志。
func (rc *RunContext) Logf(format string, args ...any) {
	if rc.Logger != nil {
		rc.Logger.Printf(format, args...)
	}
}

// Emit 在配置了事件接收器时发送事件。
func (rc *RunContext) Emit(event Event) {
	if rc.Events != nil {
		rc.Events.Emit(event)
	}
}
