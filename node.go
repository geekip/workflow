package workflow

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const DefaultAction = "default"

// normalizeAction 将空 action 转换为默认路由名。
func normalizeAction(action string) string {
	if action == "" {
		return DefaultAction
	}
	return action
}

// RetryPolicy 控制 Exec 的尝试次数和每次尝试之间的等待时间。
type RetryPolicy struct {
	MaxRetries int
	Wait       time.Duration
}

// normalize 使用安全默认值修正无效的零值配置。
func (p RetryPolicy) normalize() RetryPolicy {
	if p.MaxRetries <= 0 {
		p.MaxRetries = 1
	}
	if p.Wait < 0 {
		p.Wait = 0
	}

	return p
}

// sleepContext 等待 d 时长；如果 ctx 被取消，则提前返回。
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NodeMeta 描述节点信息，用于路由、事件和诊断。
type NodeMeta struct {
	ID          string
	Name        string
	Type        string
	Description string
}

// Node 是工作流图中的可执行单元。
//
// Run 返回 action 字符串。节点的 CoreNode 会使用该 action 选择下一个后继节点。
// 返回空 action 等价于返回 DefaultAction。
type Node interface {
	Core() *CoreNode
	Run(ctx *RunContext) (string, error)
}

// CoreNode 保存所有具体节点实现共享的元数据、默认参数、重试策略和后继路由。
type CoreNode struct {
	meta       NodeMeta
	params     Params
	retry      RetryPolicy
	successors map[string]Node
	mu         sync.RWMutex
}

// NewCoreNode 创建 CoreNode，并为 Type 和 Name 应用默认值。
func NewCoreNode(meta NodeMeta) *CoreNode {
	if meta.ID == "" {
		panic("node id cannot be empty")
	}
	if meta.Type == "" {
		meta.Type = "node"
	}
	if meta.Name == "" {
		meta.Name = meta.ID
	}

	return &CoreNode{
		meta:       meta,
		params:     Params{},
		retry:      RetryPolicy{MaxRetries: 1},
		successors: map[string]Node{},
	}
}

// Meta 返回当前节点的不可变元数据。
func (c *CoreNode) Meta() NodeMeta {
	return c.meta
}

// Params 返回节点默认参数的浅拷贝。
func (c *CoreNode) Params() Params {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CopyParams(c.params)
}

// SetParams 替换节点默认参数。
func (c *CoreNode) SetParams(params Params) *CoreNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.params = CopyParams(params)
	return c
}

// Retry 返回规范化后的重试策略。
func (c *CoreNode) Retry() RetryPolicy {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.retry.normalize()
}

// SetRetry 配置 Exec 的重试次数和等待时长。
func (c *CoreNode) SetRetry(maxRetries int, wait time.Duration) *CoreNode {
	if maxRetries < 1 {
		panic("maxRetries must be at least 1")
	}
	if wait < 0 {
		panic("wait cannot be negative")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.retry = RetryPolicy{
		MaxRetries: maxRetries,
		Wait:       wait,
	}

	return c
}

// Next 将 node 注册为 DefaultAction 的后继节点，并返回 node。
func (c *CoreNode) Next(node Node) Node {
	return c.NextAction(DefaultAction, node)
}

// NextAction 将 node 注册为指定 action 的后继节点。
func (c *CoreNode) NextAction(action string, node Node) Node {
	if node == nil {
		panic("successor node cannot be nil")
	}

	action = normalizeAction(action)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.successors[action]; exists {
		log.Printf("WARN: workflow - overwriting successor action=%s node=%s", action, c.meta.ID)
	}

	c.successors[action] = node
	return node
}

// GetNext 解析 action 对应的后继节点。
//
// 找不到 action 时返回 nil，表示当前流程分支结束。
func (c *CoreNode) GetNext(action string) Node {
	action = normalizeAction(action)

	c.mu.RLock()
	defer c.mu.RUnlock()

	next := c.successors[action]
	if next == nil && len(c.successors) > 0 {
		keys := make([]string, 0, len(c.successors))
		for k := range c.successors {
			keys = append(keys, k)
		}

		log.Printf(
			"WARN: workflow - action=%s not found in successors=%v node=%s",
			action,
			keys,
			c.meta.ID,
		)
	}

	return next
}

// SuccessorsSnapshot 返回 action 到节点路由表的浅拷贝。
func (c *CoreNode) SuccessorsSnapshot() map[string]Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cp := make(map[string]Node, len(c.successors))
	for k, v := range c.successors {
		cp[k] = v
	}

	return cp
}

// PrepFunc 为 FuncNode 的 Exec 阶段准备输入。
type PrepFunc func(ctx *RunContext) (any, error)

// ExecFunc 执行 FuncNode 的核心工作。
type ExecFunc func(ctx *RunContext, prepResult any) (any, error)

// PostFunc 将 Prep/Exec 结果转换为路由 action。
type PostFunc func(ctx *RunContext, prepResult any, execResult any) (string, error)

// FallbackFunc 在全部重试失败后生成兜底的 Exec 结果。
type FallbackFunc func(ctx *RunContext, prepResult any, lastErr error) (any, error)

// FuncNode 是由 Prep、Exec、Post 和可选 Fallback 回调实现的通用节点。
type FuncNode struct {
	core *CoreNode

	PrepFunc     PrepFunc
	ExecFunc     ExecFunc
	PostFunc     PostFunc
	FallbackFunc FallbackFunc
}

// NewFuncNode 创建带有空操作默认回调的 FuncNode。
func NewFuncNode(meta NodeMeta) *FuncNode {
	return &FuncNode{
		core: NewCoreNode(meta),
		PrepFunc: func(ctx *RunContext) (any, error) {
			return nil, nil
		},
		ExecFunc: func(ctx *RunContext, prepResult any) (any, error) {
			return nil, nil
		},
		PostFunc: func(ctx *RunContext, prepResult any, execResult any) (string, error) {
			return DefaultAction, nil
		},
	}
}

// Core 返回节点共享的核心配置。
func (n *FuncNode) Core() *CoreNode {
	return n.core
}

// SetPrep 替换 Prep 回调。
func (n *FuncNode) SetPrep(f PrepFunc) *FuncNode {
	n.PrepFunc = f
	return n
}

// SetExec 替换 Exec 回调。
func (n *FuncNode) SetExec(f ExecFunc) *FuncNode {
	n.ExecFunc = f
	return n
}

// SetPost 替换 Post 回调。
func (n *FuncNode) SetPost(f PostFunc) *FuncNode {
	n.PostFunc = f
	return n
}

// SetFallback 替换 Exec 重试失败后的兜底回调。
func (n *FuncNode) SetFallback(f FallbackFunc) *FuncNode {
	n.FallbackFunc = f
	return n
}

// Run 执行 Prep、可重试的 Exec 和 Post，并发送节点生命周期事件。
func (n *FuncNode) Run(ctx *RunContext) (string, error) {
	meta := n.core.Meta()
	startedAt := time.Now()

	ctx.Emit(Event{
		Type:      EventNodeStarted,
		FlowID:    ctx.FlowID,
		RunID:     ctx.RunID,
		NodeID:    meta.ID,
		NodeName:  meta.Name,
		NodeType:  meta.Type,
		StartedAt: startedAt,
	})

	prepResult, err := n.PrepFunc(ctx)
	if err != nil {
		err = wrapErr(StagePrep, meta.ID, "prep failed", err)
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}

	execResult, err := n.execWithRetry(ctx, prepResult)
	if err != nil {
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}

	action, err := n.PostFunc(ctx, prepResult, execResult)
	if err != nil {
		err = wrapErr(StagePost, meta.ID, "post failed", err)
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}

	action = normalizeAction(action)

	ctx.Emit(Event{
		Type:     EventNodeFinished,
		FlowID:   ctx.FlowID,
		RunID:    ctx.RunID,
		NodeID:   meta.ID,
		NodeName: meta.Name,
		NodeType: meta.Type,
		Action:   action,
		EndedAt:  time.Now(),
		Duration: time.Since(startedAt),
	})

	return action, nil
}

// execWithRetry 按节点重试策略执行 Exec。
//
// 如果所有尝试都失败且配置了 fallback，则使用 fallback 结果作为 Exec 结果，
// 节点随后可以继续进入 Post 阶段。
func (n *FuncNode) execWithRetry(ctx *RunContext, prepResult any) (any, error) {
	meta := n.core.Meta()
	policy := n.core.Retry()

	var lastErr error

	for i := 0; i < policy.MaxRetries; i++ {
		if err := ctx.Err(); err != nil {
			return nil, wrapErr(StageExec, meta.ID, "context cancelled before exec", err)
		}

		result, err := n.ExecFunc(ctx, prepResult)
		if err == nil {
			return result, nil
		}

		lastErr = err

		if i < policy.MaxRetries-1 {
			if err := sleepContext(ctx, policy.Wait); err != nil {
				return nil, wrapErr(StageExec, meta.ID, "context cancelled during retry wait", err)
			}
		}
	}

	if n.FallbackFunc != nil {
		result, err := n.FallbackFunc(ctx, prepResult, lastErr)
		if err != nil {
			return nil, wrapErr(StageFallback, meta.ID, "fallback failed", err)
		}
		return result, nil
	}

	return nil, wrapErr(
		StageExec,
		meta.ID,
		fmt.Sprintf("exec failed after %d retries", policy.MaxRetries),
		lastErr,
	)
}

// emitFailed 上报带有耗时信息的节点失败事件。
func (n *FuncNode) emitFailed(ctx *RunContext, meta NodeMeta, startedAt time.Time, err error) {
	ctx.Emit(Event{
		Type:     EventNodeFailed,
		FlowID:   ctx.FlowID,
		RunID:    ctx.RunID,
		NodeID:   meta.ID,
		NodeName: meta.Name,
		NodeType: meta.Type,
		Error:    err.Error(),
		EndedAt:  time.Now(),
		Duration: time.Since(startedAt),
	})
}
