package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BatchPrepFunc 准备 BatchNode 要处理的 item 列表。
type BatchPrepFunc func(ctx *RunContext) ([]any, error)

// BatchExecItemFunc 处理批次中的单个 item。
type BatchExecItemFunc func(ctx *RunContext, item any, index int) (any, error)

// BatchPostFunc 接收所有原始 item 和逐项结果，并返回节点路由 action。
type BatchPostFunc func(ctx *RunContext, items []any, results []any) (string, error)

// BatchFallbackItemFunc 在 item 重试失败后生成兜底结果。
type BatchFallbackItemFunc func(ctx *RunContext, item any, index int, lastErr error) (any, error)

// BatchNode 在一个工作流节点内串行处理 item 列表。
type BatchNode struct {
	core             *CoreNode
	PrepFunc         BatchPrepFunc
	ExecItemFunc     BatchExecItemFunc
	PostFunc         BatchPostFunc
	FallbackItemFunc BatchFallbackItemFunc
}

// NewBatchNode 创建带有空操作默认回调的串行批处理节点。
func NewBatchNode(meta NodeMeta) *BatchNode {
	if meta.Type == "" {
		meta.Type = "batch"
	}

	return &BatchNode{
		core: NewCoreNode(meta),
		PrepFunc: func(ctx *RunContext) ([]any, error) {
			return nil, nil
		},
		ExecItemFunc: func(ctx *RunContext, item any, index int) (any, error) {
			return item, nil
		},
		PostFunc: func(ctx *RunContext, items []any, results []any) (string, error) {
			return DefaultAction, nil
		},
	}
}

// NewBatchNodeE 创建 BatchNode，并用 error 返回配置错误。
func NewBatchNodeE(meta NodeMeta) (*BatchNode, error) {
	if meta.Type == "" {
		meta.Type = "batch"
	}
	if _, err := NewCoreNodeE(meta); err != nil {
		return nil, err
	}

	return NewBatchNode(meta), nil
}

// Core 返回节点共享的核心配置。
func (n *BatchNode) Core() *CoreNode {
	return n.core
}

// SetPrep 替换批处理准备回调。
func (n *BatchNode) SetPrep(f BatchPrepFunc) *BatchNode {
	n.PrepFunc = f
	return n
}

// SetExecItem 替换逐项执行回调。
func (n *BatchNode) SetExecItem(f BatchExecItemFunc) *BatchNode {
	n.ExecItemFunc = f
	return n
}

// SetPost 替换批处理收尾回调。
func (n *BatchNode) SetPost(f BatchPostFunc) *BatchNode {
	n.PostFunc = f
	return n
}

// SetFallbackItem 替换逐项兜底回调。
func (n *BatchNode) SetFallbackItem(f BatchFallbackItemFunc) *BatchNode {
	n.FallbackItemFunc = f
	return n
}

// Run 串行处理每个准备好的 item，然后使用 Post 的结果进行路由。
func (n *BatchNode) Run(ctx *RunContext) (action string, err error) {
	meta := n.core.Meta()
	startedAt := time.Now()

	defer func() {
		if r := recover(); r != nil {
			err = wrapPanic(StagePanic, meta.ID, "batch node panic", r)
			n.emitFailed(ctx, meta, startedAt, err)
			action = ""
		}
	}()

	ctx.Emit(Event{
		Type:      EventNodeStarted,
		FlowID:    ctx.FlowID,
		RunID:     ctx.RunID,
		NodeID:    meta.ID,
		NodeName:  meta.Name,
		NodeType:  meta.Type,
		StartedAt: startedAt,
	})

	items, err := n.PrepFunc(ctx)
	if err != nil {
		err = wrapErr(StagePrep, meta.ID, "batch prep failed", err)
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}
	if items == nil {
		items = []any{}
	}

	results := make([]any, len(items))

	for i, item := range items {
		result, err := n.execItemWithRetry(ctx, item, i)
		if err != nil {
			n.emitFailed(ctx, meta, startedAt, err)
			return "", err
		}

		results[i] = result
	}

	action, err = n.PostFunc(ctx, items, results)
	if err != nil {
		err = wrapErr(StagePost, meta.ID, "batch post failed", err)
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

// execItemWithRetry 按节点重试策略执行单个批处理 item。
func (n *BatchNode) execItemWithRetry(ctx *RunContext, item any, index int) (result any, err error) {
	meta := n.core.Meta()
	policy := n.core.Retry()

	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = wrapPanic(StagePanic, meta.ID, fmt.Sprintf("batch item panic index=%d", index), r)
		}
	}()

	var lastErr error

	for i := 0; i < policy.MaxRetries; i++ {
		if err := ctx.Err(); err != nil {
			return nil, wrapErr(StageExec, meta.ID, "context cancelled before batch item exec", err)
		}

		result, err := n.ExecItemFunc(ctx, item, index)
		if err == nil {
			return result, nil
		}

		lastErr = err

		if i < policy.MaxRetries-1 {
			if err := sleepContext(ctx, policy.Delay(i)); err != nil {
				return nil, wrapErr(StageExec, meta.ID, "context cancelled during batch retry wait", err)
			}
		}
	}

	if n.FallbackItemFunc != nil {
		result, err := n.FallbackItemFunc(ctx, item, index, lastErr)
		if err != nil {
			return nil, wrapErr(
				StageFallback,
				meta.ID,
				fmt.Sprintf("batch item fallback failed index=%d", index),
				err,
			)
		}

		return result, nil
	}

	return nil, wrapErr(
		StageExec,
		meta.ID,
		fmt.Sprintf("batch item exec failed index=%d after %d retries", index, policy.MaxRetries),
		lastErr,
	)
}

// emitFailed 上报带有耗时信息的批处理节点失败事件。
func (n *BatchNode) emitFailed(ctx *RunContext, meta NodeMeta, startedAt time.Time, err error) {
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

// ParallelBatchNode 使用可配置的并发上限并发处理批处理 item。
type ParallelBatchNode struct {
	*BatchNode
	MaxConcurrency int
	FailFast       bool
}

// NewParallelBatchNode 创建并行批处理节点。
//
// maxConcurrency 小于等于 0 时会使用默认值 8。FailFast 默认开启，
// 因此第一个 item 失败会取消尚未完成的工作。
func NewParallelBatchNode(meta NodeMeta, maxConcurrency int) *ParallelBatchNode {
	if meta.Type == "" {
		meta.Type = "parallel_batch"
	}

	if maxConcurrency <= 0 {
		maxConcurrency = 8
	}

	return &ParallelBatchNode{
		BatchNode:      NewBatchNode(meta),
		MaxConcurrency: maxConcurrency,
		FailFast:       true,
	}
}

// NewParallelBatchNodeE 创建 ParallelBatchNode，并用 error 返回配置错误。
func NewParallelBatchNodeE(meta NodeMeta, maxConcurrency int) (*ParallelBatchNode, error) {
	if meta.Type == "" {
		meta.Type = "parallel_batch"
	}
	if _, err := NewCoreNodeE(meta); err != nil {
		return nil, err
	}

	return NewParallelBatchNode(meta, maxConcurrency), nil
}

// SetPrep 替换批处理准备回调，并返回 ParallelBatchNode 以支持链式调用。
func (n *ParallelBatchNode) SetPrep(f BatchPrepFunc) *ParallelBatchNode {
	n.PrepFunc = f
	return n
}

// SetExecItem 替换逐项执行回调，并返回 ParallelBatchNode 以支持链式调用。
func (n *ParallelBatchNode) SetExecItem(f BatchExecItemFunc) *ParallelBatchNode {
	n.ExecItemFunc = f
	return n
}

// SetPost 替换批处理收尾回调，并返回 ParallelBatchNode 以支持链式调用。
func (n *ParallelBatchNode) SetPost(f BatchPostFunc) *ParallelBatchNode {
	n.PostFunc = f
	return n
}

// SetFallbackItem 替换逐项兜底回调，并返回 ParallelBatchNode 以支持链式调用。
func (n *ParallelBatchNode) SetFallbackItem(f BatchFallbackItemFunc) *ParallelBatchNode {
	n.FallbackItemFunc = f
	return n
}

// Run 并发处理准备好的 item，并按输入索引保存结果。
func (n *ParallelBatchNode) Run(ctx *RunContext) (action string, err error) {
	meta := n.core.Meta()
	startedAt := time.Now()

	defer func() {
		if r := recover(); r != nil {
			err = wrapPanic(StagePanic, meta.ID, "parallel batch node panic", r)
			n.emitFailed(ctx, meta, startedAt, err)
			action = ""
		}
	}()

	ctx.Emit(Event{
		Type:      EventNodeStarted,
		FlowID:    ctx.FlowID,
		RunID:     ctx.RunID,
		NodeID:    meta.ID,
		NodeName:  meta.Name,
		NodeType:  meta.Type,
		StartedAt: startedAt,
	})

	items, err := n.PrepFunc(ctx)
	if err != nil {
		err = wrapErr(StagePrep, meta.ID, "parallel batch prep failed", err)
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}
	if items == nil {
		items = []any{}
	}

	results := make([]any, len(items))

	runCtx, cancel := context.WithCancel(ctx.Context)
	defer cancel()

	childCtx := *ctx
	childCtx.Context = runCtx

	type batchJob struct {
		index int
		item  any
	}

	jobs := make(chan batchJob)
	var wg sync.WaitGroup

	var errMu sync.Mutex
	var firstErr error

	// setErr 记录第一个错误，并按需取消其他并发工作。
	setErr := func(err error) {
		if err == nil {
			return
		}

		errMu.Lock()
		defer errMu.Unlock()

		if firstErr == nil {
			firstErr = err
			if n.FailFast {
				cancel()
			}
		}
	}

	workerCount := n.MaxConcurrency
	if len(items) < workerCount {
		workerCount = len(items)
	}

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				if runCtx.Err() != nil && n.FailFast {
					setErr(runCtx.Err())
					return
				}

				result, err := n.execItemWithRetry(&childCtx, job.item, job.index)
				if err != nil {
					setErr(err)
					return
				}

				results[job.index] = result
			}
		}()
	}

enqueue:
	for i, item := range items {
		if runCtx.Err() != nil && n.FailFast {
			break
		}

		select {
		case <-runCtx.Done():
			setErr(runCtx.Err())
			break enqueue
		case jobs <- batchJob{index: i, item: item}:
		}
	}
	close(jobs)

	wg.Wait()

	if firstErr != nil {
		err := wrapErr(StageBatch, meta.ID, "parallel batch failed", firstErr)
		n.emitFailed(ctx, meta, startedAt, err)
		return "", err
	}

	action, err = n.PostFunc(ctx, items, results)
	if err != nil {
		err = wrapErr(StagePost, meta.ID, "parallel batch post failed", err)
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
