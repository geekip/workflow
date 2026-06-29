package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Flow 从单一起始节点开始编排有向节点图。
//
// 每个节点都会返回 action，该 action 会从当前节点的后继表中选择下一个节点。
// 找不到后继节点时，流程结束。
type Flow struct {
	ID          string
	Name        string
	Description string

	Start         Node
	Params        Params
	timeout       time.Duration
	StrictRouting bool
}

// NewFlow 使用指定 id 和起始节点创建工作流。
func NewFlow(id string, start Node) *Flow {
	if id == "" {
		panic("flow id cannot be empty")
	}

	return &Flow{
		ID:     id,
		Start:  start,
		Params: Params{},
	}
}

// NewFlowE 使用指定 id 和起始节点创建工作流，并用 error 返回配置错误。
func NewFlowE(id string, start Node) (*Flow, error) {
	if id == "" {
		return nil, errors.New("flow id cannot be empty")
	}

	return NewFlow(id, start), nil
}

// SetParams 替换流程级默认参数。
func (f *Flow) SetParams(params Params) *Flow {
	f.Params = CopyParams(params)
	return f
}

// Timeout 返回流程级超时时长；0 表示不设置流程级超时。
func (f *Flow) Timeout() time.Duration {
	return f.timeout
}

// SetTimeout 配置流程级超时时长；0 表示不设置流程级超时。
func (f *Flow) SetTimeout(timeout time.Duration) *Flow {
	if timeout < 0 {
		panic("timeout cannot be negative")
	}

	f.timeout = timeout
	return f
}

// SetStrictRouting 配置是否把未匹配 action 视为错误。
func (f *Flow) SetStrictRouting(strict bool) *Flow {
	f.StrictRouting = strict
	return f
}

// Run 创建 RunContext 并执行流程。
func (f *Flow) Run(ctx context.Context, shared *Shared, params Params) (string, error) {
	rc := NewRunContext(ctx, shared, params)
	rc.FlowID = f.ID

	return f.RunWithContext(rc)
}

// RunWithContext 使用已有 RunContext 执行流程。
func (f *Flow) RunWithContext(ctx *RunContext) (action string, err error) {
	if f.Start == nil {
		return "", errors.New("flow start node is nil")
	}
	if ctx == nil {
		ctx = NewRunContext(nil, nil, nil)
	}
	if ctx.FlowID == "" {
		ctx.FlowID = f.ID
	}
	if ctx.RunID == "" {
		ctx.RunID = newRunID()
	}
	if f.timeout > 0 {
		runCtx, cancel := context.WithTimeout(ctx.Context, f.timeout)
		defer cancel()
		ctx.Context = runCtx
	}

	startedAt := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = wrapPanic(StagePanic, "", "flow panic", r)
			ctx.Emit(Event{
				Type:     EventFlowFinished,
				FlowID:   f.ID,
				RunID:    ctx.RunID,
				Error:    err.Error(),
				EndedAt:  time.Now(),
				Duration: time.Since(startedAt),
			})
			action = ""
		}
	}()

	ctx.Emit(Event{
		Type:      EventFlowStarted,
		FlowID:    f.ID,
		RunID:     ctx.RunID,
		StartedAt: startedAt,
	})

	action, err = f.orchestrate(ctx, nil)
	if err != nil {
		ctx.Emit(Event{
			Type:     EventFlowFinished,
			FlowID:   f.ID,
			RunID:    ctx.RunID,
			Error:    err.Error(),
			EndedAt:  time.Now(),
			Duration: time.Since(startedAt),
		})

		return "", err
	}

	ctx.Emit(Event{
		Type:     EventFlowFinished,
		FlowID:   f.ID,
		RunID:    ctx.RunID,
		Action:   action,
		EndedAt:  time.Now(),
		Duration: time.Since(startedAt),
	})

	return action, nil
}

// orchestrate 遍历节点图，直到最后一个 action 找不到匹配的后继节点。
func (f *Flow) orchestrate(ctx *RunContext, batchParams Params) (string, error) {
	current := f.Start
	lastAction := DefaultAction

	for current != nil {
		if err := ctx.Err(); err != nil {
			return "", wrapErr(StageFlow, "", "flow context cancelled", err)
		}

		core := current.Core()
		meta := core.Meta()

		// 参数优先级为 flow < run < batch item < node。
		mergedParams := MergeParams(
			f.Params,
			ctx.Params,
			batchParams,
			core.Params(),
		)

		nodeCtx := ctx.WithNode(meta, mergedParams)

		timeout := core.Timeout()
		var action string
		var err error
		if timeout > 0 {
			nodeRunCtx, cancel := context.WithTimeout(nodeCtx.Context, timeout)
			nodeCtx.Context = nodeRunCtx
			action, err = current.Run(nodeCtx)
			cancel()
			if err == nil && nodeRunCtx.Err() != nil {
				err = wrapErr(StageFlow, meta.ID, "node context deadline exceeded", nodeRunCtx.Err())
			}
			if err != nil {
				return "", err
			}
		} else {
			action, err = current.Run(nodeCtx)
			if err != nil {
				return "", err
			}
		}
		if err := ctx.Err(); err != nil {
			return "", wrapErr(StageFlow, meta.ID, "flow context cancelled after node run", err)
		}

		lastAction = normalizeAction(action)
		next := core.GetNext(lastAction)
		if next == nil && f.StrictRouting && len(core.SuccessorsSnapshot()) > 0 {
			return "", wrapErr(
				StageFlow,
				meta.ID,
				fmt.Sprintf("no successor for action=%s", lastAction),
				ErrMissingSuccessor,
			)
		}
		current = next
	}

	return lastAction, nil
}

// BatchFlow 使用准备好的多组参数重复执行同一个工作流图，
// 然后运行最终的批处理收尾钩子。
type BatchFlow struct {
	*Flow

	PrepBatchFunc func(ctx *RunContext) ([]Params, error)
	PostBatchFunc func(ctx *RunContext, batchParams []Params) (string, error)
}

// NewBatchFlow 创建串行批处理流程。
func NewBatchFlow(id string, start Node) *BatchFlow {
	return &BatchFlow{
		Flow: NewFlow(id, start),
		PrepBatchFunc: func(ctx *RunContext) ([]Params, error) {
			return nil, nil
		},
		PostBatchFunc: func(ctx *RunContext, batchParams []Params) (string, error) {
			return DefaultAction, nil
		},
	}
}

// NewBatchFlowE 创建串行批处理流程，并用 error 返回配置错误。
func NewBatchFlowE(id string, start Node) (*BatchFlow, error) {
	if id == "" {
		return nil, errors.New("flow id cannot be empty")
	}

	return NewBatchFlow(id, start), nil
}

// SetPrepBatch 替换用于准备每次运行参数集的钩子。
func (bf *BatchFlow) SetPrepBatch(f func(ctx *RunContext) ([]Params, error)) *BatchFlow {
	bf.PrepBatchFunc = f
	return bf
}

// SetPostBatch 替换所有批处理项完成后运行的钩子。
func (bf *BatchFlow) SetPostBatch(f func(ctx *RunContext, batchParams []Params) (string, error)) *BatchFlow {
	bf.PostBatchFunc = f
	return bf
}

// RunWithContext 串行执行每一组准备好的参数。
func (bf *BatchFlow) RunWithContext(ctx *RunContext) (action string, err error) {
	if bf.Start == nil {
		return "", errors.New("batch flow start node is nil")
	}
	if ctx == nil {
		ctx = NewRunContext(nil, nil, nil)
	}
	if ctx.FlowID == "" {
		ctx.FlowID = bf.ID
	}
	if ctx.RunID == "" {
		ctx.RunID = newRunID()
	}
	if bf.timeout > 0 {
		runCtx, cancel := context.WithTimeout(ctx.Context, bf.timeout)
		defer cancel()
		ctx.Context = runCtx
	}
	defer func() {
		if r := recover(); r != nil {
			err = wrapPanic(StagePanic, "", "batch flow panic", r)
			action = ""
		}
	}()

	batchParamsList, err := bf.PrepBatchFunc(ctx)
	if err != nil {
		return "", wrapErr(StagePrep, "", "batch flow prep failed", err)
	}
	if batchParamsList == nil {
		batchParamsList = []Params{}
	}

	for i, batchParams := range batchParamsList {
		if err := ctx.Err(); err != nil {
			return "", wrapErr(StageFlow, "", "batch flow context cancelled", err)
		}

		_, err := bf.orchestrate(ctx, batchParams)
		if err != nil {
			return "", wrapErr(StageBatch, "", fmt.Sprintf("batch flow item failed index=%d", i), err)
		}
	}

	action, err = bf.PostBatchFunc(ctx, batchParamsList)
	if err != nil {
		return "", wrapErr(StagePost, "", "batch flow post failed", err)
	}

	return normalizeAction(action), nil
}

// ParallelBatchFlow 并发执行准备好的多组参数。
type ParallelBatchFlow struct {
	*BatchFlow

	MaxConcurrency int
	FailFast       bool
}

// NewParallelBatchFlow 创建并行批处理流程。
//
// maxConcurrency 小于等于 0 时会使用默认值 8。FailFast 默认开启。
func NewParallelBatchFlow(id string, start Node, maxConcurrency int) *ParallelBatchFlow {
	if maxConcurrency <= 0 {
		maxConcurrency = 8
	}

	return &ParallelBatchFlow{
		BatchFlow:      NewBatchFlow(id, start),
		MaxConcurrency: maxConcurrency,
		FailFast:       true,
	}
}

// NewParallelBatchFlowE 创建并行批处理流程，并用 error 返回配置错误。
func NewParallelBatchFlowE(id string, start Node, maxConcurrency int) (*ParallelBatchFlow, error) {
	if id == "" {
		return nil, errors.New("flow id cannot be empty")
	}

	return NewParallelBatchFlow(id, start, maxConcurrency), nil
}

// SetPrepBatch 替换用于准备每次运行参数集的钩子，并返回 ParallelBatchFlow 以支持链式调用。
func (pf *ParallelBatchFlow) SetPrepBatch(f func(ctx *RunContext) ([]Params, error)) *ParallelBatchFlow {
	pf.PrepBatchFunc = f
	return pf
}

// SetPostBatch 替换所有批处理项完成后运行的钩子，并返回 ParallelBatchFlow 以支持链式调用。
func (pf *ParallelBatchFlow) SetPostBatch(f func(ctx *RunContext, batchParams []Params) (string, error)) *ParallelBatchFlow {
	pf.PostBatchFunc = f
	return pf
}

// RunWithContext 并发执行批处理流程项，并在所有项成功后运行 PostBatch。
func (pf *ParallelBatchFlow) RunWithContext(ctx *RunContext) (action string, err error) {
	if pf.Start == nil {
		return "", errors.New("parallel batch flow start node is nil")
	}
	if ctx == nil {
		ctx = NewRunContext(nil, nil, nil)
	}
	if ctx.FlowID == "" {
		ctx.FlowID = pf.ID
	}
	if ctx.RunID == "" {
		ctx.RunID = newRunID()
	}
	if pf.timeout > 0 {
		runCtx, cancel := context.WithTimeout(ctx.Context, pf.timeout)
		defer cancel()
		ctx.Context = runCtx
	}
	defer func() {
		if r := recover(); r != nil {
			err = wrapPanic(StagePanic, "", "parallel batch flow panic", r)
			action = ""
		}
	}()

	batchParamsList, err := pf.PrepBatchFunc(ctx)
	if err != nil {
		return "", wrapErr(StagePrep, "", "parallel batch flow prep failed", err)
	}
	if batchParamsList == nil {
		batchParamsList = []Params{}
	}

	runCtx, cancel := context.WithCancel(ctx.Context)
	defer cancel()

	childCtx := *ctx
	childCtx.Context = runCtx

	type flowJob struct {
		index       int
		batchParams Params
	}

	jobs := make(chan flowJob)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	// setErr 记录第一个 item 错误，并按需取消尚未完成的工作。
	setErr := func(err error) {
		if err == nil {
			return
		}

		errMu.Lock()
		defer errMu.Unlock()

		if firstErr == nil {
			firstErr = err
			if pf.FailFast {
				cancel()
			}
		}
	}

	workerCount := normalizeWorkerCount(pf.MaxConcurrency, len(batchParamsList))

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				if runCtx.Err() != nil && pf.FailFast {
					setErr(runCtx.Err())
					return
				}

				_, err := pf.orchestrate(&childCtx, job.batchParams)
				if err != nil {
					setErr(wrapErr(StageBatch, "", fmt.Sprintf("parallel batch flow item failed index=%d", job.index), err))
					return
				}
			}
		}()
	}

enqueue:
	for i, batchParams := range batchParamsList {
		if runCtx.Err() != nil && pf.FailFast {
			break
		}

		select {
		case <-runCtx.Done():
			setErr(runCtx.Err())
			break enqueue
		case jobs <- flowJob{index: i, batchParams: batchParams}:
		}
	}
	close(jobs)

	wg.Wait()

	if firstErr != nil {
		return "", firstErr
	}

	action, err = pf.PostBatchFunc(ctx, batchParamsList)
	if err != nil {
		return "", wrapErr(StagePost, "", "parallel batch flow post failed", err)
	}

	return normalizeAction(action), nil
}
