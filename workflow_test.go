package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParamsCopyAndMerge(t *testing.T) {
	src := Params{"a": 1, "b": 2}
	cp := CopyParams(src)
	cp["a"] = 10

	if src["a"] != 1 {
		t.Fatalf("CopyParams 应返回浅拷贝，原始值不应被修改: got %v", src["a"])
	}

	merged := MergeParams(
		Params{"a": 1, "b": 1},
		nil,
		Params{"b": 2},
		Params{"c": 3},
	)

	want := Params{"a": 1, "b": 2, "c": 3}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("MergeParams() = %#v, want %#v", merged, want)
	}
}

func TestSharedConcurrentAccessAndSnapshot(t *testing.T) {
	shared := NewShared(map[string]any{"initial": "value"})

	if got := shared.MustGet("initial"); got != "value" {
		t.Fatalf("MustGet(initial) = %v, want value", got)
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			shared.Set(key, i)
			if got, ok := shared.Get(key); !ok || got != i {
				t.Errorf("Get(%s) = (%v, %v), want (%d, true)", key, got, ok, i)
			}
		}()
	}
	wg.Wait()

	snapshot := shared.Snapshot()
	snapshot["initial"] = "changed"

	if got := shared.MustGet("initial"); got != "value" {
		t.Fatalf("Snapshot 应返回浅拷贝，修改快照不应影响 Shared: got %v", got)
	}

	shared.Delete("initial")
	if _, ok := shared.Get("initial"); ok {
		t.Fatal("Delete 后 key 仍然存在")
	}
}

func TestFlowRoutingParamPrecedenceAndEvents(t *testing.T) {
	shared := NewShared(nil)
	var events []Event

	start := NewFuncNode(NodeMeta{ID: "start"}).
		SetPrep(func(ctx *RunContext) (any, error) {
			if got := ctx.Params["value"]; got != "node" {
				t.Fatalf("参数优先级错误: got %v, want node", got)
			}
			if got := ctx.Params["flow_only"]; got != true {
				t.Fatalf("缺少 flow 参数: got %v", got)
			}
			if got := ctx.Params["run_only"]; got != true {
				t.Fatalf("缺少 run 参数: got %v", got)
			}
			return "prepared", nil
		}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			if prepResult != "prepared" {
				t.Fatalf("prepResult = %v, want prepared", prepResult)
			}
			ctx.Shared.Set("from_start", "ok")
			return "executed", nil
		}).
		SetPost(func(ctx *RunContext, prepResult any, execResult any) (string, error) {
			if execResult != "executed" {
				t.Fatalf("execResult = %v, want executed", execResult)
			}
			return "next", nil
		})
	start.Core().SetParams(Params{"value": "node"})

	end := NewFuncNode(NodeMeta{ID: "end"}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			if got := ctx.Shared.MustGet("from_start"); got != "ok" {
				t.Fatalf("Shared 未传递上游结果: got %v", got)
			}
			return nil, nil
		})

	start.Core().NextAction("next", end)

	flow := NewFlow("flow-routing", start).
		SetParams(Params{"value": "flow", "flow_only": true})
	rc := NewRunContext(context.Background(), shared, Params{"value": "run", "run_only": true})
	rc.FlowID = flow.ID
	rc.RunID = "run-1"
	rc.Events = EventSinkFunc(func(event Event) {
		events = append(events, event)
	})

	action, err := flow.RunWithContext(rc)
	if err != nil {
		t.Fatalf("RunWithContext returned error: %v", err)
	}
	if action != DefaultAction {
		t.Fatalf("action = %q, want %q", action, DefaultAction)
	}

	gotTypes := make([]EventType, 0, len(events))
	for _, event := range events {
		gotTypes = append(gotTypes, event.Type)
	}
	wantTypes := []EventType{
		EventFlowStarted,
		EventNodeStarted,
		EventNodeFinished,
		EventNodeStarted,
		EventNodeFinished,
		EventFlowFinished,
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("事件顺序 = %#v, want %#v", gotTypes, wantTypes)
	}
}

func TestFlowRunDefaultsAndCoreHelpers(t *testing.T) {
	var logs bytes.Buffer
	core := NewCoreNode(NodeMeta{ID: "core"})

	core.SetParams(Params{"a": 1})
	params := core.Params()
	params["a"] = 2
	if got := core.Params()["a"]; got != 1 {
		t.Fatalf("CoreNode.Params 应返回浅拷贝: got %v", got)
	}

	successor := NewFuncNode(NodeMeta{ID: "successor"})
	if got := core.Next(successor); got != successor {
		t.Fatalf("Next 返回值 = %v, want successor", got)
	}
	snapshot := core.SuccessorsSnapshot()
	if snapshot[DefaultAction] != successor {
		t.Fatalf("SuccessorsSnapshot 未包含默认后继")
	}
	delete(snapshot, DefaultAction)
	if core.GetNext(DefaultAction) != successor {
		t.Fatalf("修改 SuccessorsSnapshot 不应影响内部路由")
	}

	node := NewFuncNode(NodeMeta{ID: "log"}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			ctx.Logf("hello %s", "workflow")
			return nil, nil
		})
	flow := NewFlow("run-defaults", node)
	rcShared := NewShared(nil)

	action, err := flow.Run(context.Background(), rcShared, nil)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if action != DefaultAction {
		t.Fatalf("action = %q, want %q", action, DefaultAction)
	}

	rc := NewRunContext(context.Background(), nil, nil)
	rc.Logger = log.New(&logs, "", 0)
	_, err = node.Run(rc)
	if err != nil {
		t.Fatalf("node.Run returned error: %v", err)
	}
	if logs.String() != "hello workflow\n" {
		t.Fatalf("log = %q, want hello workflow\\n", logs.String())
	}
}

func TestFuncNodeRetryFallbackAndWorkflowError(t *testing.T) {
	t.Run("retry_then_fallback_continues_to_post", func(t *testing.T) {
		var attempts int
		node := NewFuncNode(NodeMeta{ID: "retry"}).
			SetPrep(func(ctx *RunContext) (any, error) {
				return "input", nil
			}).
			SetExec(func(ctx *RunContext, prepResult any) (any, error) {
				attempts++
				return nil, errors.New("temporary")
			}).
			SetFallback(func(ctx *RunContext, prepResult any, lastErr error) (any, error) {
				if attempts != 3 {
					t.Fatalf("fallback 前 attempts = %d, want 3", attempts)
				}
				if lastErr == nil || lastErr.Error() != "temporary" {
					t.Fatalf("lastErr = %v, want temporary", lastErr)
				}
				return "fallback-result", nil
			}).
			SetPost(func(ctx *RunContext, prepResult any, execResult any) (string, error) {
				if execResult != "fallback-result" {
					t.Fatalf("execResult = %v, want fallback-result", execResult)
				}
				return "", nil
			})
		node.Core().SetRetry(3, 0)

		action, err := node.Run(NewRunContext(context.Background(), nil, nil))
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if action != DefaultAction {
			t.Fatalf("action = %q, want %q", action, DefaultAction)
		}
	})

	t.Run("exec_error_is_wrapped", func(t *testing.T) {
		cause := errors.New("boom")
		node := NewFuncNode(NodeMeta{ID: "fail"}).
			SetExec(func(ctx *RunContext, prepResult any) (any, error) {
				return nil, cause
			})

		_, err := node.Run(NewRunContext(context.Background(), nil, nil))
		if err == nil {
			t.Fatal("Run expected error, got nil")
		}

		var wfErr *WorkflowError
		if !errors.As(err, &wfErr) {
			t.Fatalf("error %T 未包装为 WorkflowError: %v", err, err)
		}
		if wfErr.Stage != StageExec || wfErr.NodeID != "fail" {
			t.Fatalf("WorkflowError = stage:%s node:%s, want exec/fail", wfErr.Stage, wfErr.NodeID)
		}
		if !errors.Is(err, cause) {
			t.Fatalf("errors.Is 未识别原始错误: %v", err)
		}
	})
}

func TestBatchNodeSequentialRetryFallbackAndPost(t *testing.T) {
	attempts := map[int]int{}
	node := NewBatchNode(NodeMeta{ID: "batch"}).
		SetPrep(func(ctx *RunContext) ([]any, error) {
			return []any{1, 2, 3}, nil
		}).
		SetExecItem(func(ctx *RunContext, item any, index int) (any, error) {
			value := item.(int)
			attempts[value]++

			if value == 2 && attempts[value] == 1 {
				return nil, errors.New("retry item")
			}
			if value == 3 {
				return nil, errors.New("fallback item")
			}
			return value * 10, nil
		}).
		SetFallbackItem(func(ctx *RunContext, item any, index int, lastErr error) (any, error) {
			value := item.(int)
			return value * 100, nil
		}).
		SetPost(func(ctx *RunContext, items []any, results []any) (string, error) {
			want := []any{10, 20, 300}
			if !reflect.DeepEqual(results, want) {
				t.Fatalf("results = %#v, want %#v", results, want)
			}
			ctx.Shared.Set("batch_results", results)
			return "done", nil
		})
	node.Core().SetRetry(2, 0)

	action, err := node.Run(NewRunContext(context.Background(), NewShared(nil), nil))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if action != "done" {
		t.Fatalf("action = %q, want done", action)
	}
	if attempts[2] != 2 || attempts[3] != 2 {
		t.Fatalf("attempts = %#v, want item2=2 item3=2", attempts)
	}
}

func TestParallelBatchNodeConcurrencyAndResultOrder(t *testing.T) {
	var current atomic.Int32
	var maxSeen atomic.Int32

	node := NewParallelBatchNode(NodeMeta{ID: "parallel"}, 3)
	node.SetPrep(func(ctx *RunContext) ([]any, error) {
		items := make([]any, 12)
		for i := range items {
			items[i] = i
		}
		return items, nil
	})
	node.SetExecItem(func(ctx *RunContext, item any, index int) (any, error) {
		now := current.Add(1)
		for {
			max := maxSeen.Load()
			if now <= max || maxSeen.CompareAndSwap(max, now) {
				break
			}
		}
		defer current.Add(-1)

		time.Sleep(5 * time.Millisecond)
		return item.(int) * 2, nil
	})
	node.SetPost(func(ctx *RunContext, items []any, results []any) (string, error) {
		want := []any{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22}
		if !reflect.DeepEqual(results, want) {
			t.Fatalf("results = %#v, want %#v", results, want)
		}
		return "parallel_done", nil
	})

	action, err := node.Run(NewRunContext(context.Background(), nil, nil))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if action != "parallel_done" {
		t.Fatalf("action = %q, want parallel_done", action)
	}
	if got := maxSeen.Load(); got > 3 {
		t.Fatalf("最大并发 = %d, want <= 3", got)
	}
}

func TestParallelBatchNodeFailFastWrapsError(t *testing.T) {
	cause := errors.New("parallel item failed")
	node := NewParallelBatchNode(NodeMeta{ID: "parallel-fail"}, 2)
	node.SetPrep(func(ctx *RunContext) ([]any, error) {
		return []any{1, 2, 3}, nil
	})
	node.SetExecItem(func(ctx *RunContext, item any, index int) (any, error) {
		if item.(int) == 2 {
			return nil, cause
		}
		return item, nil
	})

	_, err := node.Run(NewRunContext(context.Background(), nil, nil))
	if err == nil {
		t.Fatal("Run expected error, got nil")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is 未识别原始错误: %v", err)
	}

	var wfErr *WorkflowError
	if !errors.As(err, &wfErr) {
		t.Fatalf("error %T 未包装为 WorkflowError: %v", err, err)
	}
	if wfErr.Stage != StageBatch || wfErr.NodeID != "parallel-fail" {
		t.Fatalf("WorkflowError = stage:%s node:%s, want batch/parallel-fail", wfErr.Stage, wfErr.NodeID)
	}
}

func TestBatchFlowParamOverrideAndPost(t *testing.T) {
	var seen []int
	start := NewFuncNode(NodeMeta{ID: "item"}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			seen = append(seen, ctx.Params["id"].(int))
			return nil, nil
		})

	flow := NewBatchFlow("batch-flow", start).
		SetPrepBatch(func(ctx *RunContext) ([]Params, error) {
			return []Params{{"id": 1}, {"id": 2}, {"id": 3}}, nil
		}).
		SetPostBatch(func(ctx *RunContext, batchParams []Params) (string, error) {
			if len(batchParams) != 3 {
				t.Fatalf("batchParams len = %d, want 3", len(batchParams))
			}
			return "all_done", nil
		})

	action, err := flow.RunWithContext(NewRunContext(context.Background(), nil, Params{"id": 0}))
	if err != nil {
		t.Fatalf("RunWithContext returned error: %v", err)
	}
	if action != "all_done" {
		t.Fatalf("action = %q, want all_done", action)
	}
	if want := []int{1, 2, 3}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen = %#v, want %#v", seen, want)
	}
}

func TestParallelBatchFlowFailFastWrapsError(t *testing.T) {
	cause := errors.New("item failed")
	start := NewFuncNode(NodeMeta{ID: "item"}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			if ctx.Params["id"] == 2 {
				return nil, cause
			}
			return nil, nil
		})

	flow := NewParallelBatchFlow("parallel-flow", start, 2)
	flow.SetPrepBatch(func(ctx *RunContext) ([]Params, error) {
		return []Params{{"id": 1}, {"id": 2}, {"id": 3}}, nil
	})

	_, err := flow.RunWithContext(NewRunContext(context.Background(), nil, nil))
	if err == nil {
		t.Fatal("RunWithContext expected error, got nil")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is 未识别原始错误: %v", err)
	}

	var wfErr *WorkflowError
	if !errors.As(err, &wfErr) {
		t.Fatalf("error %T 未包装为 WorkflowError: %v", err, err)
	}
	if wfErr.Stage != StageBatch {
		t.Fatalf("outer stage = %s, want batch", wfErr.Stage)
	}
}

func TestParallelBatchFlowSuccess(t *testing.T) {
	var mu sync.Mutex
	seen := map[int]bool{}

	start := NewFuncNode(NodeMeta{ID: "parallel-item"}).
		SetExec(func(ctx *RunContext, prepResult any) (any, error) {
			id := ctx.Params["id"].(int)
			mu.Lock()
			seen[id] = true
			mu.Unlock()
			return nil, nil
		})

	flow := NewParallelBatchFlow("parallel-flow-success", start, 2)
	flow.SetPrepBatch(func(ctx *RunContext) ([]Params, error) {
		return []Params{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}}, nil
	})
	flow.SetPostBatch(func(ctx *RunContext, batchParams []Params) (string, error) {
		return "parallel_flow_done", nil
	})

	action, err := flow.RunWithContext(NewRunContext(context.Background(), nil, nil))
	if err != nil {
		t.Fatalf("RunWithContext returned error: %v", err)
	}
	if action != "parallel_flow_done" {
		t.Fatalf("action = %q, want parallel_flow_done", action)
	}

	want := map[int]bool{1: true, 2: true, 3: true, 4: true}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen = %#v, want %#v", seen, want)
	}
}
