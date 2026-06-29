package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func BenchmarkFlowChain(b *testing.B) {
	start := NewFuncNode(NodeMeta{ID: "node-0"})
	prev := start
	for i := 1; i < 100; i++ {
		next := NewFuncNode(NodeMeta{ID: fmt.Sprintf("node-%d", i)})
		prev.Core().Next(next)
		prev = next
	}
	flow := NewFlow("chain", start)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := flow.Run(ctx, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlowValidateLargeGraph(b *testing.B) {
	start := NewFuncNode(NodeMeta{ID: "node-0"})
	prev := start
	for i := 1; i < 1000; i++ {
		next := NewFuncNode(NodeMeta{ID: fmt.Sprintf("node-%d", i)})
		prev.Core().Next(next)
		prev = next
	}
	flow := NewFlow("large-validate", start)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := flow.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchNode(b *testing.B) {
	node := NewBatchNode(NodeMeta{ID: "batch"}).
		SetPrep(func(ctx *RunContext) ([]any, error) {
			items := make([]any, 100)
			for i := range items {
				items[i] = i
			}
			return items, nil
		}).
		SetExecItem(func(ctx *RunContext, item any, index int) (any, error) {
			return item.(int) + 1, nil
		})
	ctx := NewRunContext(context.Background(), nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := node.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelBatchNode(b *testing.B) {
	node := NewParallelBatchNode(NodeMeta{ID: "parallel"}, 16).
		SetPrep(func(ctx *RunContext) ([]any, error) {
			items := make([]any, 100)
			for i := range items {
				items[i] = i
			}
			return items, nil
		}).
		SetExecItem(func(ctx *RunContext, item any, index int) (any, error) {
			return item.(int) + 1, nil
		})
	ctx := NewRunContext(context.Background(), nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := node.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSharedConcurrentAccess(b *testing.B) {
	shared := NewShared(nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			shared.Set("key", 1)
			shared.Get("key")
		}
	})
}

func BenchmarkEventSink(b *testing.B) {
	var mu sync.Mutex
	events := 0
	rc := NewRunContext(context.Background(), nil, nil)
	rc.Events = EventSinkFunc(func(event Event) {
		mu.Lock()
		events++
		mu.Unlock()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc.Emit(Event{Type: EventNodeStarted})
	}
}
