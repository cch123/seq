package sequencer

import (
	"context"
	"sync/atomic"
	"testing"
)

func BenchmarkSequencerSingleKeyRoundTrip(b *testing.B) {
	seq := mustNewBenchmarkSequencer[string](b)
	ctx := context.Background()
	key := "acct-1"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handle, err := seq.Submit(ctx, key, noopTask)
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
		if err := handle.Wait(); err != nil {
			b.Fatalf("wait: %v", err)
		}
	}
}

func BenchmarkSequencerSingleKeyQueued(b *testing.B) {
	seq := mustNewBenchmarkSequencer[string](b)
	ctx := context.Background()
	key := "acct-1"
	handles := make([]*TaskHandle, 0, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for submitted := 0; submitted < b.N; {
		handles = handles[:0]
		batch := minInt(256, b.N-submitted)

		for i := 0; i < batch; i++ {
			handle, err := seq.Submit(ctx, key, noopTask)
			if err != nil {
				b.Fatalf("submit: %v", err)
			}
			handles = append(handles, handle)
			submitted++
		}

		for _, handle := range handles {
			if err := handle.Wait(); err != nil {
				b.Fatalf("wait: %v", err)
			}
		}
	}
}

func BenchmarkSequencerParallelKeys(b *testing.B) {
	seq := mustNewBenchmarkSequencer[int](b)
	ctx := context.Background()

	const keyCount = 128
	var cursor atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := int(cursor.Add(1) % keyCount)

			handle, err := seq.Submit(ctx, key, noopTask)
			if err != nil {
				panic(err)
			}
			if err := handle.Wait(); err != nil {
				panic(err)
			}
		}
	})
}

func mustNewBenchmarkSequencer[K comparable](b *testing.B) *Sequencer[K] {
	b.Helper()

	seq, err := New[K](Options[K]{
		MaxPendingPerKey: 1 << 20,
		MaxPendingGlobal: 1 << 22,
	})
	if err != nil {
		b.Fatalf("new sequencer: %v", err)
	}
	return seq
}

func noopTask(context.Context) error {
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
