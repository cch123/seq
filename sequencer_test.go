package sequencer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameKeyRunsSequentially(t *testing.T) {
	seq := mustNewSequencer(t)

	firstGate := make(chan struct{})
	secondGate := make(chan struct{})
	started := make(chan int, 3)

	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 1
		<-firstGate
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 2
		<-secondGate
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}
	h3, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 3
		return nil
	})
	if err != nil {
		t.Fatalf("submit h3: %v", err)
	}

	assertReceiveInt(t, started, 1)
	assertNoReceiveInt(t, started, 120*time.Millisecond)

	close(firstGate)
	assertReceiveInt(t, started, 2)
	assertNoReceiveInt(t, started, 120*time.Millisecond)

	close(secondGate)
	assertReceiveInt(t, started, 3)

	waitAll(t, h1, h2, h3)
}

func TestDifferentKeysRunInParallel(t *testing.T) {
	seq := mustNewSequencer(t)

	blockA := make(chan struct{})
	startedA := make(chan struct{}, 1)
	startedB := make(chan struct{}, 1)

	hA, err := seq.Submit(context.Background(), "user-a", func(context.Context) error {
		startedA <- struct{}{}
		<-blockA
		return nil
	})
	if err != nil {
		t.Fatalf("submit hA: %v", err)
	}
	hB, err := seq.Submit(context.Background(), "user-b", func(context.Context) error {
		startedB <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("submit hB: %v", err)
	}

	assertReceiveStruct(t, startedA)
	assertReceiveStruct(t, startedB)

	close(blockA)
	waitAll(t, hA, hB)
}

func TestAsyncTaskBlocksNextUntilCompletion(t *testing.T) {
	seq := mustNewSequencer(t)

	asyncDone := make(chan error, 1)
	startedNext := make(chan struct{}, 1)

	h1, err := seq.SubmitAsync(context.Background(), "u-1", func(context.Context) (<-chan error, error) {
		return asyncDone, nil
	})
	if err != nil {
		t.Fatalf("submit async: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		startedNext <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("submit next: %v", err)
	}

	assertNoReceiveStruct(t, startedNext, 120*time.Millisecond)

	asyncDone <- nil
	close(asyncDone)

	assertReceiveStruct(t, startedNext)
	waitAll(t, h1, h2)
}

func TestCancelBeforeStartSkipsTask(t *testing.T) {
	seq := mustNewSequencer(t)

	firstGate := make(chan struct{})
	startedSecond := make(chan struct{}, 1)

	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		<-firstGate
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		startedSecond <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	if !h2.Cancel() {
		t.Fatalf("expected h2 cancel to succeed")
	}
	if err := h2.Wait(); !errors.Is(err, ErrTaskCanceled) {
		t.Fatalf("expected ErrTaskCanceled, got %v", err)
	}

	close(firstGate)
	waitAll(t, h1)
	assertNoReceiveStruct(t, startedSecond, 120*time.Millisecond)
}

func TestContextCancellationBeforeStartSkipsTask(t *testing.T) {
	seq := mustNewSequencer(t)

	firstGate := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		<-firstGate
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(ctx, "u-1", func(context.Context) error {
		t.Fatal("canceled task should not run")
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	cancel()
	if err := h2.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	close(firstGate)
	waitAll(t, h1)
}

func TestStartedTaskIgnoresLaterContextCancellation(t *testing.T) {
	seq := mustNewSequencer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	nextStarted := make(chan struct{}, 1)
	runCtxErr := make(chan error, 1)

	h1, err := seq.Submit(ctx, "u-1", func(runCtx context.Context) error {
		started <- struct{}{}
		<-release
		runCtxErr <- runCtx.Err()
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		nextStarted <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	assertReceiveStruct(t, started)
	cancel()
	assertNoReceiveStruct(t, nextStarted, 120*time.Millisecond)

	close(release)
	waitAll(t, h1)

	if err := <-runCtxErr; err != nil {
		t.Fatalf("started task should run with detached context, got %v", err)
	}

	assertReceiveStruct(t, nextStarted)
	waitAll(t, h2)
}

func TestPanicBecomesErrorAndReleasesQueue(t *testing.T) {
	seq := mustNewSequencer(t)

	nextStarted := make(chan struct{}, 1)

	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		nextStarted <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	var panicErr *PanicError
	if err := h1.Wait(); !errors.As(err, &panicErr) {
		t.Fatalf("expected PanicError, got %v", err)
	}

	assertReceiveStruct(t, nextStarted)
	waitAll(t, h2)
}

func TestCloseRejectsNewSubmissions(t *testing.T) {
	seq := mustNewSequencer(t)
	seq.Close()

	_, err := seq.Submit(context.Background(), "u-1", func(context.Context) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestQueueLimitsRejectNewTasks(t *testing.T) {
	seq, err := New[string](Options[string]{
		MaxPendingPerKey: 1,
		MaxPendingGlobal: 2,
	})
	if err != nil {
		t.Fatalf("new sequencer: %v", err)
	}

	block := make(chan struct{})
	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}

	if _, err := seq.Submit(context.Background(), "u-1", func(context.Context) error { return nil }); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected per-key ErrQueueFull, got %v", err)
	}

	h2, err := seq.Submit(context.Background(), "u-2", func(context.Context) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	if _, err := seq.Submit(context.Background(), "u-3", func(context.Context) error { return nil }); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected global ErrQueueFull, got %v", err)
	}

	close(block)
	waitAll(t, h1, h2)
}

func TestAlreadyCanceledContextIsRejected(t *testing.T) {
	seq := mustNewSequencer(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, err := seq.Submit(ctx, "u-1", func(context.Context) error {
		t.Fatal("task should not run")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if h != nil {
		t.Fatalf("expected nil handle for rejected task")
	}
	if got := seq.PendingGlobal(); got != 0 {
		t.Fatalf("expected no pending tasks, got %d", got)
	}
}

func TestPendingCountsDropAfterDrain(t *testing.T) {
	seq := mustNewSequencer(t)

	h, err := seq.Submit(context.Background(), "u-1", func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("submit h: %v", err)
	}
	waitAll(t, h)

	if got := seq.PendingForKey("u-1"); got != 0 {
		t.Fatalf("expected no pending tasks for key, got %d", got)
	}
	if got := seq.PendingGlobal(); got != 0 {
		t.Fatalf("expected no global pending tasks, got %d", got)
	}
}

func TestObserverPanicsDoNotBreakSequencer(t *testing.T) {
	seq, err := New[string](Options[string]{
		MaxPendingPerKey: 16,
		MaxPendingGlobal: 64,
		Observer:         panicObserver[string]{},
	})
	if err != nil {
		t.Fatalf("new sequencer: %v", err)
	}

	started := make(chan int, 2)
	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 1
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 2
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	waitAll(t, h1, h2)
	assertReceiveInt(t, started, 1)
	assertReceiveInt(t, started, 2)

	seq.Close()
	if _, err := seq.Submit(context.Background(), "u-1", func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestObserverBlockingDoesNotBlockSequencer(t *testing.T) {
	observer := &blockingObserver[string]{release: make(chan struct{})}
	seq, err := New[string](Options[string]{
		MaxPendingPerKey: 16,
		MaxPendingGlobal: 64,
		Observer:         observer,
	})
	if err != nil {
		t.Fatalf("new sequencer: %v", err)
	}

	started := make(chan int, 2)
	h1, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 1
		return nil
	})
	if err != nil {
		t.Fatalf("submit h1: %v", err)
	}
	h2, err := seq.Submit(context.Background(), "u-1", func(context.Context) error {
		started <- 2
		return nil
	})
	if err != nil {
		t.Fatalf("submit h2: %v", err)
	}

	waitAll(t, h1, h2)
	assertReceiveInt(t, started, 1)
	assertReceiveInt(t, started, 2)

	close(observer.release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if observer.calls.Load() >= 4 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected observer callbacks to run asynchronously")
}

type panicObserver[K comparable] struct{}

func (panicObserver[K]) OnEnqueue(K, int, int64)          { panic("enqueue") }
func (panicObserver[K]) OnStart(K, time.Duration)         { panic("start") }
func (panicObserver[K]) OnFinish(K, time.Duration, error) { panic("finish") }
func (panicObserver[K]) OnReject(K, error)                { panic("reject") }

type blockingObserver[K comparable] struct {
	release chan struct{}
	calls   atomic.Int64
}

func (o *blockingObserver[K]) OnEnqueue(K, int, int64)          { o.block() }
func (o *blockingObserver[K]) OnStart(K, time.Duration)         { o.block() }
func (o *blockingObserver[K]) OnFinish(K, time.Duration, error) { o.block() }
func (o *blockingObserver[K]) OnReject(K, error)                { o.block() }

func (o *blockingObserver[K]) block() {
	o.calls.Add(1)
	<-o.release
}

func mustNewSequencer(t *testing.T) *Sequencer[string] {
	t.Helper()

	seq, err := New[string](Options[string]{
		MaxPendingPerKey: 16,
		MaxPendingGlobal: 64,
	})
	if err != nil {
		t.Fatalf("new sequencer: %v", err)
	}
	return seq
}

func waitAll(t *testing.T, handles ...*TaskHandle) {
	t.Helper()
	for _, handle := range handles {
		if err := handle.Wait(); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}
}

func assertReceiveInt(t *testing.T, ch <-chan int, want int) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %d", want)
	}
}

func assertReceiveStruct(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for signal")
	}
}

func assertNoReceiveInt(t *testing.T, ch <-chan int, wait time.Duration) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("unexpected receive: %d", got)
	case <-time.After(wait):
	}
}

func assertNoReceiveStruct(t *testing.T, ch <-chan struct{}, wait time.Duration) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected signal")
	case <-time.After(wait):
	}
}
