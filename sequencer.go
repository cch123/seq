package sequencer

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

var (
	ErrClosed       = errors.New("sequencer is closed")
	ErrQueueFull    = errors.New("sequencer queue limit reached")
	ErrTaskCanceled = errors.New("task canceled before start")
)

// Task executes synchronously inside the sequencer. The next task for the same
// key starts only after this function returns.
type Task func(context.Context) error

// Observer receives best-effort lifecycle callbacks. Callbacks run
// asynchronously; panics are recovered and discarded.
type Observer[K comparable] interface {
	OnEnqueue(key K, pendingForKey int, pendingGlobal int64)
	OnStart(key K, queueDelay time.Duration)
	OnFinish(key K, runTime time.Duration, err error)
	OnReject(key K, err error)
}

// NopObserver ignores all events.
type NopObserver[K comparable] struct{}

func (NopObserver[K]) OnEnqueue(K, int, int64)          {}
func (NopObserver[K]) OnStart(K, time.Duration)         {}
func (NopObserver[K]) OnFinish(K, time.Duration, error) {}
func (NopObserver[K]) OnReject(K, error)                {}

// Options configures the sequencer.
type Options[K comparable] struct {
	// MaxPendingPerKey counts both the currently running task and queued tasks.
	MaxPendingPerKey int

	// MaxPendingGlobal counts both running and queued tasks across all keys.
	MaxPendingGlobal int64

	Observer Observer[K]
}

// Sequencer serializes tasks per key while allowing different keys to run in
// parallel. It is an in-process component and does not provide durability.
//
// Each active key owns one worker goroutine. The worker exits automatically when
// the key queue drains, so idle keys do not keep background goroutines alive.
type Sequencer[K comparable] struct {
	mu sync.Mutex

	queues map[K]*keyQueue[K]

	pendingGlobal    int64
	closed           bool
	maxPendingPerKey int
	maxPendingGlobal int64
	observer         Observer[K]
}

type keyQueue[K comparable] struct {
	key K

	running       *taskEntry[K]
	waiting       list.List
	workerRunning bool
}

func (q *keyQueue[K]) pending() int {
	pending := q.waiting.Len()
	if q.running != nil {
		pending++
	}
	return pending
}

func (q *keyQueue[K]) idle() bool {
	return q.running == nil && q.waiting.Len() == 0
}

func (q *keyQueue[K]) enqueue(entry *taskEntry[K]) {
	entry.node = q.waiting.PushBack(entry)
	entry.state = taskQueued
}

func (q *keyQueue[K]) claimNext() *taskEntry[K] {
	elem := q.waiting.Front()
	if elem == nil {
		return nil
	}

	q.waiting.Remove(elem)

	entry := elem.Value.(*taskEntry[K])
	entry.node = nil
	entry.state = taskClaimed
	q.running = entry
	return entry
}

func (q *keyQueue[K]) cancelQueued(entry *taskEntry[K]) bool {
	if entry.node == nil || entry.state != taskQueued {
		return false
	}

	q.waiting.Remove(entry.node)
	entry.node = nil
	entry.state = taskDone
	return true
}

func (q *keyQueue[K]) finishRunning(entry *taskEntry[K]) {
	if q.running == entry {
		q.running = nil
	}
	entry.state = taskDone
}

type taskState uint8

const (
	taskQueued taskState = iota
	taskClaimed
	taskRunning
	taskDone
)

type taskEntry[K comparable] struct {
	queue *keyQueue[K]
	key   K

	ctx  context.Context
	task Task

	handle *TaskHandle

	enqueuedAt time.Time

	cancelWatch func() bool
	node        *list.Element
	state       taskState
}

func newTaskEntry[K comparable](key K, ctx context.Context, task Task) *taskEntry[K] {
	return &taskEntry[K]{
		key:        key,
		ctx:        ctx,
		task:       task,
		handle:     newTaskHandle(),
		enqueuedAt: time.Now(),
	}
}

func (e *taskEntry[K]) startCancellationWatch(cancel func()) {
	if e.ctx.Done() == nil {
		return
	}
	e.cancelWatch = context.AfterFunc(e.ctx, cancel)
}

func (e *taskEntry[K]) stopCancellationWatch() {
	if e.cancelWatch == nil {
		return
	}
	e.cancelWatch()
	e.cancelWatch = nil
}

// TaskHandle tracks the lifecycle of one submitted task.
type TaskHandle struct {
	done chan struct{}

	once sync.Once
	mu   sync.Mutex

	err       error
	started   bool
	completed bool
	cancel    func() bool
}

// PanicError wraps a task panic so the sequencer can release the key and keep
// subsequent tasks moving.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("sequenced task panicked: %v", e.Value)
}

// New constructs a keyed sequencer with sane defaults.
func New[K comparable](opts Options[K]) (*Sequencer[K], error) {
	if opts.MaxPendingPerKey == 0 {
		opts.MaxPendingPerKey = 1024
	}
	if opts.MaxPendingGlobal == 0 {
		opts.MaxPendingGlobal = 1 << 20
	}
	if opts.MaxPendingPerKey <= 0 {
		return nil, fmt.Errorf("max pending per key must be positive: %d", opts.MaxPendingPerKey)
	}
	if opts.MaxPendingGlobal <= 0 {
		return nil, fmt.Errorf("max pending global must be positive: %d", opts.MaxPendingGlobal)
	}
	if opts.Observer == nil {
		opts.Observer = NopObserver[K]{}
	}

	return &Sequencer[K]{
		queues:           make(map[K]*keyQueue[K]),
		maxPendingPerKey: opts.MaxPendingPerKey,
		maxPendingGlobal: opts.MaxPendingGlobal,
		observer:         opts.Observer,
	}, nil
}

// Submit enqueues a synchronous task.
func (s *Sequencer[K]) Submit(ctx context.Context, key K, task Task) (*TaskHandle, error) {
	if task == nil {
		return nil, errors.New("task must not be nil")
	}
	return s.submit(ctx, key, task)
}

// Do is a convenience wrapper around Submit + Wait for the common synchronous
// call pattern.
func (s *Sequencer[K]) Do(ctx context.Context, key K, task Task) error {
	handle, err := s.Submit(ctx, key, task)
	if err != nil {
		return err
	}
	return handle.Wait()
}


// PendingForKey returns the number of running + queued tasks for a key.
func (s *Sequencer[K]) PendingForKey(key K) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue := s.queues[key]
	if queue == nil {
		return 0
	}
	return queue.pending()
}

// PendingGlobal returns the number of running + queued tasks across all keys.
func (s *Sequencer[K]) PendingGlobal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingGlobal
}

// Close rejects new submissions and lets accepted tasks drain naturally.
func (s *Sequencer[K]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *Sequencer[K]) submit(ctx context.Context, key K, task Task) (*TaskHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entry := newTaskEntry(key, ctx, task)
	entry.handle.cancel = func() bool {
		return s.cancelQueued(entry, ErrTaskCanceled)
	}

	var (
		queue         *keyQueue[K]
		startWorker   bool
		pendingForKey int
		pendingGlobal int64
		rejected      error
	)

	s.mu.Lock()
	switch {
	case s.closed:
		rejected = ErrClosed
	case s.pendingGlobal >= s.maxPendingGlobal:
		rejected = fmt.Errorf("%w: global=%d", ErrQueueFull, s.pendingGlobal)
	default:
		queue = s.queueForKeyLocked(key)
		if queue.pending() >= s.maxPendingPerKey {
			rejected = fmt.Errorf("%w: key=%v pending=%d", ErrQueueFull, key, queue.pending())
			break
		}

		entry.queue = queue
		queue.enqueue(entry)
		entry.startCancellationWatch(func() {
			s.cancelQueued(entry, ctx.Err())
		})

		s.pendingGlobal++
		pendingForKey = queue.pending()
		pendingGlobal = s.pendingGlobal
		if !queue.workerRunning {
			queue.workerRunning = true
			startWorker = true
		}
	}
	s.mu.Unlock()

	if rejected != nil {
		entry.stopCancellationWatch()
		s.observe(func(observer Observer[K]) {
			observer.OnReject(key, rejected)
		})
		return nil, rejected
	}

	s.observe(func(observer Observer[K]) {
		observer.OnEnqueue(key, pendingForKey, pendingGlobal)
	})
	if startWorker {
		go s.runWorker(queue)
	}
	return entry.handle, nil
}

func (s *Sequencer[K]) queueForKeyLocked(key K) *keyQueue[K] {
	queue := s.queues[key]
	if queue == nil {
		queue = &keyQueue[K]{key: key}
		s.queues[key] = queue
	}
	return queue
}

type runDecision[K comparable] struct {
	entry       *taskEntry[K]
	queueDelay  time.Duration
	terminalErr error
}

func (s *Sequencer[K]) runWorker(queue *keyQueue[K]) {
	for {
		next := s.takeNext(queue)
		if next.entry == nil {
			return
		}

		if next.terminalErr != nil {
			next.entry.stopCancellationWatch()
			next.entry.handle.complete(next.terminalErr)
			continue
		}

		next.entry.stopCancellationWatch()
		next.entry.handle.markStarted()
		s.observe(func(observer Observer[K]) {
			observer.OnStart(next.entry.key, next.queueDelay)
		})

		startedAt := time.Now()
		err := invoke(next.entry.task, context.WithoutCancel(next.entry.ctx))

		s.finishRun(queue, next.entry)
		next.entry.handle.complete(err)
		runTime := time.Since(startedAt)
		s.observe(func(observer Observer[K]) {
			observer.OnFinish(next.entry.key, runTime, err)
		})
	}
}

func (s *Sequencer[K]) observe(callback func(Observer[K])) {
	go func(observer Observer[K]) {
		defer func() {
			_ = recover()
		}()
		callback(observer)
	}(s.observer)
}

func (s *Sequencer[K]) takeNext(queue *keyQueue[K]) runDecision[K] {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := queue.claimNext()
	if entry == nil {
		s.retireWorkerLocked(queue)
		return runDecision[K]{}
	}

	if err := entry.ctx.Err(); err != nil {
		queue.finishRunning(entry)
		s.pendingGlobal--
		return runDecision[K]{
			entry:       entry,
			terminalErr: err,
		}
	}

	entry.state = taskRunning
	return runDecision[K]{
		entry:      entry,
		queueDelay: time.Since(entry.enqueuedAt),
	}
}

func (s *Sequencer[K]) retireWorkerLocked(queue *keyQueue[K]) {
	queue.workerRunning = false
	if queue.idle() && s.queues[queue.key] == queue {
		delete(s.queues, queue.key)
	}
}

func (s *Sequencer[K]) finishRun(queue *keyQueue[K], entry *taskEntry[K]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.state == taskDone {
		return
	}

	queue.finishRunning(entry)
	s.pendingGlobal--
}

func (s *Sequencer[K]) cancelQueued(entry *taskEntry[K], reason error) bool {
	s.mu.Lock()
	ok := s.cancelQueuedLocked(entry)
	s.mu.Unlock()

	if !ok {
		return false
	}

	entry.stopCancellationWatch()
	entry.handle.complete(reason)
	return true
}

func (s *Sequencer[K]) cancelQueuedLocked(entry *taskEntry[K]) bool {
	if entry.queue == nil || entry.state != taskQueued {
		return false
	}
	if !entry.queue.cancelQueued(entry) {
		return false
	}

	s.pendingGlobal--
	return true
}

func invoke(task Task, ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{
				Value: recovered,
				Stack: debug.Stack(),
			}
		}
	}()
	return task(ctx)
}

func newTaskHandle() *TaskHandle {
	return &TaskHandle{done: make(chan struct{})}
}

// Done closes when the task reaches a terminal state.
func (h *TaskHandle) Done() <-chan struct{} {
	return h.done
}

// Wait blocks until the task finishes and returns the terminal error.
func (h *TaskHandle) Wait() error {
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// Err returns the terminal error if the task is already done, otherwise nil.
func (h *TaskHandle) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.completed {
		return nil
	}
	return h.err
}

// Cancel removes the task if it has not started yet.
func (h *TaskHandle) Cancel() bool {
	if h.cancel == nil {
		return false
	}
	return h.cancel()
}

// Started reports whether the task has started running.
func (h *TaskHandle) Started() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.started
}

func (h *TaskHandle) markStarted() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.started = true
}

func (h *TaskHandle) complete(err error) {
	h.once.Do(func() {
		h.mu.Lock()
		h.err = err
		h.completed = true
		h.mu.Unlock()

		close(h.done)
	})
}
