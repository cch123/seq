# CLAUDE.md

## Project Overview

`github.com/cch123/seq` is a zero-dependency, pure-Go library providing per-key serialized execution with cross-key parallelism. Designed for trading/financial systems where concurrent operations on the same key (e.g., account) require strict ordering while operations on different keys run in parallel.

**Core Guarantee:** Same key → strict FIFO. Different keys → fully parallel.

## Build & Test

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run benchmarks
go test -run '^$' -bench . -benchmem ./...
```

This is a library (no binary). Build check:
```bash
go build ./...
```

## Project Structure

| File | Purpose |
|------|---------|
| `sequencer.go` | Core implementation |
| `sequencer_test.go` | Unit tests covering all semantics |
| `go_style_test.go` | Idiomatic Go usage examples |
| `sequencer_benchmark_test.go` | Performance benchmarks |
| `README.md` | Documentation (Chinese) |

## Key APIs

```go
// Create sequencer (K must be comparable)
s, err := sequencer.New[K](opts)

// Submit a task (non-blocking, returns handle)
handle, err := s.Submit(ctx, key, func(ctx context.Context) error { ... })

// Submit and wait (blocking)
err := s.Do(ctx, key, func(ctx context.Context) error { ... })

// Submit async task
handle, err := s.SubmitAsync(ctx, key, func(ctx context.Context) (<-chan error, error) { ... })

// Task handle operations
err := handle.Wait() // block until done
handle.Cancel()    // cancel if not yet started
handle.Started()   // check if running
handle.Done()      // channel closed on completion

// Shutdown
s.Close()
```

## Configuration

```go
sequencer.New[K](sequencer.Options[K]{
    MaxPendingPerKey: 100,   // queue limit per key (default: 1024)
    MaxPendingGlobal: 10000, // global queue limit (default: 1<<20)
    Observer: myObserver,    // optional lifecycle callbacks
})
```

## Important Semantics

- **Cancellation:** Unstarted tasks can be canceled. Already-started tasks run to completion (context detached via `context.WithoutCancel`). If the context is already cancelled at submission time, `Submit`/`Do`/`SubmitAsync` return a completed handle (with the context error) and a `nil` error — not an error return.
- **Panics:** Caught and wrapped in `PanicError` with stack trace; queue continues processing.
- **Backpressure:** Returns `ErrQueueFull` when limits exceeded.
- **Workers:** One goroutine per busy key, spawned lazily, cleaned up when queue drains.

## Module Info

- Module: `github.com/cch123/seq`
- Go version: 1.22
- Dependencies: none (standard library only)
