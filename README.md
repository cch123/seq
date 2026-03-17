# github.com/cch123/seq

`github.com/cch123/seq` 是一个 **进程内、按 key 串行、跨 key 并行** 的 Go 组件，适合放在同一个交易服务内，为 `OMS` 和 `Settlement` 共享的账户状态更新提供顺序化执行能力。

它参考了 Java `ExecutionSequencer` 的核心语义：

- 同一个 key 上，后一个任务必须等前一个任务真正完成后才能开始
- 不同 key 之间允许并行
- 已启动任务不会因为后继任务取消而被打断
- panic 会被转成错误并释放队列，避免后续任务饿死

这个组件只解决 **同进程内的执行定序**，**不提供持久化、重放、跨进程一致性**。如果请求不能因进程重启而丢失，仍然需要配合 inbox/outbox、数据库状态机或消息队列使用。

运行时模型上，它采用 **每个活跃 key 一个 worker goroutine** 的方式：

- 某个 key 第一次有任务进入时，会启动一个 worker goroutine
- 这个 worker 会按顺序串行执行该 key 的任务
- 当该 key 的队列清空后，worker 会自动退出
- 空闲 key 不会保留常驻 goroutine

## 适用场景

典型交易场景：

- 同一用户同一账户上的下单、撤单、冻结、解冻、结算要严格串行
- 不同用户或不同账户之间希望并行提高吞吐
- `OMS` 和 `Settlement` 在同一个服务内，但共享同一份可变状态

推荐的 key 设计：

```text
tenant_id:user_id:account_id
```

如果账户下还有更细粒度的独立子账本，也可以把子账本 ID 纳入 key，以换取更高并发。

## 保证语义

### 1. 顺序保证

同一个 key 上：

- 提交顺序 = 启动顺序
- 前一个任务完成前，后一个任务不会开始

不同 key 上：

- 没有顺序保证
- 可以同时运行

这个并行性在实现上体现为：**不同 key 的 worker goroutine 可以同时运行，同一个 key 始终只会有一个 worker 在跑。**

### 2. 取消语义

- 如果 `Submit` / `SubmitAsync` 调用时 `context` 已经结束，会直接返回该错误，请求不会入队
- 任务 **未启动** 时，可以被 `TaskHandle.Cancel()` 或排队阶段的 `context` 取消移除
- 任务 **已启动** 后，sequencer 不会强制打断
- 已启动任务会通过 `context.WithoutCancel(ctx)` 继续执行，这与 `ExecutionSequencer` “不打断已开始任务”的原则一致

### 3. 异步任务语义

`SubmitAsync` 接收一个返回 `<-chan error` 的任务：

- channel 返回一个 error：任务以该 error 结束
- channel 被直接关闭：任务视为成功结束
- 在这个 channel 完成之前，同 key 的后续任务不会开始

### 4. 错误与 panic

- 普通错误按原样返回
- panic 会被包装成 `*PanicError`
- 无论成功、失败还是 panic，sequencer 都会继续推进该 key 的后续任务

### 5. 背压

支持两个队列上限：

- `MaxPendingPerKey`
- `MaxPendingGlobal`

超过上限时直接返回 `ErrQueueFull`，不会无限制堆积内存。

### 6. Observer 语义

- `Observer` 回调是 best-effort 的
- 回调会异步执行，不阻塞提交或队列推进
- 回调 panic 会被恢复并丢弃，不影响 sequencer 正常调度
- 即便如此，也不应在 `Observer` 里做长时间阻塞或无界 goroutine 堆积

## API 概览

```go
type Task func(context.Context) error
type AsyncTask func(context.Context) (<-chan error, error)

type Options[K comparable] struct {
    MaxPendingPerKey int
    MaxPendingGlobal int64
    Observer         Observer[K]
}

type Sequencer[K comparable] struct { ... }

func New[K comparable](opts Options[K]) (*Sequencer[K], error)
func (s *Sequencer[K]) Submit(ctx context.Context, key K, task Task) (*TaskHandle, error)
func (s *Sequencer[K]) Do(ctx context.Context, key K, task Task) error
func (s *Sequencer[K]) SubmitAsync(ctx context.Context, key K, task AsyncTask) (*TaskHandle, error)
func (s *Sequencer[K]) PendingForKey(key K) int
func (s *Sequencer[K]) PendingGlobal() int64
func (s *Sequencer[K]) Close()

type TaskHandle struct { ... }

func (h *TaskHandle) Done() <-chan struct{}
func (h *TaskHandle) Wait() error
func (h *TaskHandle) Err() error
func (h *TaskHandle) Cancel() bool
func (h *TaskHandle) Started() bool
```

## 使用示例

### 同步任务

```go
package main

import (
	"context"
	"log"

	sequencer "github.com/cch123/seq"
)

func main() {
	seq, err := sequencer.New[string](sequencer.Options[string]{
		MaxPendingPerKey: 1024,
		MaxPendingGlobal: 100_000,
	})
	if err != nil {
		log.Fatal(err)
	}

	key := "tenant-1:user-42:cash"

	if err := seq.Do(context.Background(), key, func(ctx context.Context) error {
		// 这里串行执行业务逻辑，例如：
		// 1. 校验账户状态
		// 2. 更新订单状态
		// 3. 更新资金冻结
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
```

### 返回结果的 Go 风格写法

如果任务执行完需要返回结果，推荐直接用 **闭包捕获结果**：

```go
type PlaceOrderResult struct {
	OrderID string
	Version int64
}

var result PlaceOrderResult
err := seq.Do(ctx, key, func(ctx context.Context) error {
	var err error
	result, err = placeOrder(ctx)
	return err
})
if err != nil {
	return err
}

return useResult(result)
```

如果你不想当前 goroutine 阻塞，也可以用 `Submit` 然后在外面 `Wait`：

```go
var result PlaceOrderResult
handle, err := seq.Submit(ctx, key, func(ctx context.Context) error {
	var err error
	result, err = placeOrder(ctx)
	return err
})
if err != nil {
	return err
}

if err := handle.Wait(); err != nil {
	return err
}

return useResult(result)
```

### 更偏底层的异步任务

如果某段已有逻辑天然以 “完成 channel” 作为结束信号，可以用 `SubmitAsync`：

```go
replyCh := make(chan Reply, 1)

handle, err := seq.SubmitAsync(ctx, key, func(ctx context.Context) (<-chan error, error) {
	done := make(chan error, 1)

	go func() {
		reply, err := callRemoteService(ctx)
		replyCh <- reply
		close(replyCh)
		done <- err
		close(done)
	}()

	return done, nil
})
if err != nil {
	return err
}

reply := <-replyCh
if err := handle.Wait(); err != nil {
	return err
}

return useReply(reply)
```

大多数业务代码里，推荐优先使用 `Do` / `Submit` + 闭包捕获结果；`SubmitAsync` 更适合少量已经天然 channel 化的完成通知场景。

## 交易系统接入建议

### key 设计

默认建议：

```text
tenant_id:user_id:account_id
```

如果 `OMS` 和 `Settlement` 都会修改同一个账户的可变状态，它们必须共享同一个 key，不能各自维护独立队列。

### 幂等

sequencer 保证的是 **同进程执行顺序**，不是业务幂等。交易请求仍应有自己的：

- `request_id`
- 业务状态机
- 幂等校验

### 不要做的事

- 不要在同 key 任务内部同步等待自己追加的后继任务
- 不要把它当成可靠消息队列
- 不要让 `Observer` 做阻塞 I/O

## Benchmark

运行方式：

```bash
go test -run '^$' -bench . -benchmem ./...
```

当前基准覆盖：

- `BenchmarkSequencerSingleKeyRoundTrip`
- `BenchmarkSequencerSingleKeyQueued`
- `BenchmarkSequencerParallelKeys`
- `BenchmarkSequencerAsyncSingleKeyRoundTrip`

这些 benchmark 主要用来观察：

- 单 key 串行路径上的提交/调度开销
- 同 key 有排队时的额外开销
- 多 key 并发时的扩展能力
- `SubmitAsync` 相对同步路径的额外成本

基准结果会受 CPU、GOMAXPROCS、Go 版本和系统负载影响，应该只用于横向比较，不应当当作固定 SLA。
