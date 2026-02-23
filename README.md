[![Go Reference](https://pkg.go.dev/badge/github.com/teenjuna/liq.svg)](https://pkg.go.dev/github.com/teenjuna/liq)

liq is a Go library that implements a generic, buffered, batched, and persistent queue.

Most of the queue's behavior is configurable:
- Type of an item
- How a batch of items is processed
- How a batch of items is stored in memory (including aggregation)
- When a batch of items is ready to be stored in a SQLite database
- How a batch of items is encoded to bytes and vice versa
- How many workers can process batches concurrently
- How many batches can be retrieved for processing by a single worker
- What retry policy do the workers follow when dealing with processing failures
- How a SQLite database is opened:
  - In memory (no persistence)
  - In a file (with different levels of durability)
- Optional Prometheus metrics

# Installation

```bash
go get github.com/teenjuna/liq@latest
```

# Examples

Sometimes it's easier to understand a thing by looking at some examples.

Let's imagine that we're implementing a system that receives many items that look like this:

```go
type Item struct {
    ID    int
    Count int
}
```

The goal of the system is to apply `Count` increments for each unique `ID`. For the sake of
simplicity, instead of actually applying the increments in, for example, a database, we'll just
output them.

## Simple

Let's start from a simple and minimal example. Here we create a `liq.Queue[Item]` without any
additional configuration. By default, the queue is managed fully in memory, meaning there's no
persistence. It flushes its buffer after each push, meaning that each batch that will be passed to
provided process function contains only one element. Each batch is internally encoded and decoded
using the built-in JSON codec, which is one of the several built-in codec implementations. If
process function were to fail, it will be infinitely retried with the default exponential backoff
retry policy.

```go
import (
	"context"
	"fmt"
	"iter"
	"os/signal"
	"slices"
	"syscall"

	"github.com/teenjuna/liq"
)

func main() {
    queue, err := liq.New(
        func(ctx context.Context, queue *liq.Queue[Item], batch iter.Seq[Item]) error {
            items := slices.Collect(batch)
            fmt.Println("Batch:", items)
            return nil
        },
    )
    if err != nil { /* ... */ }
    defer queue.Close()

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    _ = queue.Push(ctx, Item{ID: 1, Count: 1})
    _ = queue.Push(ctx, Item{ID: 2, Count: 1})
    _ = queue.Push(ctx, Item{ID: 1, Count: 1})
    _ = queue.Push(ctx, Item{ID: 3, Count: 1})

    <-ctx.Done()
}
```

Here is the output of the program:

```
Batch: [{1 1}]
Batch: [{2 1}]
Batch: [{1 1}]
Batch: [{3 1}]
^C⏎ 
```

Such a simple configuration is mostly useful for tests. Let's look at a more involved example next.

## Complex

This example is more interesting:
- The queue is now managed in file `items.liq`. Since the file is marked as `.Durable(true)`, the
SQLite will flush the data to disk after each transaction. 
- Instead of flushing data to SQLite storage after each push, we first buffer it in an in-memory
buffer. Instead of using the default `buffer.Appending`, which just appends each pushed item to the
end, we use `buffer.Merging`, which can merge items by their `ID` on each push. The buffer will
flush after any of the following conditions becomes true:
  - Buffer contains 100 elements
  - Buffer received 1000 pushes (different from the "contains" because of `buffer.Merging`
  behaviour)
  - Buffer contains at least one item and it hasn't flushed in a 1 second interval
- Instead of using slow-ish JSON codec, it uses another built-in implementation that utilizes
`encoding/gob` package
- Flushed batches are processed by two concurrent workers instead of one
- Each worker tries to process up to 10 batches at once (internally they're merged into one batch
using the `buffer.Merging` behaviour)
- Workers use the same exponential backoff retry policy, but with different configuration:
  - Up to 5 attempts (instead of infinite)
  - Every subsequent interval grows by 1.5x instead of 2x
  - Every interval has a random jitter of up to 20% instead of up to 10%
  - If no attempts left, the batch (or, more specifically, all the real batches it consists of) is
  returned back to the queue with a minute cooldown, meaning that the workers will skip them for a
  minute
- The queue sends its (slightly customized) Prometheus metrics with the provided registry

```go
import (
	"context"
	"fmt"
	"iter"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/teenjuna/liq"
	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec/gob"
	"github.com/teenjuna/liq/retry"
)

func main() {
    registry := prometheus.NewRegistry()

    queue, err := liq.New(
        func(ctx context.Context, queue *liq.Queue[Item], batch iter.Seq[Item]) error {
            items := slices.Collect(batch)
            fmt.Println("Batch:", items)
            return nil
        },
        func(c *liq.Config[Item]) {
            c.File(liq.File("items.liq").Durable(true))
            c.FlushSize(100)
            c.FlushPushes(1000)
            c.FlushTimeout(time.Second)
            c.Buffer(buffer.Merging(
                func(i Item) int {
                    return i.ID
                },
                func(i1, i2 Item) Item {
                    return Item{ID: i1.ID, Count: i1.Count + i2.Count}
                },
            ))
            c.Codec(gob.New[Item]())
            c.Workers(2)
            c.Batches(10)
            c.RetryPolicy(retry.
                Exponential(5, time.Second, time.Minute).
                WithBase(1.5).
                WithJitter(0.2).
                WithCooldown(time.Minute),
            )
            c.Prometheus(liq.Prometheus(registry, func(c *liq.PrometheusConfig) {
                c.ProcessDuration.Buckets = prometheus.ExponentialBuckets(10, 2, 5)
            }))
        },
    )
    if err != nil { /* ... */ }
    defer queue.Close()

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    _ = queue.Push(ctx, Item{ID: 1, Count: 1})
    _ = queue.Push(ctx, Item{ID: 2, Count: 1})
    _ = queue.Push(ctx, Item{ID: 1, Count: 1})
    _ = queue.Push(ctx, Item{ID: 3, Count: 1})

    <-ctx.Done()
}
```

And here is the output:

```
Batch: [{3 1} {1 2} {2 1}]
^C⏎ 
```

This example is a bit silly because some of the options are overkill for the 4 pushes we do, but
let's ignore that.

# Architecture

## Data flow

TODO: diagram

## Concepts 

### Process function

TODO

### Worker

TODO

### Buffer

TODO

### Codec

TODO

### Retry policy

TODO

## Metrics

TODO: description of available metrics
