package liq

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec/json"
	"github.com/teenjuna/liq/internal"
	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/retry"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrClosed is the error that is returned by Queue methods when it's already closed.
	ErrClosed = errors.New("queue is closed")
)

// ProcessFunc is a function that is used by a [Queue] to process its data.
//
// If non-nil error is returned, the operation will be retried based on the [Config.RetryPolicy].
// If there are no attempts remaining, the data will be returned back into the queue.
//
// In most cases, the upper limit of items in the batch is equal to [Config.FlushSize] ×
// [Config.Batches]. Some batches may be larger if they were flushed by [Queue.Flush] or
// [Queue.Close].
//
// Provided context will be cancelled if [Queue.Close] is called.
//
// Provided queue may be used to push elements back into the queue. This will work even when the
// queue is closing if you pass the provided context into the [Queue.Push] method.
//
//	Important
//
//	Don't ever push elements back into the queue if your function can return an error. This will
//	lead to data duplication.
type ProcessFunc[Item any] = func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error

// Queue is a buffered, batched, and persistent queue for items of type Item.
//
// Each opened queue expects to eventually be closed using the [Queue.Close]. Otherwise some data
// may be lost.
//
// Items are pushed into the queue using the [Queue.Push]. Pushed items are stored in an in-memory
// buffer (configured by [Config.Buffer]) until either a size (configured by [Config.FlushSize]) or
// a time (configured by [Config.FlushTimeout]) limit is reached or [Queue.Flush] is called. When
// that happens, the buffer is encoded into bytes (configured by [Config.Codec]) and stored in a
// SQLite database (configured by [Config.File]).
//
// Each stored batch, starting from the oldest, is then retrieved by a background worker, which
// will decode it back into a typed buffer and pass it into the queue's [ProcessFunc]. If the
// processing is finished without an error, the batch will be deleted and the worker will retrieve
// the next batch. Otherwise the worker tries again, according to the queue's [retry.Policy]
// (configured by [Config.RetryPolicy]). If there are no attempts remaining, the batch will stay in
// the SQLite database with the same priority, though it may receive a cooldown period if
// [retry.Policy] configured it.
//
// The background worker may actually retrieve more than one batch at the same time. The upper
// limit of the number of batches is configured by [Config.Batches]. When that happens, the items
// from all batches will go through workers' own instance of a buffer configured by
// [Config.Buffer]. This means that if the buffer implements some kind of aggregation (like
// [buffer.Merging]), this aggregation will be applied to all the items of the retrieved batches
// before they're passed to the [ProcessFunc] as a single batch. If the processing succeeds, all
// the original batches will be deleted and the worker will retrieve the next set of batches. If
// the processing fails and there are no attempts remaining, all the original batches will stay in
// the SQLite database, just like with the one batch.
//
// More than one background worker can process batches. The number of workers is configured by
// [Config.Workers]. Two workers will never retrieve the same batch.
type Queue[Item any] struct {
	id      string
	cfg     *Config[Item]
	storage *sqlite.Storage

	push    chan Item
	flush   chan chan flushResult
	flushed chan struct{}
	closing *atomic.Bool

	pushCtx   context.Context
	pushStop  func()
	pushGroup *errgroup.Group

	processCtx  context.Context
	processStop func()
	// https://github.com/golang/go/issues/77249
	// processFunc ProcessFunc[Item]
	processFunc  func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error
	processGroup *errgroup.Group
}

// New creates a [Queue] with the provided [ProcessFunc] and a default [Config], which may be
// changed with the provided list of [ConfigFunc].
//
// By default, the queue:
//   - Doesn't use a file, the SQLite is in-memory (e.g. no persistence)
//   - Flushes buffer on each push (e.g. has single-item buffers)
//   - Uses [buffer.Appending] as a buffer
//   - Uses [json.Codec] as a codec
//   - Has only one processing worker which retrieves one batch
//   - Uses [retry.Exponential] as a retry policy, with infinite attempts and interval up to an
//     hour.
func New[Item any](
	processFunc ProcessFunc[Item],
	configFuncs ...ConfigFunc[Item],
) (
	*Queue[Item],
	error,
) {
	cfg := &Config[Item]{}
	cfg.File(nil)
	cfg.FlushSize(1)
	cfg.Batches(1)
	cfg.Workers(1)
	cfg.Codec(json.New[Item]())
	cfg.Buffer(buffer.Appending[Item]())
	cfg.RetryPolicy(retry.Exponential(0, time.Second, time.Hour))
	cfg.Prometheus(nil)
	if !testing.Testing() {
		cfg.InternalErrorHandler(func(err error) {
			slog.Error("Internal error", slog.String("message", err.Error()))
		})
		cfg.ProcessErrorHandler(func(err error) {
			slog.Error("Process error", slog.String("message", err.Error()))
		})
	}
	for _, cf := range configFuncs {
		if cf != nil {
			cf(cfg)
		}
	}

	storage, err := sqlite.New(func(c *sqlite.Config) {
		c.URI(cfg.file.uri())
		c.Batches(cfg.batches)
		c.Workers(cfg.workers + 1)
		c.Cooldown(cfg.retryPolicy.Derive().Cooldown())
	})
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	var (
		push    = make(chan Item, cfg.flushSize)
		flush   = make(chan chan flushResult)
		flushed = make(chan struct{}, cfg.workers)
		closing = new(atomic.Bool)

		pushCtx_, pushStop = context.WithCancel(context.Background())
		pushGroup, pushCtx = errgroup.WithContext(pushCtx_)

		processCtx_, processStop = context.WithCancel(context.Background())
		processGroup, processCtx = errgroup.WithContext(processCtx_)
	)

	stats, err := storage.Stats()
	if err != nil {
		processStop()
		pushStop()
		return nil, fmt.Errorf("get stats from sqlite: %w", err)
	}

	cfg.metrics.batches.Set(float64(stats.Batches))
	cfg.metrics.items.Add(float64(stats.Items))

	queue := Queue[Item]{
		id:      internal.GenerateID(),
		cfg:     cfg,
		storage: storage,

		push:    push,
		flush:   flush,
		flushed: flushed,
		closing: closing,

		pushCtx:   pushCtx,
		pushStop:  pushStop,
		pushGroup: pushGroup,

		processCtx:   processCtx,
		processStop:  processStop,
		processFunc:  processFunc,
		processGroup: processGroup,
	}

	queue.workers()

	return &queue, nil
}

// Push pushes the item into the queue's in-memory buffer.
//
// It may return an error if provided context cancels before the item is pushed. It will also
// return the [ErrClosed] if the queue is closed (except in cases when the method is called from
// the [ProcessFunc] while the queue is still closing).
func (q *Queue[Item]) Push(ctx context.Context, item Item) error {
	if q.closing.Load() {
		if !q.isProcessContext(ctx) {
			return ErrClosed
		}
		ctx = context.WithoutCancel(ctx)
	}

	select {
	case q.push <- item:
		q.cfg.metrics.itemsPushed.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush flushes the queue's in-memory buffer.
//
// It may return an error if the provided context cancels before the flush is complete. It will
// return the [ErrClosed] if the queue is closed.
func (q *Queue[Item]) Flush(ctx context.Context) error {
	resCh := make(chan flushResult, 1)
	q.flush <- resCh

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resCh:
		return res.err
	}
}

// Close closes the queue.
//
// It tries to do this gracefully by following these steps:
//  1. Prevent new items from being pushed (except the items pushed from the [ProcessFunc])
//  2. Stop the process workers by cancelling their context, which is passed to the [ProcessFunc]
//  3. Stop the push worker (the one that passes items into the in-memory buffer)
//  4. Close the underlying SQLite database
//
// It will return the [ErrClosed] on the subsequent calls.
func (q *Queue[Item]) Close() error {
	// Signal to push worker that it must stop receiving external items.
	if q.closing.Swap(true) {
		return ErrClosed
	}

	errs := make([]error, 0)

	// Signal to process workers that they must stop.
	q.processStop()
	if err := q.processGroup.Wait(); err != nil {
		errs = append(errs, fmt.Errorf("process workers: %w", err))
	}

	// Signal to push worker that it must stop.
	q.pushStop()
	if err := q.pushGroup.Wait(); err != nil {
		errs = append(errs, fmt.Errorf("push worker: %w", err))
	}

	// Close the SQLite database.
	if err := q.storage.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close sqlite: %w", err))
	}

	return errors.Join(errs...)
}

func (q *Queue[Item]) workers() {
	q.pushGroup.Go(q.pushWorker)
	for range q.cfg.workers {
		q.processGroup.Go(q.processWorker)
	}
	go func() {
		select {
		case <-q.pushCtx.Done():
		case <-q.processCtx.Done():
		}
		if q.closing.Load() {
			return
		}
		var errs []error
		if err := q.pushCtx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("push worker: %w", err))
		}
		if err := q.processCtx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("process worker: %w", err))
		}
		if err := q.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close: %w", err))
		}
		if handler := q.cfg.internalErrorHandler; handler != nil {
			handler(errors.Join(errs...))
		}
	}()
}

func (q *Queue[Item]) pushWorker() error {
	var (
		buffer  = q.cfg.buffer.Derive()
		codec   = q.cfg.codec.Derive()
		timeout = ticker(q.cfg.flushTimeout)
	)
	collect := func() {
		for buffer.Size() < q.cfg.flushSize {
			select {
			case item := <-q.push:
				buffer.Push(item)
			default:
				return
			}
		}
	}
	for {
		var flushCh chan flushResult
		select {
		case <-q.pushCtx.Done():
			collect()
			if buffer.Size() == 0 {
				return nil
			}
			q.cfg.metrics.itemsFlushed.WithLabelValues("context").Add(float64(buffer.Size()))

		case flushCh = <-q.flush:
			collect()
			if buffer.Size() == 0 {
				notify(flushCh, flushResult{})
				continue
			}
			q.cfg.metrics.itemsFlushed.WithLabelValues("manual").Add(float64(buffer.Size()))

		case <-timeout:
			if buffer.Size() == 0 {
				continue
			}
			q.cfg.metrics.itemsFlushed.WithLabelValues("timeout").Add(float64(buffer.Size()))

		case item := <-q.push:
			buffer.Push(item)
			if buffer.Size() < q.cfg.flushSize {
				continue
			}
			q.cfg.metrics.itemsFlushed.WithLabelValues("size").Add(float64(buffer.Size()))
		}

		data, err := codec.Encode(buffer.Iter())
		if err != nil {
			err = fmt.Errorf("encode items: %w", err)
			notify(flushCh, flushResult{err: err})
			return err
		}

		id, err := q.storage.Push(data, buffer.Size())
		if err != nil {
			err = fmt.Errorf("push items storage: %w", err)
			notify(flushCh, flushResult{err: err})
			return err
		}

		q.cfg.metrics.batches.Add(1)
		q.cfg.metrics.items.Add(float64(buffer.Size()))

		notify(flushCh, flushResult{id: id})
		notify(q.flushed, struct{}{})

		buffer.Reset()
	}
}

func (q *Queue[Item]) processWorker() error {
	var (
		buffer   = q.cfg.buffer.Derive()
		codec    = q.cfg.codec.Derive()
		cooldown = timer(0)
		wait     = false
	)

	for {
		if wait {
			select {
			case <-q.processCtx.Done():
				return nil
			case <-q.flushed:
			case <-cooldown:
			}
		}

		batches, err := q.storage.Claim()
		if err != nil {
			return fmt.Errorf("claim batches: %w", err)
		}
		if len(batches) == 0 {
			// This can happen if 2 or more workers have seen that q.batches.Load() != 0 and didn't
			// wait for the q.push notification. In this case, they race to claim batches. Since
			// claim is atomic, if there are not enough batches, some workers can receive nothing.

			stat, err := q.storage.Stats()
			if err != nil {
				return fmt.Errorf("get stats: %w", err)
			}

			cooldown = timer(time.Until(stat.NextCooldownEnd))
			wait = true
			continue
		}

		var items int
		for _, batch := range batches {
			items += batch.Size
			if err := codec.Decode(batch.Data, buffer.Push); err != nil {
				return fmt.Errorf("decode batch: %w", err)
			}
		}

		var (
			ok    = false
			ctx   = q.markProcessContext(q.processCtx)
			retry = q.cfg.retryPolicy.Derive()
		)
		for {
			if !retry.Attempt(ctx) {
				break
			}
			start := time.Now()
			if err := q.processFunc(ctx, q, buffer.Iter()); err == nil {
				q.cfg.metrics.itemsProcessed.Add(float64(buffer.Size()))
				q.cfg.metrics.processDuration.Observe(float64(time.Since(start).Milliseconds()))
				ok = true
				break
			} else {
				q.cfg.metrics.processErrors.Inc()
				q.cfg.metrics.processDuration.Observe(float64(time.Since(start).Milliseconds()))
				if handler := q.cfg.processErrorHandler; handler != nil {
					handler(err)
				}
			}
		}

		batchIDs := make([]sqlite.BatchID, len(batches))
		for i, batch := range batches {
			batchIDs[i] = batch.ID
		}

		if ok {
			if err := q.storage.Delete(batchIDs...); err != nil {
				err = fmt.Errorf("delete batches: %w", err)
				return err
			}
			q.cfg.metrics.batches.Sub(float64(len(batches)))
			q.cfg.metrics.items.Sub(float64(items))
		} else {
			if err := q.storage.Release(batchIDs...); err != nil {
				err = fmt.Errorf("release batches: %w", err)
				return err
			}
			notify(q.flushed, struct{}{})
		}

		buffer.Reset()
	}
}

func (q *Queue[Item]) markProcessContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, processCtxMarker{}, q.id)
}

func (q *Queue[Item]) isProcessContext(ctx context.Context) bool {
	id, ok := ctx.Value(processCtxMarker{}).(string)
	return ok && id == q.id
}

func notify[T any](ch chan T, v T) {
	if ch != nil {
		select {
		case ch <- v:
		default:
		}
	}
}

func ticker(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return make(<-chan time.Time)
	}
	return time.Tick(d)
}

func timer(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return make(<-chan time.Time)
	}
	return time.After(d)
}

type processCtxMarker struct{}

type flushResult struct {
	id  sqlite.BatchID
	err error
}
