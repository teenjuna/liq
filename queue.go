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
	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/retry"
	"golang.org/x/sync/errgroup"
)

var (
	ErrClosed = errors.New("queue is closed")
)

type Queue[Item any] struct {
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

type ProcessFunc[Item any] = func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error

type ConfigFunc[Item any] = func(c *Config[Item])

func New[Item any](
	processFunc ProcessFunc[Item],
	configFuncs ...ConfigFunc[Item],
) (
	*Queue[Item],
	error,
) {
	cfg := &Config[Item]{}
	cfg.File(":memory:")
	cfg.FlushSize(1)
	cfg.Batches(1)
	cfg.Workers(1)
	cfg.Codec(func() Codec[Item] { return json.New[Item]() })
	cfg.Buffer(func() Buffer[Item] { return buffer.Appending[Item]() })
	cfg.RetryPolicy(func() RetryPolicy { return retry.Exponential(0, time.Second, time.Hour) })
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

	storage, err := sqlite.New(
		sqlite.WithFile(cfg.file),
		sqlite.WithBatches(cfg.batches),
		sqlite.WithWorkers(cfg.workers+1),
		sqlite.WithCooldown(cfg.retryPolicy().Cooldown()),
	)
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

func (q *Queue[Item]) Push(ctx context.Context, item Item) error {
	if q.closing.Load() {
		if !isProcessContext(ctx) {
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
		buffer  = q.cfg.buffer()
		codec   = q.cfg.codec()
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
		buffer   = q.cfg.buffer()
		codec    = q.cfg.codec()
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
			ctx   = markProcessContext(q.processCtx)
			retry = q.cfg.retryPolicy()
		)
		for {
			if !retry.Attempt(ctx) {
				break
			}
			start := time.Now()
			q.cfg.metrics.processAttempts.Inc()
			if err := q.processFunc(ctx, q, buffer.Iter()); err != nil {
				q.cfg.metrics.processErrors.Inc()
				q.cfg.metrics.processDuration.Observe(float64(time.Since(start).Milliseconds()))
				if handler := q.cfg.processErrorHandler; handler != nil {
					handler(err)
				}
			} else {
				q.cfg.metrics.itemsProcessed.Add(float64(buffer.Size()))
				q.cfg.metrics.processDuration.Observe(float64(time.Since(start).Milliseconds()))
				ok = true
				break
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

func markProcessContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, processCtxMarker{}, true)
}

func isProcessContext(ctx context.Context) bool {
	v, ok := ctx.Value(processCtxMarker{}).(bool)
	return ok && v
}

type flushResult struct {
	id  sqlite.BatchID
	err error
}
