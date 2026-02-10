package liq

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/teenjuna/liq/internal/sqlite"
)

var (
	ErrClosed = errors.New("queue is closed")
)

type Queue[Item any] struct {
	cfg     *config[Item]
	storage *sqlite.Storage

	closing *atomic.Bool
	batches *atomic.Int64
	items   *atomic.Int64

	push      chan Item
	flush     chan chan flushResult
	flushed   chan struct{}
	processed chan processResult

	pushCtx   context.Context
	pushStop  func()
	pushGroup *errgroup.Group

	processCtx  context.Context
	processStop func()
	// https://github.com/golang/go/issues/77249
	// processFunc  ProcessFunc[Item]
	processFunc  func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error
	processGroup *errgroup.Group
}

type ProcessFunc[Item any] = func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error

func New[Item any](
	processFunc ProcessFunc[Item],
	options ...Option[Item],
) (*Queue[Item], error) {
	cfg := newConfig(options...)
	storage, err := sqlite.New(
		sqlite.WithFile(cfg.file),
		sqlite.WithBatches(cfg.batches),
		sqlite.WithWorkers(cfg.workers+1),
		sqlite.WithCooldown(cfg.retryPolicy.Cooldown()),
	)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	var (
		closing = new(atomic.Bool)
		batches = new(atomic.Int64)
		items   = new(atomic.Int64)

		push      = make(chan Item, cfg.flushSize)
		flush     = make(chan chan flushResult)
		flushed   = make(chan struct{}, cfg.workers)
		processed = make(chan processResult)

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

	batches.Store(int64(stats.Batches))
	items.Store(int64(stats.Items))

	queue := Queue[Item]{
		cfg:     cfg,
		storage: storage,

		closing: closing,
		batches: batches,
		items:   items,

		push:      push,
		flush:     flush,
		flushed:   flushed,
		processed: processed,

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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue[Item]) Flush(ctx context.Context) error {
	resCh := make(chan flushResult)
	notify(q.flush, resCh)

	var fres flushResult
	select {
	case <-ctx.Done():
		return ctx.Err()
	case fres = <-resCh:
	}

	if fres.err != nil || fres.id == "" || !testing.Testing() {
		return fres.err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pres := <-q.processed:
			if pres.id != fres.id {
				continue
			}
			if pres.err != nil {
				return fmt.Errorf("process: %w", pres.err)
			}
			return nil
		}
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
		if !q.closing.Load() {
			// TODO: maybe panic is not a good idea...
			panic(q.Close())
		}
	}()
}

func (q *Queue[Item]) pushWorker() error {
	var (
		buffer = q.cfg.buffer.Derive()
		codec  = q.cfg.codec.Derive()
		tick   = ticker(q.cfg.flushTimeout)
	)
	for {
		var (
			flush   bool
			flushCh chan flushResult
		)
		select {
		case <-q.pushCtx.Done():
		loop:
			for {
				select {
				case item := <-q.push:
					buffer.Push(item)
					if buffer.Size() >= q.cfg.flushSize {
						break loop
					}
				default:
					break loop
				}
			}
			if buffer.Size() != 0 {
				flush = true
				break
			}
			return nil
		case flushCh = <-q.flush:
			flush = buffer.Size() != 0
			if !flush {
				notify(flushCh, flushResult{})
			}
		case <-tick:
			flush = buffer.Size() != 0
		case item := <-q.push:
			buffer.Push(item)
			flush = buffer.Size() >= q.cfg.flushSize
		}

		if !flush {
			continue
		}

		data, err := codec.Encode(buffer)
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

		q.batches.Add(1)
		q.items.Add(int64(buffer.Size()))
		notify(flushCh, flushResult{id: id})
		notify(q.flushed, struct{}{})

		buffer.Reset()
	}
}

func (q *Queue[Item]) processWorker() error {
	var (
		buffer = q.cfg.buffer.Derive()
		codec  = q.cfg.codec.Derive()
		tick   = timer(0)
		wait   = false
	)

	for {
		if wait {
			select {
			case <-q.processCtx.Done():
				return nil
			case <-q.flushed:
			case <-tick:
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

			tick = timer(time.Until(stat.NextCooldownEnd))
			wait = true
			continue
		}

		var items int
		for _, batch := range batches {
			items += batch.Size
			if err := codec.Decode(batch.Data, buffer); err != nil {
				return fmt.Errorf("decode batch: %w", err)
			}
		}

		var (
			ctx        = markProcessContext(q.processCtx)
			retry      = q.cfg.retryPolicy.Derive()
			ok         bool
			processErr error
		)
		for {
			if !retry.Attempt(ctx) {
				break
			}
			if processErr = q.processFunc(ctx, q, buffer.Iter()); processErr == nil {
				ok = true
				break
			}
		}

		batchIDs := make([]sqlite.BatchID, len(batches))
		for i, batch := range batches {
			batchIDs[i] = batch.ID
		}

		notifyAll := func(err error) {
			for _, id := range batchIDs {
				notify(q.processed, processResult{id: id, err: err})
			}
		}

		if ok {
			if err := q.storage.Delete(batchIDs...); err != nil {
				err = fmt.Errorf("delete batches: %w", err)
				notifyAll(err)
				return err
			}
			q.batches.Add(-int64(len(batches)))
			q.items.Add(-int64(items))
			notifyAll(nil)
		} else {
			if err := q.storage.Release(batchIDs...); err != nil {
				err = fmt.Errorf("release batches: %w", err)
				notifyAll(err)
				return err
			}
			notify(q.flushed, struct{}{})
			notifyAll(processErr)
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

type processResult struct {
	id  sqlite.BatchID
	err error
}

type flushResult struct {
	id  sqlite.BatchID
	err error
}
