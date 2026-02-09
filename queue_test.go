// TODO: package liq -> liq_test

package liq

import (
	"context"
	"errors"
	"iter"
	"math/rand/v2"
	"path"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/teenjuna/liq/internal/testing/require"
	"github.com/teenjuna/liq/retry"
)

type Item struct {
	ID string
	N1 int
	N2 int
}

var Data = func() []Item {
	items := make([]Item, 0)
	for i := range 1000 {
		items = append(items, Item{
			ID: strconv.Itoa(i),
			N1: rand.IntN(1000),
			N2: rand.IntN(1000),
		})
	}
	return items
}()

func TestQueueFlushBySize(t *testing.T) {
	run(t, func(t *testing.T) {
		processed := make(chan struct{})

		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				items := slices.Collect(batch)
				require.Equal(t, items, Data)
				processed <- struct{}{}
				return nil
			},
			WithFlushSize[Item](len(Data)),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		synctest.Wait()
		expect(t, processed)
	})
}

func TestQueueFlushByTimeout(t *testing.T) {
	run(t, func(t *testing.T) {
		const timeout = time.Hour
		processed := make(chan struct{})

		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				items := slices.Collect(batch)
				require.Equal(t, items, Data)
				processed <- struct{}{}
				return nil
			},
			WithFlushSize[Item](len(Data)*2),
			WithFlushTimeout[Item](timeout),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		synctest.Wait()
		<-time.After(timeout)

		synctest.Wait()
		expect(t, processed)
	})
}

func TestQueueManualFlush(t *testing.T) {
	run(t, func(t *testing.T) {
		// Unlike other *Flush* tests, this test uses buffered channel. This is needed because
		// this test actually blocks the main goroutine when it cals queue.Flush(). If channel was
		// unbuffered, this will happen:
		//
		// * queue.Flush() call flushes the buffer to SQLite and (since it was called inside a
		// test) also blocks until the flushed buffer is processed by processFunc.
		// * processFunc runs and blocks on channel send (processed <- struct{}{}).
		//
		// Since processFunc can only be unblocked by expect(t, processed), which happens after
		// the blocked queue.Flush(), the test deadlocks.
		processed := make(chan struct{}, 1)

		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				items := slices.Collect(batch)
				require.Equal(t, items, Data)
				processed <- struct{}{}
				return nil
			},
			WithFlushSize[Item](len(Data)*2),
			WithFlushTimeout[Item](time.Hour),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		synctest.Wait()
		select {
		case <-processed:
			t.Fatal("process func called before manual flush")
		default:
		}

		require.Nil(t, queue.Flush(t.Context()))

		synctest.Wait()
		expect(t, processed)
	})
}

func TestPushOnClose(t *testing.T) {
	require.Equal(t, len(Data)%2, 0)
	run(t, func(t *testing.T) {
		var (
			process = make(chan struct{})
			file    = tempFile(t)
		)
		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				<-process

				// Ensure queue is closed.
				require.Equal(t, queue.Close(), ErrClosed)

				// Push half of the batch back to queue.
				items := slices.Collect(batch)
				for i := range len(Data) / 2 {
					require.Nil(t, queue.Push(ctx, items[i]))
				}

				return nil
			},
			WithFile[Item](file),
			WithFlushSize[Item](len(Data)),
		)
		require.Nil(t, err)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		// Wait until items are flushed.
		synctest.Wait()

		// Close the queue. In background, because otherwise we'll have a deadlock.
		go func() {
			require.Nil(t, queue.Close())
		}()

		// Wait until processFunc is blocked on <-canProcess and queue.Close() is blocked on
		// processFunc.
		synctest.Wait()

		// Unblock processFunc.
		process <- struct{}{}

		// Wait until processFunc is finished and queue is closed.
		synctest.Wait()

		// Ensure we can't push into closed queue.
		require.Equal(t, queue.Push(t.Context(), Item{}), ErrClosed)

		// Reopen the queue and make sure processFunc receives saved half of the batch.
		processed := make(chan struct{})
		queue, err = New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				items := slices.Collect(batch)
				require.Equal(t, len(items), len(Data)/2)
				require.Equal(t, items, Data[:len(Data)/2])
				processed <- struct{}{}
				return nil
			},
			WithFile[Item](file),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		// Wait until processFunc is finished.
		<-processed
	})
}

func TestDataPersistenceBetweenRestarts(t *testing.T) {
	run(t, func(t *testing.T) {
		var (
			file        = tempFile(t)
			ctx, cancel = context.WithCancel(t.Context())
		)
		queue, err := New(
			func(_ context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				<-ctx.Done()
				return ctx.Err()
			},
			WithFile[Item](file),
			WithFlushSize[Item](len(Data)/2+1),
		)
		require.Nil(t, err)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		// Wait until internal workers are blocked.
		synctest.Wait()

		// At this point, there is one batch with len(Data)/2+1 elements inside SQLite and one
		// batch with len(Data)/2-1 elements inside in-memory buffer. We cancel the context so
		// that processFunc unblocks but fails, so that the batch in SQLite doesn't go anywhere.
		cancel()

		// Close the queue, hoping that both batches are persisted.
		require.Nil(t, queue.Close())

		// Reopen the queue and make sure that both batches were persisted.
		processed := make(chan struct{})
		queue, err = New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				items := slices.Collect(batch)
				require.Equal(t, len(items), len(Data))
				require.Equal(t, items, Data)
				processed <- struct{}{}
				return nil
			},
			WithFile[Item](file),
			WithBatches[Item](2),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		// Wait until processFunc is finished.
		<-processed
	})
}

func TestMultipleBatches(t *testing.T) {
	const (
		batchSize = 10
		batches   = 10
	)
	require.Equal(t, len(Data)%(batchSize*batches), 0)

	run(t, func(t *testing.T) {
		// First, we create a mock queue that won't process any items. This will allow us to push
		// all batches before they can be processed.
		file := tempFile(t)
		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				<-ctx.Done()
				return ctx.Err()
			},
			WithFile[Item](file),
			WithFlushSize[Item](batchSize),
		)
		require.Nil(t, err)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		require.Nil(t, queue.Close())

		// Now we reopen the queue and ensure that:
		// * Each batch has batchSize*batches items;
		// * Each item is processed exactly once;
		// * processFunc is called exactly len(Data)/(batchSize*batches) times.
		var (
			processed      = make(chan struct{})
			processedItems = make(map[string]struct{}, len(Data))
		)
		queue, err = New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				var items int
				for item := range batch {
					items += 1
					_, ok := processedItems[item.ID]
					require.Equal(t, ok, false)
					processedItems[item.ID] = struct{}{}
				}
				require.Equal(t, items, batchSize*batches)
				processed <- struct{}{}
				return nil
			},
			WithFile[Item](file),
			WithBatches[Item](batches),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		for range len(Data) / (batchSize * batches) {
			<-processed
		}

		require.Equal(t, len(processedItems), len(Data))
	})
}

func TestMultipleWorkers(t *testing.T) {
	const (
		workers     = 10
		batchSize   = 100
		processTime = time.Second
	)
	require.Equal(t, len(Data)%batchSize, 0)

	run(t, func(t *testing.T) {
		// First, we create a mock queue that won't process any items. This will allow us to push
		// all batches before they can be processed.
		file := tempFile(t)
		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				<-ctx.Done()
				return ctx.Err()
			},
			WithFile[Item](file),
			WithFlushSize[Item](batchSize),
		)
		require.Nil(t, err)

		for _, item := range Data {
			require.Nil(t, queue.Push(t.Context(), item))
		}

		require.Nil(t, queue.Close())

		// Now we reopen the queue and ensure that total processing time is `workers` times lower
		// than it would otherwise be.
		started := time.Now()
		processed := make(chan struct{}, workers)
		queue, err = New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				<-time.After(processTime)
				processed <- struct{}{}
				return nil
			},
			WithFile[Item](file),
			WithWorkers[Item](workers),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		for range len(Data) / batchSize {
			<-processed
		}

		totalProcessTime := time.Since(started)

		require.Equal(t,
			totalProcessTime,
			processTime*time.Duration(len(Data)/batchSize/workers),
		)
	})
}

func TestProcessRetries(t *testing.T) {
	const (
		interval = time.Millisecond * 100
		cooldown = time.Millisecond * 500
		workers  = 10
	)
	run(t, func(t *testing.T) {
		processed := make(chan struct{}, workers)
		queue, err := New(
			func(ctx context.Context, queue *Queue[Item], batch iter.Seq[Item]) error {
				processed <- struct{}{}
				return errors.New("retry")
			},
			WithWorkers[Item](workers),
			WithRetryPolicy[Item](retry.
				NewFixed(3, interval).
				WithJitter(0).
				WithCooldown(cooldown),
			),
		)
		require.Nil(t, err)
		deferClose(t, queue)

		require.Nil(t, queue.Push(t.Context(), Item{}))

		requireDelay := func(delay time.Duration) {
			t.Helper()
			tt := time.Now()
			<-processed
			ts := time.Since(tt)
			require.Equal(t, ts, delay)
		}

		requireDelay(0)
		for range 1000 {
			requireDelay(interval)
			requireDelay(interval)
			requireDelay(cooldown)
		}
	})
}

func run(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	synctest.Test(t, fn)
}

func deferClose[Item any](t *testing.T, queue *Queue[Item]) {
	t.Cleanup(func() {
		if err := queue.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	})
}

func expect[T any](t *testing.T, ch chan T) {
	select {
	case <-ch:
	default:
		t.Fatal("channel is empty")
	}
}

func tempFile(t *testing.T) string {
	return path.Join(t.TempDir(), strconv.Itoa(rand.Int()))
}
