package sqlite_test

import (
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestNew(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, err := sqlite.New(sqlite.WithFile(file))
		deferClose(t, storage)
		require.Nil(t, err)
		require.NotNil(t, storage)
	})
}

func TestPush(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(sqlite.WithFile(file))

		id, err := storage.Push([]byte{1}, 1)
		require.Nil(t, err)
		require.NotEqual(t, id, "")

		require.Nil(t, storage.Close())

		id, err = storage.Push([]byte{2}, 2)
		require.Equal(t, err, sqlite.ErrClosed)
		require.Equal(t, id, "")
	})
}

func TestClaim1(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(sqlite.WithFile(file))
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
			{
				data: []byte{3},
				size: 2,
			},
		}

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		for _, i := range inputs {
			batches, err := storage.Claim()
			require.Nil(t, err)
			require.Equal(t, len(batches), 1)

			batch := batches[0]
			require.Equal(t, batch.Data, i.data)
			require.Equal(t, batch.Size, i.size)
			require.Equal(t, batch.ClaimedTimes, 1)
			require.Equal(
				t,
				batch.CooldownEnd.In(time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			)
			require.NotEqual(t, batch.ID, "")
			require.NotEqual(t, batch.PushedAt, 0)
			require.NotEqual(t, batch.ClaimedAt, 0)
		}

		batches, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches), 0)
	})
}

func TestClaim2(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(
			sqlite.WithFile(file),
			sqlite.WithBatches(2),
		)
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
			{
				data: []byte{3},
				size: 2,
			},
		}

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		batches, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches), 2)
		require.Equal(t, batches[0].Data, inputs[0].data)
		require.Equal(t, batches[0].Size, inputs[0].size)
		require.Equal(t, batches[1].Data, inputs[1].data)
		require.Equal(t, batches[1].Size, inputs[1].size)

		batches, err = storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches), 1)
		require.Equal(t, batches[0].Data, inputs[2].data)
		require.Equal(t, batches[0].Size, inputs[2].size)

		batches, err = storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches), 0)
	})
}

func TestClaimAtomicity(t *testing.T) {
	const (
		workers    = 100
		iterations = 100
	)
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(
			sqlite.WithFile(file),
			sqlite.WithWorkers(workers),
			sqlite.WithBatches(1),
		)
		deferClose(t, storage)

		_, _ = storage.Push([]byte{1, 2}, 1)

		var (
			claimed = new(atomic.Bool)
			wg      = new(sync.WaitGroup)
		)

		for range workers {
			wg.Go(func() {
				for range iterations {
					batches, err := storage.Claim()
					require.Nil(t, err)

					if len(batches) == 0 {
						continue
					}

					require.Equal(t, claimed.Swap(true), false)
					require.Equal(t, claimed.Swap(false), true)
					require.Nil(t, storage.Release(batches[0].ID))
				}
			})
		}

		wg.Wait()
	})
}

func TestRelease(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(
			sqlite.WithFile(file),
			sqlite.WithBatches(2),
		)
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
		}

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		batches1, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches1), 2)

		ids := make([]sqlite.BatchID, len(batches1))
		for i, b := range batches1 {
			ids[i] = b.ID
		}

		batches2, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches2), 0)

		require.Nil(t, storage.Release(ids...))

		batches3, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches3), 2)

		for i := range batches3 {
			require.Equal(t, batches3[i].ID, batches1[i].ID)
			require.Equal(t, batches3[i].Data, batches1[i].Data)
			require.Equal(t, batches3[i].Size, batches1[i].Size)
			require.Equal(t, batches3[i].PushedAt, batches1[i].PushedAt)
			require.Equal(t, batches3[i].ClaimedTimes, 2)
			require.NotEqual(t, batches3[i].ClaimedAt, batches1[i].ClaimedAt)
		}
	})
}

func TestReleaseCooldown(t *testing.T) {
	const (
		cooldown = time.Minute
	)
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(
			sqlite.WithFile(file),
			sqlite.WithBatches(2),
			sqlite.WithCooldown(cooldown),
		)
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
		}

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		synctest.Test(t, func(t *testing.T) {
			batches1, err := storage.Claim()
			require.Nil(t, err)
			require.Equal(t, len(batches1), 2)

			ids := make([]sqlite.BatchID, len(batches1))
			for i, b := range batches1 {
				ids[i] = b.ID
			}

			require.Nil(t, storage.Release(ids...))

			synctest.Wait()

			batches2, err := storage.Claim()
			require.Nil(t, err)
			require.Equal(t, len(batches2), 0)

			<-time.After(cooldown)

			synctest.Wait()

			batches3, err := storage.Claim()
			require.Nil(t, err)
			require.Equal(t, len(batches3), 2)
		})
	})
}

func TestDelete(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(
			sqlite.WithFile(file),
			sqlite.WithBatches(2),
		)
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
		}

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		batches1, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches1), 2)

		batches2, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches2), 0)

		require.Nil(t, storage.Delete(batches1[0].ID))

		batches3, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches3), 0)

		require.Nil(t, storage.Release(batches1[1].ID))

		batches4, err := storage.Claim()
		require.Nil(t, err)
		require.Equal(t, len(batches4), 1)
		require.Equal(t, batches4[0].ID, batches1[1].ID)
		require.Equal(t, batches4[0].Data, batches1[1].Data)
		require.Equal(t, batches4[0].Size, batches1[1].Size)
		require.Equal(t, batches4[0].PushedAt, batches1[1].PushedAt)
		require.Equal(t, batches4[0].ClaimedTimes, 2)
		require.NotEqual(t, batches4[0].ClaimedAt, batches1[1].ClaimedAt)
	})
}

func TestStats(t *testing.T) {
	run(t, func(t *testing.T, file string) {
		storage, _ := sqlite.New(sqlite.WithFile(":memory:"))
		deferClose(t, storage)

		inputs := []struct {
			data []byte
			size int
		}{
			{
				data: []byte{1},
				size: 1,
			},
			{
				data: []byte{2},
				size: 2,
			},
		}

		stats, err := storage.Stats()
		require.Nil(t, err)
		require.Equal(t, stats.Batches, 0)
		require.Equal(t, stats.Items, 0)

		for _, i := range inputs {
			_, _ = storage.Push(i.data, i.size)
		}

		stats, err = storage.Stats()
		require.Nil(t, err)
		require.Equal(t, stats.Batches, 2)
		require.Equal(t, stats.Items, 3)
	})
}

func run(t *testing.T, fn func(t *testing.T, file string)) {
	t.Helper()
	t.Run("In file", func(t *testing.T) {
		t.Helper()
		fn(t, path.Join(t.TempDir(), "file"))
	})
	t.Run("In memory", func(t *testing.T) {
		t.Helper()
		fn(t, ":memory:")
	})
}

func deferClose(t *testing.T, storage *sqlite.Storage) {
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Fatalf("close storage: %v", err)
		}
	})
}
