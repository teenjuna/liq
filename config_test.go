package liq_test

import (
	"testing"

	"github.com/teenjuna/liq"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptionValidation(t *testing.T) {
	require.PanicWithError(t, "file can't be blank", func() {
		_ = liq.WithFile[any](" ")
	})

	require.PanicWithError(t, "file can't contain ?", func() {
		_ = liq.WithFile[any]("file?key=value")
	})

	require.PanicWithError(t, "buffer can't be nil", func() {
		_ = liq.WithBuffer[any](nil)
	})

	require.PanicWithError(t, "flush size can't be < 1", func() {
		_ = liq.WithFlushSize[any](0)
	})

	require.PanicWithError(t, "flush timeout can't be < 0", func() {
		_ = liq.WithFlushTimeout[any](-1)
	})

	require.PanicWithError(t, "workers can't be < 1", func() {
		_ = liq.WithWorkers[any](0)
	})

	require.PanicWithError(t, "batches can't be < 1", func() {
		_ = liq.WithBatches[any](0)
	})

	require.PanicWithError(t, "codec can't be nil", func() {
		_ = liq.WithCodec[any](nil)
	})

	require.PanicWithError(t, "codec can't be nil", func() {
		_ = liq.WithCodec[any](nil)
	})

	require.PanicWithError(t, "policy can't be nil", func() {
		_ = liq.WithRetryPolicy[any](nil)
	})
}
