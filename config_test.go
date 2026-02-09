// TODO: package liq -> liq_test

package liq

import (
	"testing"

	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptionValidation(t *testing.T) {
	require.PanicWithError(t, "file can't be blank", func() {
		_ = WithFile[any](" ")
	})

	require.PanicWithError(t, "file can't contain ?", func() {
		_ = WithFile[any]("file?key=value")
	})

	require.PanicWithError(t, "buffer can't be nil", func() {
		_ = WithBuffer[any](nil)
	})

	require.PanicWithError(t, "flush size can't be < 1", func() {
		_ = WithFlushSize[any](0)
	})

	require.PanicWithError(t, "flush timeout can't be < 0", func() {
		_ = WithFlushTimeout[any](-1)
	})

	require.PanicWithError(t, "workers can't be < 1", func() {
		_ = WithWorkers[any](0)
	})

	require.PanicWithError(t, "batches can't be < 1", func() {
		_ = WithBatches[any](0)
	})

	require.PanicWithError(t, "codec can't be nil", func() {
		_ = WithCodec[any](nil)
	})

	require.PanicWithError(t, "codec can't be nil", func() {
		_ = WithCodec[any](nil)
	})

	require.PanicWithError(t, "policy can't be nil", func() {
		_ = WithRetryPolicy[any](nil)
	})
}
