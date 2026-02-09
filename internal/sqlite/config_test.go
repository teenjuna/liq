package sqlite_test

import (
	"testing"

	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptionValidation(t *testing.T) {
	require.PanicWithError(t, "file can't be blank", func() {
		_ = sqlite.WithFile(" ")
	})

	require.PanicWithError(t, "file can't contain ?", func() {
		_ = sqlite.WithFile("file?key=value")
	})

	require.PanicWithError(t, "workers can't be < 1", func() {
		_ = sqlite.WithWorkers(0)
	})

	require.PanicWithError(t, "batches can't be < 1", func() {
		_ = sqlite.WithBatches(0)
	})

	require.PanicWithError(t, "cooldown can't be < 0", func() {
		_ = sqlite.WithCooldown(-1)
	})
}
