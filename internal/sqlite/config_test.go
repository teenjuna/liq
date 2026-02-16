package sqlite_test

import (
	"testing"

	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptionValidation(t *testing.T) {
	cfg := &sqlite.Config{}

	require.PanicWithError(t, "file can't be blank", func() {
		cfg.File(" ")
	})

	require.PanicWithError(t, "file can't contain ?", func() {
		cfg.File("file?key=value")
	})

	require.PanicWithError(t, "workers can't be < 1", func() {
		cfg.Workers(0)
	})

	require.PanicWithError(t, "batches can't be < 1", func() {
		cfg.Batches(0)
	})

	require.PanicWithError(t, "cooldown can't be < 0", func() {
		cfg.Cooldown(-1)
	})
}
