package sqlite_test

import (
	"testing"

	"github.com/teenjuna/liq/internal/sqlite"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptionValidation(t *testing.T) {
	cfg := &sqlite.Config{}

	require.PanicWithError(t, "URI can't be blank", func() {
		cfg.URI(" ")
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
