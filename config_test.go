package liq_test

import (
	"testing"
	"time"

	"github.com/teenjuna/liq"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestOptions(t *testing.T) {
	c := &liq.Config[any]{}

	require.PanicWithError(t, "file can't be blank", func() {
		c.File(" ")
	})

	require.PanicWithError(t, "file can't contain ?", func() {
		c.File("file?key=value")
	})

	require.PanicWithError(t, "buffer can't be nil", func() {
		c.Buffer(nil)
	})

	require.PanicWithError(t, "flush size can't be < 1", func() {
		c.FlushSize(0)
	})

	require.PanicWithError(t, "flush timeout can't be < 0", func() {
		c.FlushTimeout(-time.Second)
	})

	require.PanicWithError(t, "workers can't be < 1", func() {
		c.Workers(0)
	})

	require.PanicWithError(t, "batches can't be < 1", func() {
		c.Batches(0)
	})

	require.PanicWithError(t, "codec can't be nil", func() {
		c.Codec(nil)
	})

	require.PanicWithError(t, "policy can't be nil", func() {
		c.RetryPolicy(nil)
	})
}
