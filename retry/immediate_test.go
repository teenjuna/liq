package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/teenjuna/liq/internal/testing/require"
	"github.com/teenjuna/liq/retry"
)

func TestNewImmediate(t *testing.T) {
	run(t, "With infinite attempts", func(t *testing.T) {
		p := retry.NewImmediate(0)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts", func(t *testing.T) {
		p := retry.NewImmediate(5)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts and cooldown", func(t *testing.T) {
		p := retry.NewImmediate(5).WithCooldown(time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Second)
	})

	run(t, "With invalid attempts", func(t *testing.T) {
		require.PanicWithError(t, "attempts can't be < 0", func() {
			_ = retry.NewImmediate(-1)
		})
	})

	run(t, "With invalid cooldown", func(t *testing.T) {
		require.PanicWithError(t, "cooldown can't be < 0", func() {
			_ = retry.NewImmediate(5).WithCooldown(time.Duration(-1))
		})
	})

	run(t, "With infinite attempts and cooldown", func(t *testing.T) {
		require.PanicWithError(t, "can't set cooldown with infinite attempts", func() {
			_ = retry.NewImmediate(0).WithCooldown(time.Second)
		})
	})
}

func TestImmediateAttempt(t *testing.T) {
	run(t, "Finite attempts", func(t *testing.T) {
		p := retry.NewImmediate(2)
		require.Equal(t, p.Attempt(t.Context()), true)
		require.Equal(t, p.Attempt(t.Context()), true)
		require.Equal(t, p.Attempt(t.Context()), false)
	})

	run(t, "Infinite attempts", func(t *testing.T) {
		p := retry.NewImmediate(0)
		for range 1000 {
			require.Equal(t, p.Attempt(t.Context()), true)
		}
	})

	run(t, "Context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		p := retry.NewImmediate(0)
		require.Equal(t, p.Attempt(ctx), true)
		cancel()
		require.Equal(t, p.Attempt(ctx), false)
	})
}
