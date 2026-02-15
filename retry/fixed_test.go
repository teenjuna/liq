package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/teenjuna/liq/internal/testing/require"
	"github.com/teenjuna/liq/retry"
)

var _ retry.Policy = (*retry.FixedPolicy)(nil)

func TestFixed(t *testing.T) {
	run(t, "With infinite attempts", func(t *testing.T) {
		p := retry.Fixed(0, time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts", func(t *testing.T) {
		p := retry.Fixed(5, time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts, jitter and cooldown", func(t *testing.T) {
		p := retry.Fixed(5, time.Second).WithJitter(0.1).WithCooldown(time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Second)
	})

	run(t, "With invalid attempts", func(t *testing.T) {
		require.PanicWithError(t, "attempts can't be < 0", func() {
			_ = retry.Fixed(-1, time.Second)
		})
	})

	run(t, "With invalid interval", func(t *testing.T) {
		require.PanicWithError(t, "interval can't be < 0", func() {
			_ = retry.Fixed(0, -1)
		})
	})

	run(t, "With invalid jitter", func(t *testing.T) {
		require.PanicWithError(t, "jitter can't be < 0", func() {
			_ = retry.Fixed(0, time.Second).WithJitter(-0.1)
		})
		require.PanicWithError(t, "jitter can't be >= 1", func() {
			_ = retry.Fixed(0, time.Second).WithJitter(1)
		})
	})

	run(t, "With invalid cooldown", func(t *testing.T) {
		require.PanicWithError(t, "cooldown can't be < 0", func() {
			_ = retry.Fixed(5, time.Second).WithCooldown(time.Duration(-1))
		})
	})

	run(t, "With infinite attempts and cooldown", func(t *testing.T) {
		require.PanicWithError(t, "can't set cooldown with infinite attempts", func() {
			_ = retry.Fixed(0, time.Second).WithCooldown(time.Second)
		})
	})
}

func TestFixedAttempt(t *testing.T) {
	run(t, "Finite attempts (immediate)", func(t *testing.T) {
		p := retry.Fixed(3, 0).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), false) })
	})

	run(t, "Finite attempts (second)", func(t *testing.T) {
		p := retry.Fixed(3, time.Second).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), false) })
	})

	run(t, "Infinite attempts", func(t *testing.T) {
		p := retry.Fixed(0, time.Second).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		for range 1000 {
			f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		}
	})

	run(t, "Context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		p := retry.Fixed(0, time.Second).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(ctx), true) })
		cancel()
		f(0, func() { require.Equal(t, p.Attempt(ctx), false) })
	})
}

func TestFixedDerive(t *testing.T) {
	const (
		attempts = 5
		interval = time.Second
		jitter   = 0.1
		cooldown = time.Second
	)

	test := func(t *testing.T, p *retry.FixedPolicy) {
		for range attempts {
			require.Equal(t, p.Attempt(t.Context()), true)
		}
		require.Equal(t, p.Attempt(t.Context()), false)
		require.Equal(t, p.Cooldown(), cooldown)
	}

	run(t, "Derive before use", func(t *testing.T) {
		p1 := retry.Fixed(attempts, interval).WithCooldown(cooldown)
		p2 := p1.Derive().(*retry.FixedPolicy)
		test(t, p1)
		test(t, p2)
	})

	run(t, "Derive after use", func(t *testing.T) {
		p1 := retry.Fixed(attempts, interval).WithCooldown(cooldown)
		test(t, p1)
		p2 := p1.Derive().(*retry.FixedPolicy)
		test(t, p2)
	})
}
