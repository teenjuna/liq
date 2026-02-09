package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/teenjuna/liq/internal/testing/require"
	"github.com/teenjuna/liq/retry"
)

func TestNewExponential(t *testing.T) {
	run(t, "With infinite attempts", func(t *testing.T) {
		p := retry.NewExponential(0, time.Second, time.Minute)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts", func(t *testing.T) {
		p := retry.NewExponential(5, time.Second, time.Minute)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts, base, jitter and cooldown", func(t *testing.T) {
		p := retry.NewExponential(5, time.Second, time.Minute).
			WithBase(3).
			WithJitter(0.1).
			WithCooldown(time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Second)
	})

	run(t, "With invalid attempts", func(t *testing.T) {
		require.PanicWithError(t, "attempts can't be < 0", func() {
			_ = retry.NewExponential(-1, time.Second, time.Minute)
		})
	})

	run(t, "With invalid interval", func(t *testing.T) {
		require.PanicWithError(t, "minInterval can't be <= 0", func() {
			_ = retry.NewExponential(0, 0, time.Minute)
		})
		require.PanicWithError(t, "minInterval can't be >= maxInterval", func() {
			_ = retry.NewExponential(0, time.Second, time.Second)
		})
	})

	run(t, "With invalid base", func(t *testing.T) {
		require.PanicWithError(t, "base can't be <= 1", func() {
			_ = retry.NewExponential(0, time.Second, time.Minute).WithBase(1)
		})
	})

	run(t, "With invalid jitter", func(t *testing.T) {
		require.PanicWithError(t, "jitter can't be < 0", func() {
			_ = retry.NewExponential(0, time.Second, time.Minute).WithJitter(-0.1)
		})
		require.PanicWithError(t, "jitter can't be >= 1", func() {
			_ = retry.NewExponential(0, time.Second, time.Minute).WithJitter(1)
		})
	})

	run(t, "With invalid cooldown", func(t *testing.T) {
		require.PanicWithError(t, "cooldown can't be < 0", func() {
			_ = retry.NewExponential(5, time.Second, time.Minute).WithCooldown(time.Duration(-1))
		})
	})

	run(t, "With infinite attempts and cooldown", func(t *testing.T) {
		require.PanicWithError(t, "can't set cooldown with infinite attempts", func() {
			_ = retry.NewExponential(0, time.Second, time.Minute).WithCooldown(time.Second)
		})
	})
}

func TestExponentialAttempt(t *testing.T) {
	run(t, "Finite attempts", func(t *testing.T) {
		p := retry.NewExponential(5, time.Second, time.Second*8).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*4, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*8, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), false) })
	})

	run(t, "Infinite attempts", func(t *testing.T) {
		p := retry.NewExponential(0, time.Second, time.Second*8).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*4, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*8, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		for range 1000 {
			f(time.Second*8, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		}
	})

	run(t, "Context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		p := retry.NewExponential(0, time.Second, time.Second*8).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second*4, func() { require.Equal(t, p.Attempt(ctx), true) })
		cancel()
		f(0, func() { require.Equal(t, p.Attempt(ctx), false) })
	})
}

func TestExponentialDerive(t *testing.T) {
	const (
		attempts    = 3
		minInterval = time.Second
		maxInterval = time.Second * 2
		jitter      = 0.1
		cooldown    = time.Second
	)

	test := func(t *testing.T, p *retry.Exponential) {
		for range attempts {
			require.Equal(t, p.Attempt(t.Context()), true)
		}
		require.Equal(t, p.Attempt(t.Context()), false)
		require.Equal(t, p.Cooldown(), cooldown)
	}

	run(t, "Derive before use", func(t *testing.T) {
		p1 := retry.NewExponential(attempts, minInterval, maxInterval).WithCooldown(cooldown)
		p2 := p1.Derive().(*retry.Exponential)
		test(t, p1)
		test(t, p2)
	})

	run(t, "Derive after use", func(t *testing.T) {
		p1 := retry.NewExponential(attempts, minInterval, maxInterval).WithCooldown(cooldown)
		test(t, p1)
		p2 := p1.Derive().(*retry.Exponential)
		test(t, p2)
	})
}
