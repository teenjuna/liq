package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/teenjuna/liq/internal/testing/require"
	"github.com/teenjuna/liq/retry"
)

func TestNewLinear(t *testing.T) {
	run(t, "With infinite attempts", func(t *testing.T) {
		p := retry.NewLinear(0, time.Second, time.Minute)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts", func(t *testing.T) {
		p := retry.NewLinear(5, time.Second, time.Minute)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Duration(0))
	})

	run(t, "With finite attempts, step, jitter and cooldown", func(t *testing.T) {
		p := retry.NewLinear(5, time.Second, time.Minute).
			WithStep(time.Second).
			WithJitter(0.1).
			WithCooldown(time.Second)
		require.NotNil(t, p)
		require.Equal(t, p.Cooldown(), time.Second)
	})

	run(t, "With invalid attempts", func(t *testing.T) {
		require.PanicWithError(t, "attempts can't be < 0", func() {
			_ = retry.NewLinear(-1, time.Second, time.Minute)
		})
	})

	run(t, "With invalid interval", func(t *testing.T) {
		require.PanicWithError(t, "minInterval can't be <= 0", func() {
			_ = retry.NewLinear(0, 0, time.Minute)
		})
		require.PanicWithError(t, "minInterval can't be >= maxInterval", func() {
			_ = retry.NewLinear(0, time.Second, time.Second)
		})
	})

	run(t, "With invalid step", func(t *testing.T) {
		require.PanicWithError(t, "step can't be <= 0", func() {
			_ = retry.NewLinear(0, time.Second, time.Minute).WithStep(0)
		})
	})

	run(t, "With invalid jitter", func(t *testing.T) {
		require.PanicWithError(t, "jitter can't be < 0", func() {
			_ = retry.NewLinear(0, time.Second, time.Minute).WithJitter(-0.1)
		})
		require.PanicWithError(t, "jitter can't be >= 1", func() {
			_ = retry.NewLinear(0, time.Second, time.Minute).WithJitter(1)
		})
	})

	run(t, "With invalid cooldown", func(t *testing.T) {
		require.PanicWithError(t, "cooldown can't be < 0", func() {
			_ = retry.NewLinear(5, time.Second, time.Minute).WithCooldown(time.Duration(-1))
		})
	})

	run(t, "With infinite attempts and cooldown", func(t *testing.T) {
		require.PanicWithError(t, "can't set cooldown with infinite attempts", func() {
			_ = retry.NewLinear(0, time.Second, time.Minute).WithCooldown(time.Second)
		})
	})
}

func TestLinearAttempt(t *testing.T) {
	run(t, "Finite attempts", func(t *testing.T) {
		p := retry.NewLinear(5, time.Second, time.Second*4).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*3, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*4, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), false) })
	})

	run(t, "Infinite attempts", func(t *testing.T) {
		p := retry.NewLinear(0, time.Second, time.Second*5).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*3, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*4, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		f(time.Second*5, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		for range 1000 {
			f(time.Second*5, func() { require.Equal(t, p.Attempt(t.Context()), true) })
		}
	})

	run(t, "Context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		p := retry.NewLinear(0, time.Second, time.Second*5).WithJitter(0.1)
		f := delayFunc(t, 0.1)
		f(0, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second*2, func() { require.Equal(t, p.Attempt(ctx), true) })
		f(time.Second*3, func() { require.Equal(t, p.Attempt(ctx), true) })
		cancel()
		f(0, func() { require.Equal(t, p.Attempt(ctx), false) })
	})
}

func TestLinearDerive(t *testing.T) {
	const (
		attempts    = 3
		minInterval = time.Second
		maxInterval = time.Second * 2
		jitter      = 0.1
		cooldown    = time.Second
	)

	test := func(t *testing.T, p *retry.Linear) {
		for range attempts {
			require.Equal(t, p.Attempt(t.Context()), true)
		}
		require.Equal(t, p.Attempt(t.Context()), false)
		require.Equal(t, p.Cooldown(), cooldown)
	}

	run(t, "Derive before use", func(t *testing.T) {
		p1 := retry.NewLinear(attempts, minInterval, maxInterval).WithCooldown(cooldown)
		p2 := p1.Derive().(*retry.Linear)
		test(t, p1)
		test(t, p2)
	})

	run(t, "Derive after use", func(t *testing.T) {
		p1 := retry.NewLinear(attempts, minInterval, maxInterval).WithCooldown(cooldown)
		test(t, p1)
		p2 := p1.Derive().(*retry.Linear)
		test(t, p2)
	})
}
