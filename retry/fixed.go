package retry

import (
	"context"
	"time"
)

type FixedRetryPolicy struct {
	attempted int
	attempts  int
	infinite  bool
	jitter    float64
	interval  time.Duration
	cooldown  time.Duration
}

func Fixed(attempts int, interval time.Duration) *FixedRetryPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if interval <= 0 {
		panic("interval can't be <= 0")
	}
	return &FixedRetryPolicy{
		attempts: attempts,
		infinite: attempts == 0,
		interval: interval,
		jitter:   0.1,
	}
}

func (r *FixedRetryPolicy) WithJitter(jitter float64) *FixedRetryPolicy {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *FixedRetryPolicy) WithCooldown(cooldown time.Duration) *FixedRetryPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *FixedRetryPolicy) Attempt(ctx context.Context) (ok bool) {
	defer func() {
		if ok {
			r.attempted += 1
		}
	}()

	if r.attempted == 0 {
		return true
	}

	if !r.infinite && r.attempted >= r.attempts {
		return false
	}

	return wait(ctx, r.interval, r.jitter)
}

func (r *FixedRetryPolicy) Cooldown() time.Duration {
	return r.cooldown
}
