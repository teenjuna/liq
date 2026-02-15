package retry

import (
	"context"
	"time"
)

type FixedPolicy struct {
	attempted int
	attempts  int
	infinite  bool
	jitter    float64
	interval  time.Duration
	cooldown  time.Duration
}

func Fixed(attempts int, interval time.Duration) *FixedPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if interval < 0 {
		panic("interval can't be < 0")
	}
	return &FixedPolicy{
		attempts: attempts,
		infinite: attempts == 0,
		interval: interval,
		jitter:   0.1,
	}
}

func (r *FixedPolicy) WithJitter(jitter float64) *FixedPolicy {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *FixedPolicy) WithCooldown(cooldown time.Duration) *FixedPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *FixedPolicy) Attempt(ctx context.Context) (ok bool) {
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

func (r *FixedPolicy) Cooldown() time.Duration {
	return r.cooldown
}

func (r *FixedPolicy) Derive() Policy {
	return Fixed(r.attempts, r.interval).
		WithJitter(r.jitter).
		WithCooldown(r.cooldown)
}
