package retry

import (
	"context"
	"time"

	"github.com/teenjuna/liq/internal"
)

type Fixed struct {
	attempted int
	attempts  int
	infinite  bool
	jitter    float64
	interval  time.Duration
	cooldown  time.Duration
}

var _ internal.RetryPolicy = (*Fixed)(nil)

func NewFixed(attempts int, interval time.Duration) *Fixed {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if interval <= 0 {
		panic("interval can't be <= 0")
	}
	return &Fixed{
		attempts: attempts,
		infinite: attempts == 0,
		interval: interval,
		jitter:   0.1,
	}
}

func (r *Fixed) WithJitter(jitter float64) *Fixed {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *Fixed) WithCooldown(cooldown time.Duration) *Fixed {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *Fixed) Attempt(ctx context.Context) (ok bool) {
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

func (r *Fixed) Cooldown() time.Duration {
	return r.cooldown
}

func (r *Fixed) Derive() internal.RetryPolicy {
	return NewFixed(r.attempts, r.interval).
		WithJitter(r.jitter).
		WithCooldown(r.cooldown)
}
