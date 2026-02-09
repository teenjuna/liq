package retry

import (
	"context"
	"math"
	"time"

	"github.com/teenjuna/liq/internal"
)

type Exponential struct {
	attempted   int
	attempts    int
	infinite    bool
	jitter      float64
	base        float64
	minInterval time.Duration
	maxInterval time.Duration
	maxReached  bool
	cooldown    time.Duration
}

var _ internal.RetryPolicy = (*Exponential)(nil)

func NewExponential(attempts int, minInterval, maxInterval time.Duration) *Exponential {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if minInterval <= 0 {
		panic("minInterval can't be <= 0")
	}
	if minInterval >= maxInterval {
		panic("minInterval can't be >= maxInterval")
	}

	return &Exponential{
		attempts:    attempts,
		infinite:    attempts == 0,
		minInterval: minInterval,
		maxInterval: maxInterval,
		base:        2,
		jitter:      0.1,
	}
}

func (r *Exponential) WithBase(base float64) *Exponential {
	if base <= 1 {
		panic("base can't be <= 1")
	}
	r.base = base
	return r
}

func (r *Exponential) WithJitter(jitter float64) *Exponential {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *Exponential) WithCooldown(cooldown time.Duration) *Exponential {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *Exponential) Attempt(ctx context.Context) (ok bool) {
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

	var interval time.Duration
	if r.maxReached {
		interval = r.maxInterval
	} else {
		multiplier := time.Duration(math.Pow(r.base, float64((r.attempted - 1))))
		interval = r.minInterval * multiplier
		if interval > r.maxInterval {
			r.maxReached = true
			interval = r.maxInterval
		}
	}

	return wait(ctx, interval, r.jitter)
}

func (r *Exponential) Cooldown() time.Duration {
	return r.cooldown
}

func (r *Exponential) Derive() internal.RetryPolicy {
	return NewExponential(r.attempts, r.minInterval, r.maxInterval).
		WithBase(r.base).
		WithJitter(r.jitter).
		WithCooldown(r.cooldown)
}
