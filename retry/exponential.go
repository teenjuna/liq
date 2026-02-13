package retry

import (
	"context"
	"math"
	"time"

	"github.com/teenjuna/liq/internal"
)

type ExponentialRetryPolicy struct {
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

var _ internal.RetryPolicy = (*ExponentialRetryPolicy)(nil)

func Exponential(attempts int, minInterval, maxInterval time.Duration) *ExponentialRetryPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if minInterval <= 0 {
		panic("minInterval can't be <= 0")
	}
	if minInterval >= maxInterval {
		panic("minInterval can't be >= maxInterval")
	}

	return &ExponentialRetryPolicy{
		attempts:    attempts,
		infinite:    attempts == 0,
		minInterval: minInterval,
		maxInterval: maxInterval,
		base:        2,
		jitter:      0.1,
	}
}

func (r *ExponentialRetryPolicy) WithBase(base float64) *ExponentialRetryPolicy {
	if base <= 1 {
		panic("base can't be <= 1")
	}
	r.base = base
	return r
}

func (r *ExponentialRetryPolicy) WithJitter(jitter float64) *ExponentialRetryPolicy {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *ExponentialRetryPolicy) WithCooldown(cooldown time.Duration) *ExponentialRetryPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *ExponentialRetryPolicy) Attempt(ctx context.Context) (ok bool) {
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

func (r *ExponentialRetryPolicy) Cooldown() time.Duration {
	return r.cooldown
}
