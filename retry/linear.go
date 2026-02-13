package retry

import (
	"context"
	"time"

	"github.com/teenjuna/liq/internal"
)

type LinearRetryPolicy struct {
	attempted   int
	attempts    int
	infinite    bool
	jitter      float64
	step        time.Duration
	minInterval time.Duration
	maxInterval time.Duration
	maxReached  bool
	cooldown    time.Duration
}

var _ internal.RetryPolicy = (*LinearRetryPolicy)(nil)

func Linear(attempts int, minInterval, maxInterval time.Duration) *LinearRetryPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if minInterval <= 0 {
		panic("minInterval can't be <= 0")
	}
	if minInterval >= maxInterval {
		panic("minInterval can't be >= maxInterval")
	}

	var step time.Duration
	if attempts == 0 {
		step = minInterval
	} else if attempts > 2 {
		step = (maxInterval - minInterval) / time.Duration((attempts - 2))
	}

	return &LinearRetryPolicy{
		attempts:    attempts,
		infinite:    attempts == 0,
		minInterval: minInterval,
		maxInterval: maxInterval,
		step:        step,
		jitter:      0.1,
	}
}

func (r *LinearRetryPolicy) WithStep(step time.Duration) *LinearRetryPolicy {
	if step <= 0 {
		panic("step can't be <= 0")
	}
	r.step = step
	return r
}

func (r *LinearRetryPolicy) WithJitter(jitter float64) *LinearRetryPolicy {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

func (r *LinearRetryPolicy) WithCooldown(cooldown time.Duration) *LinearRetryPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *LinearRetryPolicy) Attempt(ctx context.Context) (ok bool) {
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
		delta := r.step * time.Duration(r.attempted-1)
		interval = r.minInterval + delta
		if interval > r.maxInterval {
			r.maxReached = true
			interval = r.maxInterval
		}
	}

	return wait(ctx, interval, r.jitter)
}

func (r *LinearRetryPolicy) Cooldown() time.Duration {
	return r.cooldown
}
