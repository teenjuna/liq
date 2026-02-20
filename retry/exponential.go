package retry

import (
	"context"
	"math"
	"time"
)

// ExponentialPolicy is a [Policy] that implements exponential backoff retry strategy.
//
// The delay between retries grows exponentially: each subsequent wait interval is multiplied by
// the base value (configured by [ExponentialPolicy.WithBase]), up to a maximum interval.
//
// Instances are created via [Exponential] function. The zero value is not valid.
type ExponentialPolicy struct {
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

// Exponential creates an [ExponentialPolicy].
//
// Zero attempts mean infinite retries.
//
// Panics if attempts < 0, minInterval <= 0 or maxInterval < minInterval.
//
// Default config:
//   - [ExponentialPolicy.WithBase] is set to 2
//   - [ExponentialPolicy.WithJitter] is set to 0.1 (10%)
//   - [ExponentialPolicy.WithCooldown] is set to 0
func Exponential(attempts int, minInterval, maxInterval time.Duration) *ExponentialPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	if minInterval <= 0 {
		panic("minInterval can't be <= 0")
	}
	if minInterval >= maxInterval {
		panic("maxInterval can't be < minInterval")
	}

	return &ExponentialPolicy{
		attempts:    attempts,
		infinite:    attempts == 0,
		minInterval: minInterval,
		maxInterval: maxInterval,
		base:        2,
		jitter:      0.1,
	}
}

// WithBase seths the number by which each subsequent attempt delay is multiplicated.
func (r *ExponentialPolicy) WithBase(base float64) *ExponentialPolicy {
	if base <= 1 {
		panic("base can't be <= 1")
	}
	r.base = base
	return r
}

// WithJitter adds random jitter to the retry interval to prevent "thundering herd" problem. It is
// a fraction of the interval (0.0 to 1.1 exclusive). Panics if the value is not in the interval.
func (r *ExponentialPolicy) WithJitter(jitter float64) *ExponentialPolicy {
	if jitter < 0 {
		panic("jitter can't be < 0")
	}
	if jitter >= 1 {
		panic("jitter can't be >= 1")
	}
	r.jitter = jitter
	return r
}

// WithCooldown sets the duration a failed batch is unavailable before re-processing (not to be
// confused with the interval before retry!). Panics if a cooldown > 0 is set on a policy with
// infinite attempts or if
func (r *ExponentialPolicy) WithCooldown(cooldown time.Duration) *ExponentialPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *ExponentialPolicy) Attempt(ctx context.Context) (ok bool) {
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

func (r *ExponentialPolicy) Cooldown() time.Duration {
	return r.cooldown
}

func (r *ExponentialPolicy) Derive() Policy {
	return Exponential(r.attempts, r.minInterval, r.maxInterval).
		WithBase(r.base).
		WithJitter(r.jitter).
		WithCooldown(r.cooldown)
}
