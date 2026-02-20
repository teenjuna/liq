package retry

import (
	"context"
	"time"
)

// LinearPolicy is a [Policy] that implements linear backoff retry strategy.
//
// The delay between retries grows linearly: each subsequent wait interval is increased by
// a fixed step value (configured by [LinearPolicy.WithStep]), up to a maximum interval.
//
// Instances are created via [Linear] function. The zero value is not valid.
type LinearPolicy struct {
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

// Linear creates a [LinearPolicy].
//
// Zero attempts mean infinite retries.
//
// Panics if attempts < 0, minInterval <= 0 or minInterval >= maxInterval.
//
// Default config:
//   - [LinearPolicy.WithStep] is calculated automatically based on attempts (or minInterval in
//     case of infinite attempts)
//   - [LinearPolicy.WithJitter] is set to 0.1 (10%)
//   - [LinearPolicy.WithCooldown] is set to 0
//
// If attempts > 2, step is calculated as: (maxInterval - minInterval) / (attempts - 2)
func Linear(attempts int, minInterval, maxInterval time.Duration) *LinearPolicy {
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

	return &LinearPolicy{
		attempts:    attempts,
		infinite:    attempts == 0,
		minInterval: minInterval,
		maxInterval: maxInterval,
		step:        step,
		jitter:      0.1,
	}
}

// WithStep sets the fixed duration added to the interval for each subsequent retry attempt.
func (r *LinearPolicy) WithStep(step time.Duration) *LinearPolicy {
	if step <= 0 {
		panic("step can't be <= 0")
	}
	r.step = step
	return r
}

// WithJitter adds random jitter to the retry interval to prevent "thundering herd" problem. It is
// a fraction of the interval (0.0 to 1.0 exclusive). Panics if the value is not in the interval.
func (r *LinearPolicy) WithJitter(jitter float64) *LinearPolicy {
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
// infinite attempts or if cooldown < 0.
func (r *LinearPolicy) WithCooldown(cooldown time.Duration) *LinearPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *LinearPolicy) Attempt(ctx context.Context) (ok bool) {
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

func (r *LinearPolicy) Cooldown() time.Duration {
	return r.cooldown
}

func (r *LinearPolicy) Derive() Policy {
	return Linear(r.attempts, r.minInterval, r.maxInterval).
		WithStep(r.step).
		WithJitter(r.jitter).
		WithCooldown(r.cooldown)
}
