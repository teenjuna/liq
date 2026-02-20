package retry

import (
	"context"
	"time"
)

// FixedPolicy is a [Policy] that implements fixed interval retry strategy.
//
// The delay between retries remains constant throughout all retry attempts.
//
// Instances are created via [Fixed] function. The zero value is not valid.
type FixedPolicy struct {
	attempted int
	attempts  int
	infinite  bool
	jitter    float64
	interval  time.Duration
	cooldown  time.Duration
}

// Fixed creates a [FixedPolicy].
//
// Zero attempts mean infinite retries.
//
// Panics if attempts < 0 or interval < 0.
//
// Default config:
//   - [FixedPolicy.WithJitter] is set to 0.1 (10%)
//   - [FixedPolicy.WithCooldown] is set to 0
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

// WithJitter adds random jitter to the retry interval to prevent "thundering herd" problem. It is
// a fraction of the interval (0.0 to 1.0 exclusive). Panics if the value is not in the interval.
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

// WithCooldown sets the duration a failed batch is unavailable before re-processing (not to be
// confused with the interval before retry!). Panics if a cooldown > 0 is set on a policy with
// infinite attempts or if cooldown < 0.
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
