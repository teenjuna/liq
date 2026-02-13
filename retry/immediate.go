package retry

import (
	"context"
	"time"

	"github.com/teenjuna/liq/internal"
)

// TODO: maybe delete this and just use Fixed(0, 0) by default?

type ImmediateRetryPolicy struct {
	attempted int
	attempts  int
	infinite  bool
	cooldown  time.Duration
}

var _ internal.RetryPolicy = (*ImmediateRetryPolicy)(nil)

func Immediate(attempts int) *ImmediateRetryPolicy {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	return &ImmediateRetryPolicy{
		attempts: attempts,
		infinite: attempts == 0,
	}
}

func (r *ImmediateRetryPolicy) WithCooldown(cooldown time.Duration) *ImmediateRetryPolicy {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *ImmediateRetryPolicy) Attempt(ctx context.Context) (ok bool) {
	defer func() {
		if ok && !r.infinite {
			r.attempted += 1
		}
	}()

	if !r.infinite && r.attempted >= r.attempts {
		return false
	}

	return ctx.Err() == nil
}

func (r *ImmediateRetryPolicy) Cooldown() time.Duration {
	return r.cooldown
}
