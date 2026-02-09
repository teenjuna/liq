package retry

import (
	"context"
	"time"

	"github.com/teenjuna/liq/internal"
)

// TODO: maybe delete this and just use Fixed(0, 0) by default?

type Immediate struct {
	attempted int
	attempts  int
	infinite  bool
	cooldown  time.Duration
}

var _ internal.RetryPolicy = (*Immediate)(nil)

func NewImmediate(attempts int) *Immediate {
	if attempts < 0 {
		panic("attempts can't be < 0")
	}
	return &Immediate{
		attempts: attempts,
		infinite: attempts == 0,
	}
}

func (r *Immediate) WithCooldown(cooldown time.Duration) *Immediate {
	if r.infinite && cooldown > 0 {
		panic("can't set cooldown with infinite attempts")
	}
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	r.cooldown = cooldown
	return r
}

func (r *Immediate) Attempt(ctx context.Context) (ok bool) {
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

func (r *Immediate) Cooldown() time.Duration {
	return r.cooldown
}

func (r *Immediate) Derive() internal.RetryPolicy {
	return NewImmediate(r.attempts).WithCooldown(r.cooldown)
}
