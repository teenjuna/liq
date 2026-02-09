package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

func wait(ctx context.Context, interval time.Duration, jitter float64) bool {
	if jitter < 0 || jitter >= 1 {
		panic("invalid jitter")
	}

	m := (rand.Float64() * 2) - 1
	j := m * jitter * float64(interval)
	d := interval + time.Duration(j)

	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
