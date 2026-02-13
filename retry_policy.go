package liq

import (
	"context"
	"time"
)

// NOTE: When docs are there, note that retry policy is not considered thread-safe.
type RetryPolicy interface {
	Attempt(ctx context.Context) bool
	Cooldown() time.Duration
}
