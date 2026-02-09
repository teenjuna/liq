package internal

import (
	"context"
	"time"
)

type RetryPolicy interface {
	Attempt(ctx context.Context) bool
	Cooldown() time.Duration
	Derive() RetryPolicy
}
