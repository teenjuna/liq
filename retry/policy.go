package retry

import (
	"context"
	"time"
)

// NOTE: When docs are there, note that retry policy is not considered thread-safe.
type Policy interface {
	Attempt(ctx context.Context) bool
	Cooldown() time.Duration
	Derive() Policy
}
