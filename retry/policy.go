// This package contains the main [Policy] interface and several implementations.
package retry

import (
	"context"
	"time"
)

// Policy defines the retry behaviour of processing workers.
//
// Implementations are not considered thread-safe and each instance is used by a single worker.
type Policy interface {
	// Attempt checks if another retry attempt should be made.
	//
	// This method blocks until an attempt can be made or the context is cancelled.
	// It internally handles waiting between attempts based on the policy configuration.
	// Returns true if an attempt should be made, false if no attempts remain.
	Attempt(ctx context.Context) bool
	// Cooldown returns the duration to wait before a failed batch can be retrieved for processing
	// again.
	//
	// When a batch fails processing and is released back to the queue, this duration
	// determines how long the batch will be unavailable before being eligible for
	// re-processing.
	Cooldown() time.Duration
	// Derive returns a new Policy instance for a single processing cycle.
	//
	// The returned policy maintains its own internal state for tracking attempts.
	Derive() Policy
}
