package retry_test

import (
	"testing"
	"testing/synctest"
	"time"
)

const (
	// Amount of time allowed for measurment error.
	EPSILON = time.Microsecond * 10
)

func run(t *testing.T, name string, fn func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		t.Helper()
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			fn(t)
		})
	})
}

func delayFunc(t *testing.T, jitter float64) func(delay time.Duration, fn func()) {
	t.Helper()
	return func(delay time.Duration, fn func()) {
		delta := time.Duration(float64(delay) * jitter)
		minDelay := (delay - delta).Truncate(EPSILON)
		maxDelay := (delay + delta + EPSILON).Truncate(EPSILON)

		tt := time.Now()
		fn()
		ts := time.Since(tt).Truncate(EPSILON)

		if ts < minDelay {
			t.Fatalf("delay %s < min delay %s", ts, minDelay)
		}

		if ts > maxDelay {
			t.Fatalf("delay %s > max delay %s", ts, maxDelay)
		}
	}
}
