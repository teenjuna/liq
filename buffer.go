package liq

import "github.com/teenjuna/liq/internal"

// NOTE: When docs are there, note that buffer is not considered thread-safe.
type Buffer[Item any] = internal.Buffer[Item]
