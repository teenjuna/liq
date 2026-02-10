package liq

import "github.com/teenjuna/liq/internal"

// NOTE: When docs are there, note that codec is not considered thread-safe.
type Codec[Item any] = internal.Codec[Item]
