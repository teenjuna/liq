package liq

import "github.com/teenjuna/liq/internal"

// TODO: note that this is considered not thread-safe.
// See docs for [internal.Codec].
type Codec[Item any] = internal.Codec[Item]
