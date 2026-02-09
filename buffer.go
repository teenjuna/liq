package liq

import (
	"github.com/teenjuna/liq/internal"
)

// TODO: note that this is considered not thread-safe.
// See docs for [internal.Buffer].
type Buffer[Item any] = internal.Buffer[Item]
