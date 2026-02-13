package liq

import "iter"

// NOTE: When docs are there, note that buffer is not considered thread-safe.
type Buffer[Item any] interface {
	Push(item Item)
	Size() int
	Iter() iter.Seq[Item]
	Reset()
}
