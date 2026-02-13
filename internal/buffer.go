package internal

import "iter"

type Buffer[Item any] interface {
	Push(item Item)
	Size() int
	Iter() iter.Seq[Item]
	Reset()
}
