package buffer

import (
	"iter"
	"slices"

	"github.com/teenjuna/liq/internal"
)

var _ internal.Buffer[any] = (*Appending[any])(nil)

type Appending[Item any] struct {
	items []Item
}

func NewAppending[Item any]() *Appending[Item] {
	return &Appending[Item]{
		items: make([]Item, 0),
	}
}

func (b *Appending[Item]) Push(item Item) {
	b.items = append(b.items, item)
}

func (b *Appending[Item]) Size() int {
	return len(b.items)
}

func (b *Appending[Item]) Iter() iter.Seq[Item] {
	return slices.Values(b.items)
}

func (b *Appending[Item]) Reset() {
	clear(b.items)
	b.items = b.items[:0]
}
