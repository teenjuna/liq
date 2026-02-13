package buffer

import (
	"iter"
	"slices"

	"github.com/teenjuna/liq/internal"
)

var _ internal.Buffer[any] = (*AppendingBuffer[any])(nil)

type AppendingBuffer[Item any] struct {
	items []Item
}

func Appending[Item any]() *AppendingBuffer[Item] {
	return &AppendingBuffer[Item]{
		items: make([]Item, 0),
	}
}

func (b *AppendingBuffer[Item]) Push(item Item) {
	b.items = append(b.items, item)
}

func (b *AppendingBuffer[Item]) Size() int {
	return len(b.items)
}

func (b *AppendingBuffer[Item]) Iter() iter.Seq[Item] {
	return slices.Values(b.items)
}

func (b *AppendingBuffer[Item]) Reset() {
	clear(b.items)
	b.items = b.items[:0]
}
