package buffer

import (
	"iter"
	"maps"

	"github.com/teenjuna/liq/internal"
)

var _ internal.Buffer[any] = (*Merging[any, struct{}])(nil)

type Merging[Item any, Key comparable] struct {
	items     map[Key]Item
	keyFunc   func(Item) Key
	mergeFunc func(Item, Item) Item
}

func NewMerging[Item any, Key comparable](
	keyFunc func(Item) Key,
	mergeFunc func(Item, Item) Item,
) *Merging[Item, Key] {
	return &Merging[Item, Key]{
		items:     make(map[Key]Item, 0),
		keyFunc:   keyFunc,
		mergeFunc: mergeFunc,
	}
}

func (b *Merging[Item, Key]) Push(item Item) {
	key := b.keyFunc(item)
	if existingItem, ok := b.items[key]; ok {
		newItem := b.mergeFunc(existingItem, item)
		b.items[key] = newItem
	} else {
		b.items[key] = item
	}
}

func (b *Merging[Item, Key]) Size() int {
	return len(b.items)
}

func (b *Merging[Item, Key]) Iter() iter.Seq[Item] {
	return maps.Values(b.items)
}

func (b *Merging[Item, Key]) Reset() {
	clear(b.items)
}

func (b *Merging[Item, Key]) Derive() internal.Buffer[Item] {
	return NewMerging(b.keyFunc, b.mergeFunc)
}
