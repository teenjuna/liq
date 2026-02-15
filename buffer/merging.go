package buffer

import (
	"iter"
	"maps"
)

type MergingBuffer[Item any, Key comparable] struct {
	items     map[Key]Item
	keyFunc   func(Item) Key
	mergeFunc func(Item, Item) Item
}

func Merging[Item any, Key comparable](
	keyFunc func(Item) Key,
	mergeFunc func(Item, Item) Item,
) *MergingBuffer[Item, Key] {
	return &MergingBuffer[Item, Key]{
		items:     make(map[Key]Item, 0),
		keyFunc:   keyFunc,
		mergeFunc: mergeFunc,
	}
}

func (b *MergingBuffer[Item, Key]) Push(item Item) {
	key := b.keyFunc(item)
	if existingItem, ok := b.items[key]; ok {
		newItem := b.mergeFunc(existingItem, item)
		b.items[key] = newItem
	} else {
		b.items[key] = item
	}
}

func (b *MergingBuffer[Item, Key]) Size() int {
	return len(b.items)
}

func (b *MergingBuffer[Item, Key]) Iter() iter.Seq[Item] {
	return maps.Values(b.items)
}

func (b *MergingBuffer[Item, Key]) Reset() {
	clear(b.items)
}

func (b *MergingBuffer[Item, Key]) Derive() Buffer[Item] {
	return Merging(b.keyFunc, b.mergeFunc)
}
