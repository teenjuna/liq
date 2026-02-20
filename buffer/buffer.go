package buffer

import "iter"

// Buffer is an in-memory container for queue items.
//
// Implementations are not considered thread-safe and each instance is used by a single worker.
type Buffer[Item any] interface {
	// Push adds an item to the buffer.
	Push(item Item)
	// Size returns the number of items in the buffer.
	Size() int
	// Pushed returns the number of pushes made to buffer, which can be different if the buffer
	// performs some kind of aggregation on pushes.
	Pushes() int
	// Iter returns a sequence of all items in the buffer.
	Iter() iter.Seq[Item]
	// Reset clears all items from the buffer.
	Reset()
	// Derive returns a new buffer instance with the same settings.
	//
	// The returned buffer maintains its own internal state independent of the original.
	Derive() Buffer[Item]
}
