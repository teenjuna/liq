// This package contains the main [Codec] interface and several implementations inside subpackages.
package codec

import "iter"

// Codec encodes and decodes queue items for storage.
//
// Implementations are not considered thread-safe and each instance is used by a single worker.
type Codec[Item any] interface {
	// Encode serializes a sequence of items into a byte slice.
	Encode(batch iter.Seq[Item]) ([]byte, error)
	// Decode deserializes a byte slice into items, pushing each to the provided function.
	Decode(data []byte, push func(Item)) error
	// Derive returns a new Codec instance with the same settings.
	//
	// The returned codec maintains its own internal state independent of the original.
	Derive() Codec[Item]
}
