package liq

import "iter"

// NOTE: When docs are there, note that codec is not considered thread-safe.
type Codec[Item any] interface {
	Encode(batch iter.Seq[Item]) ([]byte, error)
	Decode(data []byte, push func(Item)) error
}
