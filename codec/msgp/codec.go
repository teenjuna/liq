package msgp

import (
	"iter"

	"github.com/tinylib/msgp/msgp"
)

type Codec[Item any, ItemPtr msgpable[Item]] struct {
	buf []byte
}

func New[Item any, ItemPtr msgpable[Item]]() *Codec[Item, ItemPtr] {
	buf := make([]byte, 0)
	return &Codec[Item, ItemPtr]{
		buf: buf,
	}
}

func (c *Codec[Item, ItemPtr]) Encode(batch iter.Seq[Item]) ([]byte, error) {
	c.buf = c.buf[:0]
	for item := range batch {
		b, err := ItemPtr(&item).MarshalMsg(c.buf)
		if err != nil {
			return nil, err
		}
		c.buf = b
	}

	return c.buf, nil
}

func (c *Codec[Item, ItemPtr]) Decode(data []byte, push func(Item)) error {
	for {
		var item Item
		d, err := ItemPtr(&item).UnmarshalMsg(data)
		if err == msgp.ErrShortBytes {
			break
		} else if err != nil {
			return err
		}
		data = d
		push(item)
	}

	return nil
}

type msgpable[Item any] interface {
	*Item
	msgp.Encodable
	msgp.Decodable

	// For the future:

	msgp.Marshaler
	msgp.Unmarshaler
}
