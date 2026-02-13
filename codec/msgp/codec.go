package msgp

import (
	"github.com/teenjuna/liq/internal"
	"github.com/tinylib/msgp/msgp"
)

type Codec[Item any, ItemPtr msgpable[Item]] struct {
	buf []byte
}

var _ internal.Codec[msgp.Raw] = (*Codec[msgp.Raw, *msgp.Raw])(nil)

func New[Item any, ItemPtr msgpable[Item]]() *Codec[Item, ItemPtr] {
	buf := make([]byte, 0)
	return &Codec[Item, ItemPtr]{
		buf: buf,
	}
}

func (c *Codec[Item, ItemPtr]) Encode(buffer internal.Buffer[Item]) ([]byte, error) {
	c.buf = c.buf[:0]
	for item := range buffer.Iter() {
		b, err := ItemPtr(&item).MarshalMsg(c.buf)
		if err != nil {
			return nil, err
		}
		c.buf = b
	}

	return c.buf, nil
}

func (c *Codec[Item, ItemPtr]) Decode(data []byte, buffer internal.Buffer[Item]) error {
	for {
		var item Item
		d, err := ItemPtr(&item).UnmarshalMsg(data)
		if err == msgp.ErrShortBytes {
			break
		} else if err != nil {
			return err
		}
		data = d
		buffer.Push(item)
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
