package msgp

import (
	"bytes"
	"errors"
	"io"

	"github.com/tinylib/msgp/msgp"

	"github.com/teenjuna/liq/internal"
)

type msgpable[Item any] interface {
	*Item
	msgp.Encodable
	msgp.Decodable
}

type Codec[Item any, ItemPtr msgpable[Item]] struct {
	buf *bytes.Buffer
}

var _ internal.Codec[msgp.Raw] = (*Codec[msgp.Raw, *msgp.Raw])(nil)

func New[Item any, ItemPtr msgpable[Item]]() *Codec[Item, ItemPtr] {
	return &Codec[Item, ItemPtr]{
		buf: new(bytes.Buffer),
	}
}

func (c *Codec[Item, ItemPtr]) Encode(buffer internal.Buffer[Item]) ([]byte, error) {
	c.buf.Reset()
	writer := msgp.NewWriter(c.buf)

	for item := range buffer.Iter() {
		if err := ItemPtr(&item).EncodeMsg(writer); err != nil {
			return nil, err
		}
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	return c.buf.Bytes(), nil
}

func (c *Codec[Item, ItemPtr]) Decode(data []byte, buffer internal.Buffer[Item]) error {
	reader := msgp.NewReader(bytes.NewReader(data))

	for {
		var item Item
		if err := ItemPtr(&item).DecodeMsg(reader); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		buffer.Push(item)
	}

	return nil
}

func (c *Codec[Item, ItemPtr]) Derive() internal.Codec[Item] {
	return New[Item, ItemPtr]()
}
