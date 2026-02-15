package gob

import (
	"bytes"
	"encoding/gob"
	"io"
	"iter"

	"github.com/teenjuna/liq/codec"
)

type Codec[Item any] struct {
	buf *bytes.Buffer
}

func New[Item any]() *Codec[Item] {
	return &Codec[Item]{
		buf: new(bytes.Buffer),
	}
}

func (c *Codec[Item]) Encode(batch iter.Seq[Item]) ([]byte, error) {
	c.buf.Reset()
	enc := gob.NewEncoder(c.buf)

	for item := range batch {
		if err := enc.Encode(&item); err != nil {
			return nil, err
		}
	}

	res := c.buf.Bytes()
	out := make([]byte, len(res))
	copy(out, res)

	return out, nil
}

func (c *Codec[Item]) Decode(data []byte, push func(Item)) error {
	dec := gob.NewDecoder(bytes.NewReader(data))

	for {
		var item Item
		err := dec.Decode(&item)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		push(item)
	}
}

func (c *Codec[Item]) Derive() codec.Codec[Item] {
	return New[Item]()
}
