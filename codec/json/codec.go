package json

import (
	"bytes"
	"encoding/json"
	"iter"
	"slices"
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
	items := slices.Collect(batch)

	c.buf.Reset()
	enc := json.NewEncoder(c.buf)

	if err := enc.Encode(items); err != nil {
		return nil, err
	}

	res := c.buf.Bytes()
	out := make([]byte, len(res))
	copy(out, res)

	return out, nil
}

func (c *Codec[Item]) Decode(data []byte, push func(Item)) error {
	var items []Item
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	for _, item := range items {
		push(item)
	}

	return nil
}
