package json

import (
	"bytes"
	"encoding/json"
	"slices"

	"github.com/teenjuna/liq/internal"
)

type Codec[Item any] struct {
	buf *bytes.Buffer
}

var _ internal.Codec[any] = (*Codec[any])(nil)

func New[Item any]() *Codec[Item] {
	return &Codec[Item]{
		buf: new(bytes.Buffer),
	}
}

func (c *Codec[Item]) Encode(buffer internal.Buffer[Item]) ([]byte, error) {
	items := slices.Collect(buffer.Iter())

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

func (c *Codec[Item]) Decode(data []byte, buffer internal.Buffer[Item]) error {
	var items []Item
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	for _, item := range items {
		buffer.Push(item)
	}

	return nil
}
