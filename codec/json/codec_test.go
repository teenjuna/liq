package json_test

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec/json"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestCodec(t *testing.T) {
	type Item struct {
		ID string
		N1 int
		N2 float64
	}

	buffer := buffer.NewAppending[Item]()
	codec := json.New[Item]()

	var items []Item
	for i := range 1000 {
		item := Item{
			ID: strconv.Itoa(i),
			N1: rand.IntN(1000),
			N2: rand.Float64() * 1000,
		}
		items = append(items, item)
		buffer.Push(item)
	}

	data, err := codec.Encode(buffer)
	require.Nil(t, err)
	require.NotEqual(t, len(data), 0)

	buffer.Reset()

	err = codec.Decode(data, buffer)
	require.Nil(t, err)

	bufferItems := slices.Collect(buffer.Iter())
	require.Equal(t, bufferItems, items)
}
