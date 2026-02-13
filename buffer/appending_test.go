package buffer_test

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/teenjuna/liq"
	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/internal/testing/require"
)

var _ liq.Buffer[any] = (*buffer.AppendingBuffer[any])(nil)

func TestAppendingBuffer(t *testing.T) {
	type Item struct {
		ID string
		N1 int
		N2 int
	}

	var input []Item
	for i := range 1000 {
		input = append(input, Item{
			ID: strconv.Itoa(i),
			N1: rand.IntN(1000),
			N2: rand.IntN(1000),
		})
	}

	buffer := buffer.Appending[Item]()
	require.Equal(t, buffer.Size(), 0)

	for i, item := range input {
		buffer.Push(item)
		require.Equal(t, buffer.Size(), i+1)
	}

	items := slices.Collect(buffer.Iter())
	require.Equal(t, len(items), buffer.Size())
	require.Equal(t, len(items), len(input))
	require.Equal(t, items, input)

	buffer.Reset()

	items = slices.Collect(buffer.Iter())
	require.Equal(t, buffer.Size(), 0)
	require.Equal(t, len(items), 0)
}
