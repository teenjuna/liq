package buffer_test

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/internal/testing/require"
)

func TestMergingBuffer(t *testing.T) {
	type Item struct {
		ID string
		N1 int
		N2 int
	}

	var (
		cmp   = func(i1, i2 Item) int { return cmp.Compare(i1.ID, i2.ID) }
		merge = func(i1, i2 Item) Item {
			return Item{ID: i1.ID, N1: i1.N1 + i2.N1, N2: i1.N2 + i2.N2}
		}
	)

	var input []Item
	for i := range 1000 {
		input = append(input, Item{
			ID: strconv.Itoa(i),
			N1: rand.IntN(1000),
			N2: rand.IntN(1000),
		})
	}
	slices.SortFunc(input, cmp)

	buffer := buffer.Merging(
		func(i Item) string { return i.ID },
		merge,
	)
	require.Equal(t, buffer.Size(), 0)

	for i, item := range input {
		buffer.Push(item)
		require.Equal(t, buffer.Size(), i+1)
	}

	items := slices.SortedFunc(buffer.Iter(), cmp)
	require.Equal(t, len(items), buffer.Size())
	require.Equal(t, len(items), len(input))
	require.Equal(t, items, input)

	var doubledInputs []Item
	for _, item := range input {
		buffer.Push(item)
		require.Equal(t, buffer.Size(), len(input))
		doubledInputs = append(doubledInputs, Item{
			ID: item.ID,
			N1: item.N1 * 2,
			N2: item.N2 * 2,
		})
	}

	items = slices.SortedFunc(buffer.Iter(), cmp)
	require.Equal(t, len(items), buffer.Size())
	require.Equal(t, len(items), len(input))
	require.Equal(t, items, doubledInputs)

	buffer.Reset()

	items = slices.Collect(buffer.Iter())
	require.Equal(t, buffer.Size(), 0)
	require.Equal(t, len(items), 0)
}
