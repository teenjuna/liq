package msgp_test

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec/msgp"
	"github.com/teenjuna/liq/internal/testing/require"
)

// NOTE: For some reason, msgp generates a file that tries to import our msgp package alongside of
// the original package, which makes the file invalid. I'm leaving the command for reference, but
// the file should be fixed by hand...
// msgp -file=codec_test.go -o=codec_msgp_test.go -tests=false

type Item struct {
	ID string
	N1 int
	N2 float64
}

func TestCodec(t *testing.T) {

	buffer := buffer.NewAppending[Item]()
	codec := msgp.New[Item]()

	for range 2 {
		buffer.Reset()

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

		derived := codec.Derive()
		require.NotEqual(t, derived, codec)
	}
}
