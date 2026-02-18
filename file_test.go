package liq

import (
	"testing"

	"github.com/teenjuna/liq/internal/testing/require"
)

func TestURI(t *testing.T) {
	require.Equal(t, File("myfile").uri(), "myfile")
	require.Equal(t, File("myfile?foo=bar").uri(), "myfile")
	require.Equal(t, File("myfile").Durable(true).uri(), "myfile?_sync=full")
	require.Equal(t, File("myfile?foo=bar").Durable(true).uri(), "myfile?_sync=full")
}
