package liq

import (
	"net/url"
	"strings"
)

// FileConfig is a config of the file used for the SQLite database.
//
// An instance can be created only by the [File] function. The zero value is invalid.
type FileConfig struct {
	path    string
	durable bool
}

// File returns a [FileConfig] with the provided file path.
//
// The file path can be a relative or absolute path to a SQLite database file.
func File(file string) *FileConfig {
	return &FileConfig{path: strings.TrimSpace(file)}
}

// Durable makes each flush of the queue's buffer into the SQLite file durably
// synchronized to disk, which is disabled by default.
//
// This is achieved by setting SQLite's PRAGMA synchronous to FULL instead of NORMAL. This
// setting is expected to make flushes slower, so enable it only if you know what you're doing.
// You can read more about it here:
//   - https://sqlite.org/wal.html#performance_considerations
//   - https://sqlite.org/pragma.html#pragma_synchronous
func (c *FileConfig) Durable(durable bool) *FileConfig {
	c.durable = durable
	return c
}

func (c *FileConfig) uri() string {
	if c == nil {
		return ":memory:"
	}

	query := url.Values{}
	if c.durable {
		query.Set("_sync", "full")
	}

	uri, err := url.Parse(c.path)
	if err != nil {
		return c.path + "?" + query.Encode()
	}

	uri.RawQuery = query.Encode()

	return uri.String()
}
