package sqlite

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Config holds configuration for a [Storage].
type Config struct {
	uri      *url.URL
	workers  int
	batches  int
	cooldown time.Duration
}

type ConfigFunc = func(c *Config)

// URI sets the SQLite database URI.
//
// Panics if URI is blank or cannot be parsed.
func (c *Config) URI(uri string) {
	if uri = strings.TrimSpace(uri); uri == "" {
		panic("URI can't be blank")
	}

	if prefix := "file:"; !strings.HasPrefix(uri, prefix) {
		uri = prefix + uri
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		panic(fmt.Errorf("invalid uri `%s`: %w", uri, err))
	}

	c.uri = parsed
}

// Workers sets the maximum number of concurrent database connections.
//
// Panics if workers is less than 1.
func (c *Config) Workers(workers int) {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	c.workers = workers
}

// Batches sets the maximum number of batches that can be claimed in a single [Storage.Claim] call.
//
// Panics if batches is less than or equal to 0.
func (c *Config) Batches(batches int) {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	c.batches = batches
}

// Cooldown sets the duration to wait before released batches can be re-claimed.
//
// Panics if cooldown is negative.
func (c *Config) Cooldown(cooldown time.Duration) {
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	c.cooldown = cooldown
}
