package sqlite

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	uri      *url.URL
	workers  int
	batches  int
	cooldown time.Duration
}

type ConfigFunc = func(c *Config)

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

func (c *Config) Workers(workers int) {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	c.workers = workers
}

func (c *Config) Batches(batches int) {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	c.batches = batches
}

func (c *Config) Cooldown(cooldown time.Duration) {
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	c.cooldown = cooldown
}
