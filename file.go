package liq

import (
	"net/url"
	"strings"
)

type FileConfig struct {
	path    string
	durable bool
}

func File(file string) *FileConfig {
	return &FileConfig{path: strings.TrimSpace(file)}
}

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
