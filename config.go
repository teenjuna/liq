package liq

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Config[Item any] struct {
	file         string
	codec        func() Codec[Item]
	buffer       func() Buffer[Item]
	retryPolicy  func() RetryPolicy
	flushSize    int
	flushTimeout time.Duration
	workers      int
	batches      int
	metrics      *metrics
}

func (c *Config[Item]) Apply(
	// https://github.com/golang/go/issues/77249
	// configFunc ConfigFunc[Item]
	configFunc func(*Config[Item]),
) *Config[Item] {
	if configFunc != nil {
		configFunc(c)
	}
	return c
}

func (c *Config[Item]) File(file string) *Config[Item] {
	file = strings.TrimSpace(file)
	if file == "" {
		panic("file can't be blank")
	}
	if strings.Contains(file, "?") {
		panic("file can't contain ?")
	}
	c.file = file
	return c
}

func (c *Config[Item]) FlushSize(size int) *Config[Item] {
	if size <= 0 {
		panic("flush size can't be < 1")
	}
	c.flushSize = size
	return c
}

func (c *Config[Item]) FlushTimeout(timeout time.Duration) *Config[Item] {
	if timeout < 0 {
		panic("flush timeout can't be < 0")
	}
	c.flushTimeout = timeout
	return c
}

func (c *Config[Item]) Buffer(buffer func() Buffer[Item]) *Config[Item] {
	if buffer == nil {
		panic("buffer can't be nil")
	}
	c.buffer = buffer
	return c
}

func (c *Config[Item]) Codec(codec func() Codec[Item]) *Config[Item] {
	if codec == nil {
		panic("codec can't be nil")
	}
	c.codec = codec
	return c
}

func (c *Config[Item]) Workers(workers int) *Config[Item] {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	c.workers = workers
	return c
}

func (c *Config[Item]) Batches(batches int) *Config[Item] {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	c.batches = batches
	return c
}

func (c *Config[Item]) RetryPolicy(policy func() RetryPolicy) *Config[Item] {
	if policy == nil {
		panic("policy can't be nil")
	}
	c.retryPolicy = policy
	return c
}

func (c *Config[Item]) Prometheus(registerer prometheus.Registerer) *Config[Item] {
	c.metrics = newMetrics(registerer)
	return c
}
