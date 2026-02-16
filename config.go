package liq

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec"
	"github.com/teenjuna/liq/retry"
)

type ConfigFunc[Item any] = func(c *Config[Item])

type Config[Item any] struct {
	file                 string
	codec                codec.Codec[Item]
	buffer               buffer.Buffer[Item]
	retryPolicy          retry.Policy
	flushSize            int
	flushTimeout         time.Duration
	workers              int
	batches              int
	metrics              *metrics
	internalErrorHandler func(error)
	processErrorHandler  func(error)
}

func (c *Config[Item]) File(file string) {
	file = strings.TrimSpace(file)
	if file == "" {
		panic("file can't be blank")
	}
	if strings.Contains(file, "?") {
		panic("file can't contain ?")
	}
	c.file = file
}

func (c *Config[Item]) FlushSize(size int) {
	if size <= 0 {
		panic("flush size can't be < 1")
	}
	c.flushSize = size
}

func (c *Config[Item]) FlushTimeout(timeout time.Duration) {
	if timeout < 0 {
		panic("flush timeout can't be < 0")
	}
	c.flushTimeout = timeout
}

func (c *Config[Item]) Buffer(buffer buffer.Buffer[Item]) {
	if buffer == nil {
		panic("buffer can't be nil")
	}
	c.buffer = buffer
}

func (c *Config[Item]) Codec(codec codec.Codec[Item]) {
	if codec == nil {
		panic("codec can't be nil")
	}
	c.codec = codec
}

func (c *Config[Item]) Workers(workers int) {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	c.workers = workers
}

func (c *Config[Item]) Batches(batches int) {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	c.batches = batches
}

func (c *Config[Item]) RetryPolicy(policy retry.Policy) {
	if policy == nil {
		panic("policy can't be nil")
	}
	c.retryPolicy = policy
}

func (c *Config[Item]) Prometheus(registerer prometheus.Registerer) {
	c.metrics = newMetrics(registerer)
}

func (c *Config[Item]) InternalErrorHandler(handler func(error)) {
	c.internalErrorHandler = handler
}

func (c *Config[Item]) ProcessErrorHandler(handler func(error)) {
	c.processErrorHandler = handler
}
