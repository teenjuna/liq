package liq

import (
	"strings"
	"time"

	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec"
	"github.com/teenjuna/liq/retry"
)

// ConfigFunc is a function that configures a [Queue].
type ConfigFunc[Item any] = func(c *Config[Item])

// Config contains configuration for a [Queue].
type Config[Item any] struct {
	file                 *FileConfig
	codec                codec.Codec[Item]
	buffer               buffer.Buffer[Item]
	retryPolicy          retry.Policy
	flushSize            int
	flushPushes          int
	flushTimeout         time.Duration
	workers              int
	batches              int
	metrics              *metrics
	internalErrorHandler func(error)
	processErrorHandler  func(error)
}

// File configures the file for the SQLite database. If file is nil, SQLite database will be opened
// in-memory, making the queue not persistent.
//
// Panics if file is blank or contains the `?` symbol.
func (c *Config[Item]) File(file *FileConfig) {
	if file != nil && file.path == "" {
		panic("file can't be blank")
	}
	if file != nil && strings.Contains(file.path, "?") {
		panic("file can't contain `?` symbol")
	}
	c.file = file
}

// FlushSize sets the size of the buffer after which it will be automatically flushed.
//
// It can be different from the [Config.FlushPushes] if the buffer performs some kind of
// aggregation, like [buffer.Merging].
//
// Panics if size < 0.
func (c *Config[Item]) FlushSize(size int) {
	if size < 0 {
		panic("flush size can't be < 0")
	}
	c.flushSize = size
}

// FlushPushes sets the number of pushes to buffer after which it will be automatically flushed.
//
// It can be different from the [Config.FlushSize] if the buffer performs some kind of aggregation,
// like [buffer.Merging].
//
// Panics if pushes < 0.
func (c *Config[Item]) FlushPushes(pushes int) {
	if pushes < 0 {
		panic("flush pushes can't be < 0")
	}
	c.flushPushes = pushes
}

// FlushTimeout sets the amount of time after which the buffer will be automatically flushed. Zero
// means no timeout.
//
// Panics if the timeout is < 0.
func (c *Config[Item]) FlushTimeout(timeout time.Duration) {
	if timeout < 0 {
		panic("flush timeout can't be < 0")
	}
	c.flushTimeout = timeout
}

// Buffer sets the in-memory buffer which will be used by the internal workers for storing the
// items in-memory.
//
// Panics if the buffer is nil.
func (c *Config[Item]) Buffer(buffer buffer.Buffer[Item]) {
	if buffer == nil {
		panic("buffer can't be nil")
	}
	c.buffer = buffer
}

// Codec sets the codec which will be used by the internal workers for turning buffers into bytes
// and vice versa.
//
// Panics if the codec is nil.
func (c *Config[Item]) Codec(codec codec.Codec[Item]) {
	if codec == nil {
		panic("codec can't be nil")
	}
	c.codec = codec
}

// Workers sets the number of processing workers managed by the queue.
//
// Two workers will never retrieve the same batch at the same time.
//
// Panics if the workers < 1.
func (c *Config[Item]) Workers(workers int) {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	c.workers = workers
}

// Batches sets the upper-limit for number of batches that each processing worker will try to
// retrieve at once.
//
// Before passing the items into the [ProcessFunc], the items will go through the worker's own
// instance of buffer (configured by [Config.Buffer]). This means that if the buffer implements
// some kind of aggregation (like [buffer.Merging]), this aggregation will be applied to all the
// items of the retrieved batches before they're passed to the [ProcessFunc] as a single batch.
//
// Panics if the batches < 1.
func (c *Config[Item]) Batches(batches int) {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	c.batches = batches
}

// RetryPolicy sets the retry policy for the [ProcessFunc].
//
// Panics if the policy is nil.
func (c *Config[Item]) RetryPolicy(policy retry.Policy) {
	if policy == nil {
		panic("policy can't be nil")
	}
	c.retryPolicy = policy
}

// Prometheus sets the [PrometheusConfig] that will be used to provide queue's metrics.
//
// If config is nil, no metrics will be provided.
func (c *Config[Item]) Prometheus(config *PrometheusConfig) {
	if config == nil {
		config = Prometheus(nil)
	}
	c.metrics = config.metrics()
}

// InternalErrorHandler sets the function that will be called in case of internal error.
//
// If handler is nil, nothing will be called.
func (c *Config[Item]) InternalErrorHandler(handler func(error)) {
	c.internalErrorHandler = handler
}

// ProcessErrorHandler sets the function that will be called in case [ProcessFunc] returns error.
//
// If handler is nil, nothing will be called.
func (c *Config[Item]) ProcessErrorHandler(handler func(error)) {
	c.processErrorHandler = handler
}
