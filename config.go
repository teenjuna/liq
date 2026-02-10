package liq

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/teenjuna/liq/buffer"
	"github.com/teenjuna/liq/codec/json"
	"github.com/teenjuna/liq/internal"
	"github.com/teenjuna/liq/retry"
)

// TODO: aliases for buffer, codec etc

type Option[Item any] = func(*config[Item])

func WithFile[Item any](file string) Option[Item] {
	file = strings.TrimSpace(file)
	if file == "" {
		panic("file can't be blank")
	}
	if strings.Contains(file, "?") {
		panic("file can't contain ?")
	}
	return func(c *config[Item]) {
		c.file = file
	}
}

func WithFlushSize[Item any](size int) Option[Item] {
	if size <= 0 {
		panic("flush size can't be < 1")
	}
	return func(c *config[Item]) {
		c.flushSize = size
	}
}

func WithFlushTimeout[Item any](timeout time.Duration) Option[Item] {
	if timeout < 0 {
		panic("flush timeout can't be < 0")
	}
	return func(c *config[Item]) {
		c.flushTimeout = timeout
	}
}

func WithWorkers[Item any](workers int) Option[Item] {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	return func(c *config[Item]) {
		c.workers = workers
	}
}

func WithBatches[Item any](batches int) Option[Item] {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	return func(c *config[Item]) {
		c.batches = batches
	}
}

func WithCodec[Item any](codec internal.Codec[Item]) Option[Item] {
	if codec == nil {
		panic("codec can't be nil")
	}
	return func(c *config[Item]) {
		c.codec = codec
	}
}

func WithBuffer[Item any](buffer internal.Buffer[Item]) Option[Item] {
	if buffer == nil {
		panic("buffer can't be nil")
	}
	return func(c *config[Item]) {
		c.buffer = buffer
	}
}

func WithRetryPolicy[Item any](policy internal.RetryPolicy) Option[Item] {
	if policy == nil {
		panic("policy can't be nil")
	}
	return func(c *config[Item]) {
		c.retryPolicy = policy
	}
}

func WithPrometheus[Item any](
	registerer prometheus.Registerer,
	namespace, subsystem string,
) Option[Item] {
	return func(c *config[Item]) {
		c.metrics = newMetrics(registerer, namespace, subsystem)
	}
}

type config[Item any] struct {
	file         string
	codec        internal.Codec[Item]
	buffer       internal.Buffer[Item]
	retryPolicy  internal.RetryPolicy
	flushSize    int
	flushTimeout time.Duration
	workers      int
	batches      int
	metrics      *metrics
}

func newConfig[Item any](options ...Option[Item]) *config[Item] {
	options = append([]Option[Item]{
		WithFile[Item](":memory:"),
		WithCodec(json.New[Item]()),
		WithBuffer(buffer.NewAppending[Item]()),
		WithRetryPolicy[Item](retry.NewImmediate(0)),
		WithBatches[Item](1),
		WithWorkers[Item](1),
		WithPrometheus[Item](nil, "namespace", "subsystem"),
	}, options...)

	cfg := config[Item]{}
	for _, opt := range options {
		opt(&cfg)
	}

	return &cfg
}
