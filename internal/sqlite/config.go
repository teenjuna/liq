package sqlite

import (
	"strings"
	"time"
)

type Option = func(*config)

func WithFile(file string) Option {
	file = strings.TrimSpace(file)
	if file == "" {
		panic("file can't be blank")
	}
	if strings.Contains(file, "?") {
		panic("file can't contain ?")
	}
	return func(c *config) {
		c.file = file
	}
}

func WithWorkers(workers int) Option {
	if workers < 1 {
		panic("workers can't be < 1")
	}
	return func(c *config) {
		c.workers = workers
	}
}

func WithBatches(batches int) Option {
	if batches <= 0 {
		panic("batches can't be < 1")
	}
	return func(c *config) {
		c.batches = batches
	}
}

func WithCooldown(cooldown time.Duration) Option {
	if cooldown < 0 {
		panic("cooldown can't be < 0")
	}
	return func(c *config) {
		c.cooldown = cooldown
	}
}

type config struct {
	file     string
	workers  int
	batches  int
	cooldown time.Duration
}

func newConfig(options ...Option) *config {
	options = append([]Option{
		WithFile(":memory:"),
		WithWorkers(1),
		WithBatches(1),
	}, options...)

	cfg := config{}
	for _, opt := range options {
		opt(&cfg)
	}

	return &cfg
}
