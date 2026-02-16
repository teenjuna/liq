package sqlite

import (
	"strings"
	"time"
)

type Config struct {
	file     string
	workers  int
	batches  int
	cooldown time.Duration
}

type ConfigFunc = func(c *Config)

func (c *Config) File(file string) {
	file = strings.TrimSpace(file)
	if file == "" {
		panic("file can't be blank")
	}
	if strings.Contains(file, "?") {
		panic("file can't contain ?")
	}
	c.file = file
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
