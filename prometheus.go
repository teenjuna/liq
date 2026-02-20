package liq

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusConfig is a config of the Prometheus metrics provided by the queue.
//
// An instance can be created only by the [Prometheus] function. The zero value is invalid.
type PrometheusConfig struct {
	// Namespace of the metrics.
	Namespace string
	// Subsystem of the metrics.
	Subsystem string
	// Options for the batches gauge.
	Batches prometheus.GaugeOpts
	// Options for the items gauge.
	Items prometheus.GaugeOpts
	// Options for the pushed items counter.
	ItemsPushed prometheus.CounterOpts
	// Options for the flushed items counter.
	ItemsFlushed prometheus.CounterOpts
	// Options for the processed items counter.
	ItemsProcessed prometheus.CounterOpts
	// Options for the process errors counter.
	ProcessErrors prometheus.CounterOpts
	// Options for the process duration histogram.
	ProcessDuration prometheus.HistogramOpts

	registerer prometheus.Registerer
}

// Prometheus returns a [PrometheusConfig] with the provided registerer. If registerer is nil,
// metrics will not be registered. Many default parameters can be configured by passing
// configuration functions.
func Prometheus(
	registerer prometheus.Registerer,
	configFuncs ...func(c *PrometheusConfig),
) *PrometheusConfig {
	const (
		namespace = "liq"
		subsystem = ""
	)

	c := PrometheusConfig{
		registerer: registerer,
		Namespace:  namespace,
		Subsystem:  subsystem,
		Batches: prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batches",
			Help:      "Number of batches in queue",
		},
		Items: prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items",
			Help:      "Number of items in queue",
		},
		ItemsPushed: prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_pushed",
			Help:      "Number of items pushed into queue's buffer",
		},
		ItemsFlushed: prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_flushed",
			Help:      "Number of items flushed from queue's buffer",
		},
		ItemsProcessed: prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_processed",
			Help:      "Number of processed items",
		},
		ProcessErrors: prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_errors",
			Help:      "Number of errors occurred during batch processing",
		},
		ProcessDuration: prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_duration",
			Help:      "Duration of batch processing",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		},
	}

	for _, cf := range configFuncs {
		if cf != nil {
			cf(&c)
		}
	}

	return &c
}

func (c *PrometheusConfig) metrics() *metrics {
	m := metrics{
		batches:         prometheus.NewGauge(c.Batches),
		items:           prometheus.NewGauge(c.Items),
		itemsPushed:     prometheus.NewCounter(c.ItemsPushed),
		itemsFlushed:    prometheus.NewCounterVec(c.ItemsFlushed, []string{"type"}),
		itemsProcessed:  prometheus.NewCounter(c.ItemsProcessed),
		processErrors:   prometheus.NewCounter(c.ProcessErrors),
		processDuration: prometheus.NewHistogram(c.ProcessDuration),
	}

	if c.registerer != nil {
		c.registerer.MustRegister(
			m.batches,
			m.items,
			m.itemsPushed,
			m.itemsFlushed,
			m.itemsProcessed,
			m.processErrors,
			m.processDuration,
		)
	}

	return &m
}

type metrics struct {
	batches         prometheus.Gauge
	items           prometheus.Gauge
	itemsPushed     prometheus.Counter
	itemsFlushed    *prometheus.CounterVec
	itemsProcessed  prometheus.Counter
	processErrors   prometheus.Counter
	processDuration prometheus.Histogram
}
