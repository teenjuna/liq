package liq

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	batches         prometheus.Gauge
	items           prometheus.Gauge
	itemsPushed     prometheus.Counter
	itemsFlushed    *prometheus.CounterVec
	itemsProcessed  prometheus.Counter
	processErrors   prometheus.Counter
	processDuration prometheus.Histogram
}

func newMetrics(registerer prometheus.Registerer) *metrics {
	const (
		namespace = "liq"
		subsystem = ""
	)

	m := metrics{
		batches: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batches",
			Help:      "Number of batches in queue",
		}),
		items: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items",
			Help:      "Number of items in queue",
		}),
		itemsPushed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_pushed",
			Help:      "Number of items pushed into queue's buffer",
		}),
		itemsFlushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_flushed",
			Help:      "Number of items flushed from queue's buffer",
		}, []string{"type"}),
		itemsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_processed",
			Help:      "Number of processed items",
		}),
		processErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_errors",
			Help:      "Number of errors occurred during batch processing",
		}),
		processDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_duration",
			Help:      "Duration of batch processing",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		}),
	}

	if registerer != nil {
		registerer.MustRegister(
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
