package liq

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	batches         prometheus.Gauge
	items           prometheus.Gauge
	pushes          prometheus.Counter
	flushes         *prometheus.CounterVec
	processErrors   prometheus.Counter
	processDuration prometheus.Histogram
}

func newMetrics(registerer prometheus.Registerer, namespace, subsystem string) *metrics {
	registerer = prometheus.WrapRegistererWith(
		prometheus.Labels{"component": "liq"},
		registerer,
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
		pushes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "pushes",
			Help:      "Number of pushes into queue's buffer",
		}),
		flushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "flushes",
			Help:      "Number of flushes of queue's buffer",
		}, []string{"type"}),
		processErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_errors",
			Help:      "Number of errors occured during batch processing",
		}),
		processDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_duration",
			Help:      "Duration of batch processing",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 8),
		}),
	}

	if registerer != nil {
		registerer.MustRegister(
			m.batches,
			m.items,
			m.pushes,
			m.flushes,
			m.processErrors,
			m.processDuration,
		)
	}

	return &m
}
