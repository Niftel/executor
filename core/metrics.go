package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Bootstrap metrics are labelled by mode: "remote" (push the pack + launch the
// daemon over SSH), "local" (run the daemon on the executor), or "inventory_sync".
var (
	BootstrapTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "praetor_executor_bootstraps_total",
		Help: "Total bootstrap attempts by mode.",
	}, []string{"mode"})

	BootstrapFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "praetor_executor_bootstrap_failures_total",
		Help: "Bootstrap attempts that returned an error, by mode.",
	}, []string{"mode"})

	BootstrapDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "praetor_executor_bootstrap_duration_seconds",
		Help:    "Bootstrap wall time by mode.",
		Buckets: prometheus.DefBuckets,
	}, []string{"mode"})
)
