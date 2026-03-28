package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	mongoCommandsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mog_mongo_commands_total",
			Help: "Total number of Mongo commands handled by MoG.",
		},
		[]string{"proto", "cmd"},
	)

	mongoCommandDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mog_mongo_command_duration_seconds",
			Help:    "End-to-end time spent handling a Mongo command in MoG.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"proto", "cmd"},
	)
)

func init() {
	prometheus.MustRegister(mongoCommandsTotal)
	prometheus.MustRegister(mongoCommandDurationSeconds)
}

func ObserveMongoCommand(proto, cmd string, dur time.Duration) {
	if proto == "" {
		proto = "unknown"
	}
	if cmd == "" {
		cmd = "unknown"
	}
	mongoCommandsTotal.WithLabelValues(proto, cmd).Inc()
	mongoCommandDurationSeconds.WithLabelValues(proto, cmd).Observe(dur.Seconds())
}

