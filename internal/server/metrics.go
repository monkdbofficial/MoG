package server

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"mog/internal/logging"
)

// StartMetricsServer starts an HTTP server for Prometheus metrics.
func StartMetricsServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	logging.Logger().Info("starting metrics server", zap.String("addr", addr))

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logging.Logger().Error("metrics server failed", zap.Error(err))
	}
}
