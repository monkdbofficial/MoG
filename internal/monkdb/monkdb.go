package monkdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"mog/internal/config"
	"mog/internal/logging"
)

// NewPool creates a new pgxpool.Pool.
func NewPool(ctx context.Context, cfg *config.DBConfig) (*pgxpool.Pool, error) {
	// MonkDB speaks the Postgres wire protocol but does not require TLS for local/dev use.
	// Disable TLS negotiation explicitly to avoid "tls error: EOF" against non-TLS endpoints.
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// pgxpool defaults MaxConns to 4, which is far too low for high-QPS adapters.
	// Use an aggressive CPU-scaled default while still allowing the DB/server to be the limiting factor.
	maxConns := int32(runtime.NumCPU() * 64)
	if maxConns < 128 {
		maxConns = 128
	}
	if maxConns > 1024 {
		maxConns = 1024
	}
	pc.MaxConns = maxConns
	pc.MinConns = maxConns / 2
	pc.MaxConnIdleTime = 5 * time.Minute
	pc.MaxConnLifetime = 30 * time.Minute
	pc.HealthCheckPeriod = 15 * time.Second

	// Optimize for high throughput:
	// 1. Enable prepared statement caching (handled by pgx by default, but we can tune it)
	// 2. Reduce handshake overhead
	pc.ConnConfig.Config.RuntimeParams["statement_timeout"] = "30000" // 30s
	pc.ConnConfig.Config.RuntimeParams["search_path"] = "doc,public"

	if logging.Logger() != nil {
		logging.Logger().Info("db pool configured",
			zap.Int32("max_conns", pc.MaxConns),
			zap.Int32("min_conns", pc.MinConns),
		)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return nil, fmt.Errorf("failed to run readiness query: %w", err)
	}

	return pool, nil
}
