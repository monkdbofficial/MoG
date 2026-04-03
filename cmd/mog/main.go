package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.uber.org/zap"

	"mog/internal/config"
	"mog/internal/data"
	"mog/internal/logging"
	"mog/internal/monkdb"
	"mog/internal/server"
	"mog/internal/version"
)

// main is the program entrypoint.
func main() {
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "print version/build info and exit")
	flag.BoolVar(&showVersion, "v", false, "print version/build info and exit")
	flag.Parse()
	if showVersion {
		fmt.Println(version.Info())
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := logging.Init(cfg.LogLevel, cfg.LogFile); err != nil {
		fmt.Printf("failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	renderStartupScreen(cfg)

	pool, err := monkdb.NewPool(context.Background(), &cfg.DB)
	if err != nil {
		logging.Logger().Warn("failed to create database pool",
			zap.String("db_host", cfg.DB.Host),
			zap.Int("db_port", cfg.DB.Port),
			zap.String("db_user", cfg.DB.User),
			zap.String("db_name", cfg.DB.DBName),
			zap.String("hint", "Start MonkDB/Postgres or set MOG_DB_HOST/MOG_DB_PORT (and MOG_DB_USER/MOG_DB_PASSWORD/MOG_DB_NAME)"),
			zap.Error(err),
		)
		os.Exit(1)
	}
	defer pool.Close()

	srv, err := server.New(pool, cfg)
	if err != nil {
		logging.Logger().Fatal("failed to create server", zap.Error(err))
	}
	defer srv.Close()

	go srv.Start(context.Background())
	go server.StartMetricsServer(cfg.MetricsPort)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logging.Logger().Info("shutting down MoG")
}

// renderStartupScreen is a helper used by the adapter.
func renderStartupScreen(cfg *config.Config) {
	const width = 72
	const labelWidth = 20
	const valueWidth = width - labelWidth - 3
	const (
		colorReset  = "\033[0m"
		colorCyan   = "\033[1;36m"
		colorYellow = "\033[1;33m"
		colorWhite  = "\033[1;37m"
		colorGreen  = "\033[1;32m"
		colorGray   = "\033[1;30m"
	)

	lines := []string{
		"MoG · MongoDB wire protocol -> MonkDB translator",
		"",
		"Website : https://www.monkdb.com",
		"Support : hello@monkdb.com",
		"",
		fmt.Sprintf("Proxy Port    : %d", cfg.MongoPort),
		fmt.Sprintf("Metrics Port  : %d", cfg.MetricsPort),
		fmt.Sprintf("Log Level     : %s", strings.ToUpper(cfg.LogLevel)),
		fmt.Sprintf("Raw Mongo JSON: %s", func() string {
			v := strings.TrimSpace(os.Getenv("MOG_STORE_RAW_MONGO_JSON"))
			if v == "" {
				return "DEFAULT"
			}
			return v
		}()),
	}

	border := strings.Repeat("═", width)
	fmt.Println()
	fmt.Printf("%s╔%s╗%s\n", colorGray, border, colorReset)
	for _, line := range lines {
		fmt.Printf("%s║%s %-*s %s║%s\n", colorGray, colorReset, width-2, line, colorGray, colorReset)
	}

	table := []struct {
		label string
		value string
	}{
		{"MoG version", version.Info()},
		{"Mongo listener", fmt.Sprintf(":%d (Mongo wire)", cfg.MongoPort)},
		{"MonkDB host", fmt.Sprintf("%s:%d", cfg.DB.Host, cfg.DB.Port)},
		{"MonkDB database", cfg.DB.DBName},
		{"Metrics", fmt.Sprintf(":%d/metrics", cfg.MetricsPort)},
		{"Log file", func() string {
			if cfg.LogFile == "" {
				return "STDOUT"
			}
			return cfg.LogFile
		}()},
	}

	fmt.Printf("%s╠%s╣%s\n", colorGray, border, colorReset)
	for _, row := range table {
		label := formatCell(row.label, labelWidth)
		value := formatCell(row.value, valueWidth)
		fmt.Printf("%s║%s %s%s%s %s│%s %s%s%s %s║%s\n",
			colorGray, colorReset,
			colorYellow, label, colorReset,
			colorGray, colorReset,
			colorCyan, value, colorReset,
			colorGray, colorReset,
		)
	}

	fmt.Printf("%s╚%s╝%s\n", colorGray, border, colorReset)
	fmt.Println()
	fmt.Print(colorGreen)
	fmt.Print(strings.TrimRight(data.Logo, "\n"))
	fmt.Print(colorReset)
	fmt.Print("\n\n")
}

// formatCell is a helper used by the adapter.
func formatCell(text string, width int) string {
	if len(text) > width {
		if width > 3 {
			text = text[:width-3] + "..."
		} else {
			text = text[:width]
		}
	}
	return fmt.Sprintf("%-*s", width, text)
}
