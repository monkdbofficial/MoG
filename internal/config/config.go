package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// DBConfig holds the database connection configuration.
type DBConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
}

// Config holds all the configuration for the application.
type Config struct {
	DB            DBConfig `mapstructure:"db"`
	MongoPort     int      `mapstructure:"MOG_MONGO_PORT"`
	MongoUser     string   `mapstructure:"MOG_MONGO_USER"`
	MongoPassword string   `mapstructure:"MOG_MONGO_PASSWORD"`
	LogLevel      string   `mapstructure:"MOG_LOG_LEVEL"`
	LogFile       string   `mapstructure:"log_file"`
	MetricsPort   int      `mapstructure:"MOG_METRICS_PORT"`
}

func loadDotEnv() {
	// Minimal .env loader (no extra dependencies). It only sets keys that are not already set
	// in the environment, so real env vars always win.
	if p := strings.TrimSpace(os.Getenv("MOG_ENV_FILE")); p != "" {
		loadDotEnvFile(p)
		return
	}

	// Search upward from the current working directory so running the binary from a subdir
	// still picks up the repo-root `.env`.
	if wd, err := os.Getwd(); err == nil && wd != "" {
		cur := wd
		for i := 0; i < 12; i++ {
			candidate := filepath.Join(cur, ".env")
			if _, err := os.Stat(candidate); err == nil {
				loadDotEnvFile(candidate)
				return
			}
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}

	// Fallback: look near the executable.
	if exe, err := os.Executable(); err == nil && exe != "" {
		exeDir := filepath.Dir(exe)
		for _, candidate := range []string{
			filepath.Join(exeDir, ".env"),
			filepath.Join(filepath.Dir(exeDir), ".env"),
		} {
			if _, err := os.Stat(candidate); err == nil {
				loadDotEnvFile(candidate)
				return
			}
		}
	}
}

func loadDotEnvFile(p string) {
	if p == "" {
		return
	}

	b, err := os.ReadFile(p)
	if err != nil {
		return
	}
	lines := strings.Split(string(b), "\n")
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" {
			continue
		}
		// Strip optional surrounding quotes.
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		// Do not override existing env vars.
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, val)
	}
}

// Load loads the configuration from environment variables.
func Load() (*Config, error) {
	loadDotEnv()
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v_err := viper.BindEnv("db.host", "MOG_DB_HOST")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("db.port", "MOG_DB_PORT")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("db.user", "MOG_DB_USER")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("db.password", "MOG_DB_PASSWORD")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("db.dbname", "MOG_DB_NAME")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("MOG_MONGO_PORT", "MOG_MONGO_PORT")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("MOG_MONGO_USER", "MOG_MONGO_USER")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("MOG_MONGO_PASSWORD", "MOG_MONGO_PASSWORD")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("MOG_LOG_LEVEL", "MOG_LOG_LEVEL")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("log_file", "LOG_FILE")
	if v_err != nil {
		return nil, v_err
	}
	v_err = viper.BindEnv("MOG_METRICS_PORT", "MOG_METRICS_PORT")
	if v_err != nil {
		return nil, v_err
	}

	// Set default values
	// Prefer IPv4 loopback by default to avoid dual-stack "connection refused" noise on systems
	// where `localhost` resolves to both ::1 and 127.0.0.1.
	viper.SetDefault("db.host", "127.0.0.1")
	viper.SetDefault("db.port", 5432)
	viper.SetDefault("db.user", "monkdb")
	viper.SetDefault("db.password", "monkdb")
	viper.SetDefault("db.dbname", "monkdb")
	viper.SetDefault("MOG_MONGO_PORT", 27017)
	viper.SetDefault("MOG_MONGO_USER", "user")
	viper.SetDefault("MOG_MONGO_PASSWORD", "password")
	viper.SetDefault("MOG_LOG_LEVEL", "info")
	viper.SetDefault("MOG_METRICS_PORT", 8080)

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
