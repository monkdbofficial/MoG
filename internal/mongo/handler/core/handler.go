package mongo

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"mog/internal/logging"
	"mog/internal/translator"
)

// Core handler types and helpers.
const (
	reportedMongoVersion = "8.0.0"
	reportedFCV          = "8.0"
	reportedMaxWire      = 25
	catalogCollection    = "__monkdb_catalog"
)

// Handler handles MongoDB operations.
type Handler struct {
	pool       *pgxpool.Pool
	translator *translator.Translator
	tx         pgx.Tx
	touched    map[string]struct{}

	storeRawMongoJSON bool
	logWriteInfo      bool

	blobTable          string
	blobHTTPBase       string
	blobShards         int
	blobMinBytes       int
	blobMetaEnable     bool
	blobMetaTable      string
	blobInlineReads    bool
	blobInlineMaxBytes int
	blobInlineStrict   bool
	blobHTTPTransport  http.RoundTripper
	blobOnce           sync.Once
	blobInitErr        error

	// scram conversation
	scram         *ScramSha256
	scramConv     *Conversation
	authenticated bool
}

// NewHandler creates a new Handler.
func NewHandler(pool *pgxpool.Pool, t *translator.Translator, scram *ScramSha256) *Handler {
	return &Handler{
		pool:              pool,
		translator:        t,
		scram:             scram,
		touched:           map[string]struct{}{},
		storeRawMongoJSON: envBool("MOG_STORE_RAW_MONGO_JSON", false),
		// Minimal info-level write logging is opt-in to avoid performance overhead under high QPS.
		logWriteInfo:       envBool("MOG_INFO_LOG_WRITES", false),
		blobTable:          strings.TrimSpace(os.Getenv("MOG_BLOB_TABLE")),
		blobHTTPBase:       strings.TrimSpace(envString("MOG_BLOB_HTTP_BASE", "http://localhost:6000")),
		blobShards:         envInt("MOG_BLOB_SHARDS", 3),
		blobMinBytes:       envInt("MOG_BLOB_MIN_BYTES", 256),
		blobMetaEnable:     envBool("MOG_BLOB_METADATA", false),
		blobMetaTable:      strings.TrimSpace(envString("MOG_BLOB_METADATA_TABLE", "doc.blob_metadata")),
		blobInlineReads:    envBool("MOG_BLOB_INLINE_READS", false),
		blobInlineMaxBytes: envInt("MOG_BLOB_INLINE_MAX_BYTES", 1024*1024),
		blobInlineStrict:   envBool("MOG_BLOB_INLINE_STRICT", false),
	}
}

func envString(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return def
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

func (h *Handler) blobEnabled() bool {
	return strings.TrimSpace(h.blobTable) != ""
}

func (h *Handler) httpTimeout() time.Duration {
	return 30 * time.Second
}

func (h *Handler) db() DBExecutor {
	if h.tx != nil {
		return h.tx
	}
	return h.pool
}

func (h *Handler) markTouched(physical string) {
	if physical == "" {
		return
	}
	if h.touched == nil {
		h.touched = map[string]struct{}{}
	}
	h.touched[physical] = struct{}{}
}

func (h *Handler) refreshTouched(ctx context.Context) {
	if h.pool == nil || h.tx != nil || len(h.touched) == 0 {
		return
	}
	for physical := range h.touched {
		h.refreshCollection(ctx, physical)
	}
	// reset
	h.touched = map[string]struct{}{}
}

func (h *Handler) refreshCollection(ctx context.Context, physical string) {
	if h.pool == nil || physical == "" {
		return
	}
	_ = h.ensureDocSchema(ctx)
	// Best-effort: MonkDB style backends are near-real-time; refresh makes writes visible to reads.
	if _, err := h.pool.Exec(ctx, "REFRESH TABLE doc."+physical); err != nil {
		// Don't fail the request if refresh isn't supported; just log at debug.
		if logging.Logger() != nil {
			logging.Logger().Debug("refresh table failed", zap.String("physical", physical), zap.Error(err))
		}
	}
}

type DBExecutor interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

// Handle handles a single MongoDB operation.
