package mongo

import (
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"gopkg.in/mgo.v2/bson"
	"mog/internal/logging"
)

const (
	componentField       = "db_adapter"
	adapterName          = "MoG"
	slowQueryEnv         = "MOG_SLOW_QUERY_THRESHOLD_MS"
	slowScanEnv          = "MOG_SLOW_SCAN_THRESHOLD_MS"
	slowAdapterEnv       = "MOG_SLOW_ADAPTER_THRESHOLD_MS"
	infoRequestsEnv      = "MOG_INFO_LOG_REQUESTS"
	defaultSlowQueryMS   = 100
	defaultSlowScanMS    = 50
	defaultSlowAdapterMS = 150
	defaultQueryName     = "query"
	RequestStageEntry    = "request.entry"
	RequestStageComplete = "request.complete"
	PushdownStageName    = "pushdown.analysis"
	DBStageName          = "db.query"
)

var (
	thresholdOnce sync.Once
	thresholds    struct {
		query   int
		scan    int
		adapter int
	}
)

type QueryLogOptions struct {
	Stage                  string
	Method                 string
	Operation              string
	Table                  string
	QueryName              string
	QueryTemplate          string
	ArgsCount              int
	StartedAt              time.Time
	DBDuration             time.Duration
	ScanDuration           time.Duration
	PostProcessingDuration time.Duration
	PushdownDuration       time.Duration
	TotalDuration          time.Duration
	Rows                   int
	RowsAffected           int64
	TxUsage                string
	Error                  error
	PushdownReason         string
	PushedDownFilters      []string
	NonPushedFilters       []string
	SortFields             []string
	Limit                  int
	Skip                   int
	QueryHash              uint64
}

// loadThresholds is a helper used by the adapter.
func loadThresholds() {
	thresholdOnce.Do(func() {
		thresholds.query = envIntWithDefault(slowQueryEnv, defaultSlowQueryMS)
		thresholds.scan = envIntWithDefault(slowScanEnv, defaultSlowScanMS)
		thresholds.adapter = envIntWithDefault(slowAdapterEnv, defaultSlowAdapterMS)
	})
}

// envIntWithDefault is a helper used by the adapter.
func envIntWithDefault(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// adapterLogger is a helper used by the adapter.
func adapterLogger() (*zap.Logger, bool) {
	logger := logging.Logger()
	if logger == nil {
		return nil, false
	}
	if !logger.Core().Enabled(zap.DebugLevel) {
		return nil, false
	}
	return logger.With(
		zap.String("component", componentField),
		zap.String("adapter", adapterName),
	), true
}

// adapterBaseLogger returns a logger with stable adapter-identifying fields.
func adapterBaseLogger() *zap.Logger {
	logger := logging.Logger()
	if logger == nil {
		return nil
	}
	return logger.With(
		zap.String("component", componentField),
		zap.String("adapter", adapterName),
	)
}

// AdapterDebugEnabled is a helper used by the adapter.
func AdapterDebugEnabled() bool {
	logger := logging.Logger()
	return logger != nil && logger.Core().Enabled(zap.DebugLevel)
}

// AdapterDebug is a helper used by the adapter.
func AdapterDebug(stage string, fields ...zap.Field) {
	if logger, ok := adapterLogger(); ok {
		all := append([]zap.Field{zap.String("stage", stage)}, fields...)
		logger.Debug("adapter debug", all...)
	}
}

// AdapterWarn is a helper used by the adapter.
func AdapterWarn(stage string, fields ...zap.Field) {
	logger := logging.Logger()
	if logger == nil {
		return
	}
	all := append([]zap.Field{
		zap.String("component", componentField),
		zap.String("adapter", adapterName),
		zap.String("stage", stage),
	}, fields...)
	logger.Warn("adapter warning", all...)
}

// AdapterError is a helper used by the adapter.
func AdapterError(stage string, err error, fields ...zap.Field) {
	logger := logging.Logger()
	if logger == nil {
		return
	}
	all := append([]zap.Field{
		zap.String("component", componentField),
		zap.String("adapter", adapterName),
		zap.String("stage", stage),
	}, fields...)
	logger.Error("adapter error", append(all, zap.Error(err))...)
}

// LogQuery is a helper used by the adapter.
func LogQuery(opts QueryLogOptions) {
	logger := adapterBaseLogger()
	if logger == nil {
		return
	}
	if opts.Stage == "" {
		opts.Stage = DBStageName
	}
	if opts.Method == "" {
		opts.Method = opts.Operation
	}
	if opts.Table == "" {
		opts.Table = defaultQueryName
	}
	totalDuration := opts.TotalDuration
	if totalDuration <= 0 {
		totalDuration = opts.PushdownDuration + opts.DBDuration + opts.ScanDuration + opts.PostProcessingDuration
	}
	if totalDuration <= 0 && !opts.StartedAt.IsZero() {
		totalDuration = time.Since(opts.StartedAt)
	}
	loadThresholds()
	dbDurationMs := durationMillis(opts.DBDuration)
	scanDurationMs := durationMillis(opts.ScanDuration)
	postProcessingMs := durationMillis(opts.PostProcessingDuration)
	pushdownMs := durationMillis(opts.PushdownDuration)
	totalDurationMs := durationMillis(totalDuration)
	slowQuery := thresholds.query > 0 && opts.DBDuration.Milliseconds() >= int64(thresholds.query)
	slowScan := thresholds.scan > 0 && opts.ScanDuration.Milliseconds() >= int64(thresholds.scan)
	slowAdapter := thresholds.adapter > 0 && totalDuration.Milliseconds() >= int64(thresholds.adapter)

	if opts.QueryHash == 0 {
		opts.QueryHash = QueryHash(opts.QueryTemplate)
	}

	fields := []zap.Field{
		zap.String("stage", opts.Stage),
		zap.String("method", opts.Method),
		zap.String("operation", opts.Operation),
		zap.String("query_name", opts.QueryName),
		zap.String("table", opts.Table),
		zap.Int("args_count", opts.ArgsCount),
		zap.Uint64("query_hash", opts.QueryHash),
		zap.Int("limit", opts.Limit),
		zap.Int("skip", opts.Skip),
		zap.String("pushdown_reason", opts.PushdownReason),
		zap.Strings("pushed_down_filters", opts.PushedDownFilters),
		zap.Strings("non_pushed_filters", opts.NonPushedFilters),
		zap.Strings("sort_fields", opts.SortFields),
		zap.Float64("pushdown_duration_ms", pushdownMs),
		zap.Float64("db_duration_ms", dbDurationMs),
		zap.Float64("scan_duration_ms", scanDurationMs),
		zap.Float64("post_processing_duration_ms", postProcessingMs),
		zap.Float64("total_duration_ms", totalDurationMs),
		zap.Int("rows", opts.Rows),
		zap.Int64("rows_affected", opts.RowsAffected),
		zap.String("tx_usage", opts.TxUsage),
		zap.Bool("slow_query", slowQuery),
		zap.Bool("slow_scan", slowScan),
		zap.Bool("slow_adapter_call", slowAdapter),
	}
	opts.FieldsCleanup()

	// Avoid logging large SQL strings during normal request logs. Include SQL only
	// when debugging or when it helps troubleshoot slow calls/failures.
	includeSQL := AdapterDebugEnabled() || opts.Error != nil || slowQuery || slowScan || slowAdapter
	if includeSQL && opts.QueryTemplate != "" {
		fields = append(fields, zap.String("sql", opts.QueryTemplate))
	}
	if opts.Error != nil {
		fields = append(fields, zap.Error(opts.Error))
	}

	// Production-oriented logging:
	// - Debug mode: log all stages.
	// - Errors: always log.
	// - Request completion: log at info for response-time visibility.
	// - Slow calls: log at warn.
	// - Other stages: log only if explicitly enabled via MOG_INFO_LOG_REQUESTS.
	switch {
	case AdapterDebugEnabled():
		logger.Debug("mongo query", fields...)
	case opts.Error != nil:
		logger.Error("mongo query", fields...)
	case slowQuery || slowScan || slowAdapter:
		logger.Warn("mongo query", fields...)
	case opts.Stage == RequestStageComplete:
		logger.Info("mongo request", fields...)
	default:
		if v := strings.TrimSpace(os.Getenv(infoRequestsEnv)); v != "" && v != "0" && strings.ToLower(v) != "false" {
			logger.Info("mongo query", fields...)
		}
	}
}

// durationMillis is a helper used by the adapter.
func durationMillis(d time.Duration) float64 {
	return float64(d.Milliseconds())
}

// FieldsCleanup is a helper used by the adapter.
func (opts *QueryLogOptions) FieldsCleanup() {
	if len(opts.SortFields) == 0 {
		opts.SortFields = nil
	}
	if len(opts.PushedDownFilters) == 0 {
		opts.PushedDownFilters = nil
	}
	if len(opts.NonPushedFilters) == 0 {
		opts.NonPushedFilters = nil
	}
}

// QueryHash is a helper used by the adapter.
func QueryHash(sql string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return h.Sum64()
}

// StageNames is a helper used by the adapter.
func StageNames(pipeline []bson.M) []string {
	names := make([]string, 0, len(pipeline))
	for _, stage := range pipeline {
		for op := range stage {
			names = append(names, strings.TrimPrefix(op, "$"))
		}
	}
	return names
}

// MatchFieldNames is a helper used by the adapter.
func MatchFieldNames(pipeline []bson.M) []string {
	set := map[string]struct{}{}
	for _, stage := range pipeline {
		if match, ok := stage["$match"].(bson.M); ok {
			for k := range match {
				set[k] = struct{}{}
			}
		}
	}
	return mapKeysSorted(set)
}

// SortFields is a helper used by the adapter.
func SortFields(pipeline []bson.M) []string {
	set := map[string]struct{}{}
	for _, stage := range pipeline {
		if sortDoc, ok := stage["$sort"].(bson.M); ok {
			for k := range sortDoc {
				set[k] = struct{}{}
			}
		}
	}
	return mapKeysSorted(set)
}

// Pagination is a helper used by the adapter.
func Pagination(pipeline []bson.M) (limit, skip int) {
	for _, stage := range pipeline {
		if raw, ok := stage["$limit"]; ok {
			if n, ok := toInt(raw); ok {
				limit = n
			}
		}
		if raw, ok := stage["$skip"]; ok {
			if n, ok := toInt(raw); ok {
				skip = n
			}
		}
	}
	return
}

// AggregationIntent is a helper used by the adapter.
func AggregationIntent(pipeline []bson.M) string {
	intent := []string{}
	for _, stage := range pipeline {
		if stage["$group"] != nil {
			intent = append(intent, "group")
		}
		if stage["$count"] != nil {
			intent = append(intent, "count")
		}
		if stage["$graphLookup"] != nil {
			intent = append(intent, "graph_lookup")
		}
	}
	if len(intent) == 0 {
		return "normal"
	}
	return strings.Join(intent, ",")
}

// mapKeysSorted is a helper used by the adapter.
func mapKeysSorted(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// toInt is a helper used by the adapter.
func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float32:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

// sanitizeFilterSummary is a helper used by the adapter.
func sanitizeFilterSummary(stage bson.M) []string {
	if stage == nil {
		return nil
	}
	if _, ok := stage["$match"].(bson.M); ok {
		return MatchFieldNames([]bson.M{stage})
	}
	return nil
}
