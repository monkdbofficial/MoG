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
	Stage         string
	Operation     string
	Table         string
	QueryName     string
	QueryTemplate string
	ArgsCount     int
	StartedAt     time.Time
	DBDuration    time.Duration
	ScanDuration  time.Duration
	TotalDuration time.Duration
	Rows          int
	RowsAffected  int64
	TxUsage       string
	Error         error
}

func loadThresholds() {
	thresholdOnce.Do(func() {
		thresholds.query = envIntWithDefault(slowQueryEnv, defaultSlowQueryMS)
		thresholds.scan = envIntWithDefault(slowScanEnv, defaultSlowScanMS)
		thresholds.adapter = envIntWithDefault(slowAdapterEnv, defaultSlowAdapterMS)
	})
}

func envIntWithDefault(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

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

func AdapterDebugEnabled() bool {
	logger := logging.Logger()
	return logger != nil && logger.Core().Enabled(zap.DebugLevel)
}

func AdapterDebug(stage string, fields ...zap.Field) {
	if logger, ok := adapterLogger(); ok {
		all := append([]zap.Field{zap.String("stage", stage)}, fields...)
		logger.Debug("adapter debug", all...)
	}
}

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

func LogQuery(opts QueryLogOptions) {
	if opts.Stage == "" {
		opts.Stage = DBStageName
	}
	if opts.Table == "" {
		opts.Table = defaultQueryName
	}
	totalDuration := opts.TotalDuration
	if totalDuration <= 0 {
		totalDuration = opts.DBDuration + opts.ScanDuration
	}
	if totalDuration <= 0 {
		totalDuration = time.Since(opts.StartedAt)
	}
	loadThresholds()
	dbDurationMs := float64(opts.DBDuration.Milliseconds())
	scanDurationMs := float64(opts.ScanDuration.Milliseconds())
	totalDurationMs := float64(totalDuration.Milliseconds())
	slowQuery := thresholds.query > 0 && int64(thresholds.query) <= int64(opts.DBDuration.Milliseconds())
	slowScan := thresholds.scan > 0 && int64(thresholds.scan) <= int64(opts.ScanDuration.Milliseconds())
	slowAdapter := thresholds.adapter > 0 && int64(thresholds.adapter) <= int64(totalDuration.Milliseconds())

	fields := []zap.Field{
		zap.String("operation", opts.Operation),
		zap.String("table", opts.Table),
		zap.String("query_name", opts.QueryName),
		zap.Int("args_count", opts.ArgsCount),
		zap.Uint64("query_hash", QueryHash(opts.QueryTemplate)),
		zap.Int("rows", opts.Rows),
		zap.Int64("rows_affected", opts.RowsAffected),
		zap.String("tx_usage", opts.TxUsage),
		zap.Float64("db_duration_ms", dbDurationMs),
		zap.Float64("scan_duration_ms", scanDurationMs),
		zap.Float64("total_duration_ms", totalDurationMs),
		zap.Bool("slow_query", slowQuery),
		zap.Bool("slow_scan", slowScan),
		zap.Bool("slow_adapter_call", slowAdapter),
	}

	if AdapterDebugEnabled() {
		AdapterDebug(opts.Stage, append(fields, zap.String("sql", opts.QueryTemplate))...)
		return
	}

	if opts.Error != nil {
		AdapterError(opts.Stage, opts.Error, append(fields, zap.String("sql", opts.QueryTemplate))...)
		return
	}

	if slowQuery || slowScan || slowAdapter {
		AdapterWarn(opts.Stage, append(fields, zap.String("sql", opts.QueryTemplate))...)
	}
}

func QueryHash(sql string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return h.Sum64()
}

func StageNames(pipeline []bson.M) []string {
	names := make([]string, 0, len(pipeline))
	for _, stage := range pipeline {
		for op := range stage {
			names = append(names, strings.TrimPrefix(op, "$"))
		}
	}
	return names
}

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

func mapKeysSorted(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

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

func sanitizeFilterSummary(stage bson.M) []string {
	if stage == nil {
		return nil
	}
	if _, ok := stage["$match"].(bson.M); ok {
		return MatchFieldNames([]bson.M{stage})
	}
	return nil
}
