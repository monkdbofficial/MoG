package logging

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

const (
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[37m"
	colorReset  = "\033[0m"
)

// Init initializes the global logger.
func Init(logLevel string, logFile string) error {
	// Custom console encoder for colorful, structured output
	consoleConfig := zap.NewProductionEncoderConfig()
	consoleConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleConfig.MessageKey = "msg"
	consoleConfig.LevelKey = "s"
	consoleConfig.TimeKey = "t"
	consoleConfig.CallerKey = "c"
	consoleConfig.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		switch l {
		case zapcore.DebugLevel:
			enc.AppendString("D")
		case zapcore.InfoLevel:
			enc.AppendString("I")
		case zapcore.WarnLevel:
			enc.AppendString("W")
		case zapcore.ErrorLevel:
			enc.AppendString("E")
		default:
			enc.AppendString(l.String())
		}
	}
	consoleConfig.EncodeCaller = zapcore.ShortCallerEncoder
	consoleEncoder := newColorfulConsoleEncoder(consoleConfig)

	// Standard JSON encoder for file output
	jsonConfig := zap.NewProductionEncoderConfig()
	jsonConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonConfig.MessageKey = "msg"
	jsonConfig.LevelKey = "s"
	jsonConfig.TimeKey = "t"
	jsonConfig.CallerKey = "c"
	jsonConfig.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		switch l {
		case zapcore.DebugLevel:
			enc.AppendString("D")
		case zapcore.InfoLevel:
			enc.AppendString("I")
		case zapcore.WarnLevel:
			enc.AppendString("W")
		case zapcore.ErrorLevel:
			enc.AppendString("E")
		default:
			enc.AppendString(l.String())
		}
	}
	jsonConfig.EncodeCaller = zapcore.ShortCallerEncoder
	jsonEncoder := zapcore.NewJSONEncoder(jsonConfig)

	var level zapcore.Level
	switch logLevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	cores := []zapcore.Core{
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	}

	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		cores = append(cores, zapcore.NewCore(jsonEncoder, zapcore.AddSync(f), level))
	}

	core := zapcore.NewTee(cores...)

	logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return nil
}

// colorfulConsoleEncoder is a custom encoder for colorful, structured console output.
type colorfulConsoleEncoder struct {
	zapcore.Encoder
	cfg zapcore.EncoderConfig
}

func newColorfulConsoleEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	return &colorfulConsoleEncoder{
		Encoder: zapcore.NewJSONEncoder(cfg),
		cfg:     cfg,
	}
}

func (e *colorfulConsoleEncoder) Clone() zapcore.Encoder {
	return &colorfulConsoleEncoder{
		Encoder: e.Encoder.Clone(),
		cfg:     e.cfg,
	}
}

func (e *colorfulConsoleEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := buffer.NewPool().Get()

	// Timestamp
	line.WriteString(colorCyan)
	line.WriteString(`"t"`)
	line.WriteString(colorReset)
	line.WriteString(": ")
	line.WriteString(entry.Time.Format("2006-01-02T15:04:05.000Z0700"))
	line.WriteString(", ")

	// Severity
	s := "I"
	color := colorGreen
	switch entry.Level {
	case zapcore.DebugLevel:
		s = "D"
		color = colorGray
	case zapcore.WarnLevel:
		s = "W"
		color = colorYellow
	case zapcore.ErrorLevel:
		s = "E"
		color = colorRed
	}
	line.WriteString(color)
	line.WriteString(`"s"`)
	line.WriteString(colorReset)
	line.WriteString(": ")
	line.WriteString(color)
	line.WriteString(`"`)
	line.WriteString(s)
	line.WriteString(`"`)
	line.WriteString(colorReset)
	line.WriteString(", ")

	// Caller
	if entry.Caller.Defined {
		line.WriteString(colorPurple)
		line.WriteString(`"c"`)
		line.WriteString(colorReset)
		line.WriteString(": ")
		line.WriteString(entry.Caller.TrimmedPath())
		line.WriteString(", ")
	}

	// Message
	line.WriteString(colorBlue)
	line.WriteString(`"msg"`)
	line.WriteString(colorReset)
	line.WriteString(": ")
	line.WriteString(`"`)
	line.WriteString(entry.Message)
	line.WriteString(`"`)

	// Attributes (simplified for speed)
	if len(fields) > 0 {
		line.WriteString(", ")
		line.WriteString(colorYellow)
		line.WriteString(`"attr"`)
		line.WriteString(colorReset)
		line.WriteString(": {")
		for i, f := range fields {
			if i > 0 {
				line.WriteString(", ")
			}
			line.WriteString(colorGray)
			line.WriteString(`"`)
			line.WriteString(f.Key)
			line.WriteString(`"`)
			line.WriteString(colorReset)
			line.WriteString(": ")
			// Use standard JSON encoder for the value to be safe
			valBuf, _ := e.Encoder.EncodeEntry(entry, []zapcore.Field{f})
			// Strip the outer JSON structure if possible or just use the raw value
			line.Write(valBuf.Bytes())
			valBuf.Free()
		}
		line.WriteString("}")
	}

	line.WriteString("}\n")
	return line, nil
}

// Logger returns the global logger.
func Logger() *zap.Logger {
	return logger
}
