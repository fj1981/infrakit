package cylog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Config holds the configuration for the logger.
type Config struct {
	// Filename is the file to write logs to. If empty, writes to stdout.
	Filename string
	// MaxSize is the maximum size in megabytes of the log file before it gets rotated.
	MaxSize int
	// MaxBackups is the maximum number of old log files to retain.
	MaxBackups int
	// MaxAge is the maximum number of days to retain old log files.
	MaxAge int
	// Compress determines if the rotated log files should be compressed.
	Compress bool
	// Level is the minimum level to log.
	Level slog.Level
	// Format is the log output format, "json" or "text".
	Format string
	// CallerSkip is the number of stack frames to skip to find the caller.
	CallerSkip int
	// Writer is the custom writer to write logs to.
	Writer io.Writer
	// AddSource is whether to add source information to the log.
	AddSource bool
}

// Option defines a function that configures the logger.
type Option func(*Config)

// WithFilename sets the log filename.
func WithFilename(filename string) Option {
	return func(c *Config) { c.Filename = filename }
}

// WithMaxSize sets the maximum size of the log file in megabytes.
func WithMaxSize(maxSize int) Option {
	return func(c *Config) { c.MaxSize = maxSize }
}

// WithMaxBackups sets the maximum number of old log files to retain.
func WithMaxBackups(maxBackups int) Option {
	return func(c *Config) { c.MaxBackups = maxBackups }
}

// WithMaxAge sets the maximum number of days to retain old log files.
func WithMaxAge(maxAge int) Option {
	return func(c *Config) { c.MaxAge = maxAge }
}

// WithCompress enables or disables log compression.
func WithCompress(compress bool) Option {
	return func(c *Config) { c.Compress = compress }
}

// WithLevel sets the logging level.
func WithLevel(level slog.Level) Option {
	return func(c *Config) { c.Level = level }
}

func WithLevelStr(level string) Option {
	return func(c *Config) { c.Level = LevelFromStr(level) }
}

// WithFormat sets the log output format.
func WithFormat(format string) Option {
	return func(c *Config) { c.Format = format }
}

// WithCallerSkip sets the number of stack frames to skip.
func WithCallerSkip(skip int) Option {
	return func(c *Config) { c.CallerSkip = skip }
}

// WithWriter sets the custom writer for the logger.
func WithWriter(writer io.Writer) Option {
	return func(c *Config) { c.Writer = writer }
}

// WithAddSource sets whether to add source information to the log.
func WithAddSource(addSource bool) Option {
	return func(c *Config) { c.AddSource = addSource }
}

// Logger wraps the slog.Logger.
type Logger struct {
	*slog.Logger
}

// defaultConfig holds the default configuration for loggers
var defaultConfig = &Config{
	Level:     slog.LevelInfo,
	Format:    "apache",
	AddSource: true,
}

// defaultLogger is the default logger instance
var defaultLogger = newFromConfig(defaultConfig)

// copyConfig creates a copy of the source config
func copyConfig(src *Config) *Config {
	return &Config{
		Level:      src.Level,
		Format:     src.Format,
		Filename:   src.Filename,
		MaxSize:    src.MaxSize,
		MaxBackups: src.MaxBackups,
		MaxAge:     src.MaxAge,
		Compress:   src.Compress,
		CallerSkip: src.CallerSkip,
		Writer:     src.Writer,
		AddSource:  src.AddSource,
	}
}

// New creates a new Logger instance with the given options.
func New(opts ...Option) *Logger {
	// Create a copy of the default config
	cfg := copyConfig(defaultConfig)

	// Apply the provided options to the copied config
	for _, opt := range opts {
		opt(cfg)
	}

	return newFromConfig(cfg)
}

// newFromConfig creates a new Logger from a Config
func newFromConfig(cfg *Config) *Logger {
	var writer io.Writer
	if cfg.Writer != nil {
		writer = cfg.Writer
	} else if cfg.Filename != "" {
		writer = &lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
	} else {
		writer = os.Stdout
	}

	hOpts := &slog.HandlerOptions{
		Level:     cfg.Level,
		AddSource: cfg.AddSource,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// If the user wants to skip caller frames, we handle it here.
			if a.Key == slog.SourceKey && cfg.AddSource {
				// The base skip count is 8: runtime.Caller, this func, slog handler, slog logger...
				// This is more stable than a custom handler.
				pc, file, line, ok := runtime.Caller(4 + cfg.CallerSkip)
				if ok {
					fn := runtime.FuncForPC(pc)
					return slog.String(a.Key, fmt.Sprintf("%s(%d),%s", filepath.Base(file), line, filepath.Base(fn.Name())))
				}
			}
			return a
		},
	}

	var handler slog.Handler
	switch cfg.Format {
	case "json":
		handler = slog.NewJSONHandler(writer, hOpts)
	case "apache":
		handler = NewApacheHandler(writer, hOpts)
	default:
		handler = slog.NewTextHandler(writer, hOpts)
	}

	return &Logger{slog.New(handler)}
}

func LevelFromStr(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// InitDefault initializes the default package-level logger and updates the default configuration.
func InitDefault(opts ...Option) {
	// Apply options directly to the default config
	for _, opt := range opts {
		opt(defaultConfig)
	}

	// Create a new default logger with the updated config
	defaultLogger = newFromConfig(defaultConfig)
	slog.SetDefault(defaultLogger.Logger)
}

// Default returns the default logger.
func Default() *Logger {
	return defaultLogger
}

var skipLogger sync.Map

func Skip(skip int) *Logger {
	if v, ok := skipLogger.Load(skip); ok {
		return v.(*Logger)
	}
	var l *Logger
	if skip <= 0 {
		l = New(WithAddSource(false))
	} else {
		l = New(WithCallerSkip(skip))
	}
	skipLogger.Store(skip, l)
	return l
}

func innerLog() *Logger {
	return Skip(1)
}

type printfAttr struct {
	format string
	args   []any
}

func (a printfAttr) LogValue() slog.Value {
	// 只在真正要输出时才 Sprintf，避免在 Debug 关闭时仍做格式化
	return slog.StringValue(fmt.Sprintf(a.format, a.args...))
}
func (l *Logger) Infof(format string, args ...any) {
	l.LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}

func (l *Logger) Debugf(format string, args ...any) {
	l.LogAttrs(context.Background(), slog.LevelDebug, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.LogAttrs(context.Background(), slog.LevelWarn, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}
func (l *Logger) Errorf(format string, args ...any) error {
	pa := printfAttr{format, args}
	l.LogAttrs(context.Background(), slog.LevelError, "",
		slog.Any("msg", pa),
	)
	return errors.New(pa.LogValue().String())
}

// Infof 既像 fmt.Printf，又像 log/slog 一样结构化
func Infof(format string, args ...any) {
	innerLog().LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}
func Debugf(format string, args ...any) {
	innerLog().LogAttrs(context.Background(), slog.LevelDebug, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}
func Warnf(format string, args ...any) {
	innerLog().LogAttrs(context.Background(), slog.LevelWarn, "",
		slog.Any("msg", printfAttr{format, args}),
	)
}
func Errorf(format string, args ...any) error {
	pa := printfAttr{format, args}
	innerLog().LogAttrs(context.Background(), slog.LevelError, "",
		slog.Any("msg", pa),
	)
	return errors.New(pa.LogValue().String())
}

func Info(msg string, attrs ...any) {
	innerLog().Log(context.Background(), slog.LevelInfo, msg, attrs...)
}
func Debug(msg string, attrs ...any) {
	innerLog().Log(context.Background(), slog.LevelDebug, msg, attrs...)
}
func Warn(msg string, attrs ...any) {
	innerLog().Log(context.Background(), slog.LevelWarn, msg, attrs...)
}
func Error(msg string, attrs ...any) error {
	innerLog().Log(context.Background(), slog.LevelError, msg, attrs...)

	// Include attributes in the error message
	if len(attrs) > 0 {
		// Format attributes similar to how slog formats them
		var sb strings.Builder
		sb.WriteString(msg)
		sb.WriteString(" ")

		for i := 0; i < len(attrs); i += 2 {
			if i+1 < len(attrs) {
				key, ok := attrs[i].(string)
				if ok {
					sb.WriteString(key)
					sb.WriteString("=")
					sb.WriteString(fmt.Sprint(attrs[i+1]))
					sb.WriteString(" ")
				}
			}
		}
		return errors.New(sb.String())
	}

	return errors.New(msg)
}
