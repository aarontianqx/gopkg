package logimpl

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Logger wraps slog.Logger plus extractor functions
// and supports functional options for configuration
type Logger struct {
	slogger    *slog.Logger
	extractors []ContextExtractorFunc
}

func (l *Logger) Logger() *slog.Logger {
	return l.slogger
}

func (l *Logger) LoggerCtx(ctx context.Context) *slog.Logger {
	return l.slogger.With(l.extractAll(ctx)...)
}

// extractAllRunner
func (l *Logger) extractAll(ctx context.Context) []any {
	var args []any
	for _, ex := range l.extractors {
		args = append(args, ex(ctx)...)
	}
	return args
}

// Option defines a configuration option for Logger
type Option func(*LoggerBuilder)

// LoggerBuilder holds interim configuration
// to build a Logger
type LoggerBuilder struct {
	level      slog.Level
	output     io.Writer
	format     string
	addSource  bool
	extractors []ContextExtractorFunc
}

// NewLogger creates a Logger with given options
func NewLogger(opts ...Option) *Logger {
	// set defaults
	b := &LoggerBuilder{
		level:      slog.LevelInfo,
		output:     os.Stdout,
		format:     "json",
		addSource:  true,
		extractors: defaultExtractors,
	}
	// apply options
	for _, opt := range opts {
		opt(b)
	}
	// build handler
	hOpts := &slog.HandlerOptions{
		Level:     b.level,
		AddSource: false, // we'll handle source via wrapper
	}
	var h slog.Handler
	switch strings.ToLower(b.format) {
	case "text":
		h = slog.NewTextHandler(b.output, hOpts)
	default:
		h = slog.NewJSONHandler(b.output, hOpts)
	}
	if b.addSource {
		h = &sourceFormatter{h: h}
	}
	// create slog.Logger
	sl := slog.New(h)
	return &Logger{slogger: sl, extractors: b.extractors}
}

// Option helpers:

// WithLevel sets the log level
func WithLevel(level slog.Level) Option {
	return func(b *LoggerBuilder) {
		b.level = level
	}
}

// WithOutput sets the io.Writer output
func WithOutput(w io.Writer) Option {
	return func(b *LoggerBuilder) { b.output = w }
}

// WithFormat sets the log format ("json" or "text")
func WithFormat(fmtStr string) Option {
	return func(b *LoggerBuilder) { b.format = fmtStr }
}

// WithAddSource enables or disables source formatting
func WithAddSource(enable bool) Option {
	return func(b *LoggerBuilder) { b.addSource = enable }
}

// WithExtractor registers additional ContextExtractorFunc for this Logger
func WithExtractor(ex ContextExtractorFunc) Option {
	return func(b *LoggerBuilder) {
		b.extractors = append(b.extractors, ex)
	}
}
