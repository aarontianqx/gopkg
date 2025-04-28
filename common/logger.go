package common

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// contextKeyType is a custom type for context keys to avoid collisions
type contextKeyType int

// BaseLogInfo holds common logging attributes managed by this package.
type BaseLogInfo struct {
	RequestID string
	JobName   string
}

// ContextExtractorFunc extracts key-value pairs from a context
type ContextExtractorFunc func(ctx context.Context) []any

// LogConfig holds configuration for the logger
type LogConfig struct {
	// Level sets the minimum level at which logs will be written
	Level string
	// AddSource determines whether to add source file and line info
	AddSource bool
	// Output is where logs are written (defaults to os.Stdout if nil)
	Output io.Writer
	// Format can be "json" or "text"
	Format string
}

const (
	// Context keys
	baseLogInfoKey contextKeyType = iota // Key for BaseLogInfo struct

	// Keys for log attributes
	KeyTraceID   = "traceid"
	KeySpanID    = "spanid"
	KeyRequestID = "requestid"
	KeyJobName   = "jobname"
)

// Initialize default handler and logger
var handler slog.Handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level:     slog.LevelInfo,
	AddSource: true,
})

var logger = slog.New(handler)

// defaultExtractors holds the registered context extractor functions
// Now includes trace info and base log info extractors.
var defaultExtractors = []ContextExtractorFunc{extractBaseLogInfo}

// Init initializes the logger with the given configuration
// This should be called at application startup before using the logger
func Init(config LogConfig) {
	// Configure level
	var level slog.Level
	switch strings.ToLower(config.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	// Use provided output or default to stdout
	output := config.Output
	if output == nil {
		output = os.Stdout
	}

	// Configure handler based on format
	handlerOptions := &slog.HandlerOptions{
		Level:     level,
		AddSource: config.AddSource,
	}

	if strings.ToLower(config.Format) == "text" {
		handler = slog.NewTextHandler(output, handlerOptions)
	} else {
		handler = slog.NewJSONHandler(output, handlerOptions)
	}

	logger = slog.New(handler)
}

// Logger returns the base logger instance
func Logger() *slog.Logger {
	return logger
}

// LoggerCtx returns a logger with context information extracted
func LoggerCtx(ctx context.Context) *slog.Logger {
	return logger.With(extractAllContextInfo(ctx)...)
}

// BaseLogInfoFromContext retrieves the BaseLogInfo from context.
// Returns nil if context is nil, BaseLogInfo is not found, or type assertion fails.
func BaseLogInfoFromContext(ctx context.Context) *BaseLogInfo {
	if ctx == nil {
		return nil
	}

	info, ok := ctx.Value(baseLogInfoKey).(BaseLogInfo)
	if !ok {
		return nil
	}

	// Return a copy of the info as a pointer
	result := new(BaseLogInfo)
	*result = info
	return result
}

// ContextWithBaseLogInfo adds BaseLogInfo to a context.
// It performs a copy-on-write update if info is nil, otherwise directly uses the provided info.
func ContextWithBaseLogInfo(ctx context.Context, info *BaseLogInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, baseLogInfoKey, *info) // Store a copy of the struct
}

// RegisterExtractor adds a context extractor function for logging
func RegisterExtractor(extractor ContextExtractorFunc) {
	// TODO: Consider thread safety if registration happens concurrently after init
	defaultExtractors = append(defaultExtractors, extractor)
}

// extractAllContextInfo runs all registered context extractors
func extractAllContextInfo(ctx context.Context) []any {
	var allArgs []any
	for _, extractor := range defaultExtractors {
		allArgs = append(allArgs, extractor(ctx)...)
	}
	return allArgs
}

// extractBaseLogInfo extracts common attributes from the BaseLogInfo struct in context.
func extractBaseLogInfo(ctx context.Context) []any {
	// Return early if context is nil
	if ctx == nil {
		return nil
	}
	info, ok := ctx.Value(baseLogInfoKey).(BaseLogInfo)
	if !ok {
		return nil
	}

	var ctxArgs []any
	if info.RequestID != "" {
		ctxArgs = append(ctxArgs, KeyRequestID, info.RequestID)
	}
	if info.JobName != "" {
		ctxArgs = append(ctxArgs, KeyJobName, info.JobName)
	}
	return ctxArgs
}
