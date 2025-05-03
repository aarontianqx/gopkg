package common

import (
	"context"
	"log/slog"

	"github.com/aarontianqx/gopkg/common/logimpl"
)

// defaultLogger is the package-level default
var defaultLogger = logimpl.NewLogger()

// InitLogger initializes the default logger; replaces current default
func InitLogger(opts ...logimpl.Option) {
	defaultLogger = logimpl.NewLogger(opts...)
}

// Logger returns the base slog.Logger of default instance
func Logger() *slog.Logger {
	return defaultLogger.Logger()
}

// LoggerCtx returns a logger with context-extracted attributes
func LoggerCtx(ctx context.Context) *slog.Logger {
	return defaultLogger.LoggerCtx(ctx)
}
