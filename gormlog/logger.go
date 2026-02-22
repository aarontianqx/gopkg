package gormlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Logger implements gorm's logger.Interface.
//
// Core logic: Utilizes a prefix-based stack trace filtering mechanism to automatically skip
// GORM internal calls and the current logger wrapper layer. This accurately captures the
// business code location triggering the DB operation and generates slog records with the
// correct source file and line number.
type Logger struct {
	SlowThreshold         time.Duration
	SkipErrRecordNotFound bool
	SkipErrDuplicatedKey  bool
	TraceSQL              bool
}

// Ensure Logger implements the gorm logger interface
var _ logger.Interface = (*Logger)(nil)

// callerSkipPrefixes acts as a whitelist mechanism: any file path containing these strings
// will be explicitly skipped during source tracing.
var callerSkipPrefixes = []string{
	"gorm.io/", // Built-in rule to intercept gorm framework code
}

func init() {
	// Dynamically retrieve the absolute path of the current logger.go file using runtime.Caller.
	// Add it as an "intercept feature" to the whitelist. This ensures that even when used
	// as an independent module, it can automatically filter its own wrapper stack layers.
	_, file, _, ok := runtime.Caller(0)
	if ok {
		callerSkipPrefixes = append(callerSkipPrefixes, file)
	}
}

// callerPC walks the call stack, skipping framework and current component layers,
// and returns the program counter (PC) corresponding to the business application layer.
func callerPC(skip int) uintptr {
	const maxDepth = 25
	var pcs [maxDepth]uintptr
	n := runtime.Callers(skip, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	for {
		frame, more := frames.Next()
		if !isInternalFrame(frame.File) {
			return frame.PC
		}
		if !more {
			break
		}
	}
	return 0
}

func isInternalFrame(file string) bool {
	for _, prefix := range callerSkipPrefixes {
		if strings.Contains(file, prefix) {
			return true
		}
	}
	return false
}

// logWithCaller retrieves the actual PC address, wraps it in an slog.Record,
// and directly dispatches it.
func logWithCaller(ctx context.Context, level slog.Level, msg string, attrs ...any) {
	// Retrieve the context logger for business use
	log := common.LoggerCtx(ctx)
	if !log.Enabled(ctx, level) {
		return
	}

	// skip = 3: runtime.Callers -> callerPC -> logWithCaller -> Info/Warn/Error/Trace
	pc := callerPC(3)

	r := slog.NewRecord(time.Now(), level, msg, pc)
	r.Add(attrs...)

	_ = log.Handler().Handle(ctx, r)
}

// LogMode log mode
func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

// Info print info
func (l *Logger) Info(ctx context.Context, msg string, data ...interface{}) {
	logWithCaller(ctx, slog.LevelInfo, fmt.Sprintf(msg, data...))
}

// Warn print warn messages
func (l *Logger) Warn(ctx context.Context, msg string, data ...interface{}) {
	logWithCaller(ctx, slog.LevelWarn, fmt.Sprintf(msg, data...))
}

// Error print error messages
func (l *Logger) Error(ctx context.Context, msg string, data ...interface{}) {
	logWithCaller(ctx, slog.LevelError, fmt.Sprintf(msg, data...))
}

// Trace print sql message
func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	elapsedMs := float64(elapsed) / float64(time.Millisecond)
	sql, rows := fc()

	switch {
	case err != nil &&
		(!errors.Is(err, gorm.ErrRecordNotFound) || !l.SkipErrRecordNotFound) &&
		(!errors.Is(err, gorm.ErrDuplicatedKey) || !l.SkipErrDuplicatedKey):
		logWithCaller(ctx, slog.LevelError, "gorm trace", "err", err, "elapsed_ms", elapsedMs, "rows", rows, "sql", sql)
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0:
		logWithCaller(ctx, slog.LevelWarn, "gorm slow query", "elapsed_ms", elapsedMs, "threshold", l.SlowThreshold, "rows", rows, "sql", sql)
	case l.TraceSQL:
		logWithCaller(ctx, slog.LevelDebug, "gorm trace", "elapsed_ms", elapsedMs, "rows", rows, "sql", sql)
	}
}
