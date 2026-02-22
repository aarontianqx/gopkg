package common

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"time"
)

// CallerPC walks the call stack, skipping framework and logger wrappers,
// and returns the program counter (PC) of the actual business logic layer.
//
// externalSkipPrefixes: File path prefixes to be skipped during stack tracing (e.g., specific DB wrapper paths).
// skip: The initial number of stack frames to skip.
func CallerPC(externalSkipPrefixes []string, skip int) uintptr {
	const maxDepth = 25
	var pcs [maxDepth]uintptr

	// Capture the current call stack
	n := runtime.Callers(skip, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	for {
		frame, more := frames.Next()
		// Continue until we find a frame that is not in the skip list
		if !isInternalFrame(frame.File, externalSkipPrefixes) {
			return frame.PC
		}
		if !more {
			break
		}
	}
	return 0
}

func isInternalFrame(file string, skipPrefixes []string) bool {
	for _, prefix := range skipPrefixes {
		if strings.Contains(file, prefix) {
			return true
		}
	}
	return false
}

// LogWithPC receives a specific PC address, wraps it in an slog.Record, and
// forces it to be logged, enabling accurate source code tracking.
func LogWithPC(ctx context.Context, pc uintptr, level slog.Level, msg string, attrs ...any) {
	// Abstract logger fetch. Assumes common package provides a base slog logger via context.
	log := LoggerCtx(ctx)
	if !log.Enabled(ctx, level) {
		return
	}

	// Manually construct the Record using the extracted PC to force the underlying Handler
	// to use the actual business code location.
	r := slog.NewRecord(time.Now(), level, msg, pc)
	r.Add(attrs...)

	// Submit the Record to the Handler
	_ = log.Handler().Handle(ctx, r)
}
