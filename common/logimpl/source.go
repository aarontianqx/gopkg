package logimpl

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
)

// sourceFormatter wraps a handler to emit file:line in "source" attr.
type sourceFormatter struct{ h slog.Handler }

func (sf *sourceFormatter) Enabled(ctx context.Context, lvl slog.Level) bool {
	return sf.h.Enabled(ctx, lvl)
}

func (sf *sourceFormatter) Handle(ctx context.Context, r slog.Record) error {
	if r.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{r.PC})
		frame, _ := frames.Next()
		// Use full file path as slog does, then combine with line
		src := fmt.Sprintf("%s:%d", frame.File, frame.Line)
		r.AddAttrs(slog.String("source", src))
	}
	return sf.h.Handle(ctx, r)
}

func (sf *sourceFormatter) WithAttrs(as []slog.Attr) slog.Handler {
	return &sourceFormatter{h: sf.h.WithAttrs(as)}
}

func (sf *sourceFormatter) WithGroup(name string) slog.Handler {
	return &sourceFormatter{h: sf.h.WithGroup(name)}
}
