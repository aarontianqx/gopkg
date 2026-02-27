package errext

import (
	"fmt"
	"runtime"
	"strings"
)

const maxStackDepth = 32

// StackTrace holds captured program counters from the call site.
type StackTrace struct {
	pcs []uintptr
}

func captureStack(skip int) *StackTrace {
	var pcs [maxStackDepth]uintptr
	n := runtime.Callers(skip, pcs[:])
	if n == 0 {
		return nil
	}
	return &StackTrace{pcs: pcs[:n]}
}

// String returns the stack trace as a formatted string.
// Format: package.symbol (file:line)
func (st *StackTrace) String() string {
	if st == nil {
		return ""
	}
	var b strings.Builder
	frames := runtime.CallersFrames(st.pcs)
	for {
		frame, more := frames.Next()
		if frame.Function == "" {
			if !more {
				break
			}
			continue
		}
		fmt.Fprintf(&b, "\n\t%s (%s:%d)", compactFunc(frame.Function), frame.File, frame.Line)
		if !more {
			break
		}
	}
	return b.String()
}

func compactFunc(full string) string {
	lastSlash := strings.LastIndexByte(full, '/')
	start := 0
	if lastSlash >= 0 && lastSlash < len(full)-1 {
		start = lastSlash + 1
	}
	dotRel := strings.IndexByte(full[start:], '.')
	if dotRel <= 0 {
		return full
	}
	dot := start + dotRel
	if dot >= len(full)-1 {
		return full
	}
	pkg := full[start:dot]
	symbol := full[dot+1:]
	return pkg + "." + symbol
}
