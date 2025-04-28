package common

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// Recover for needs to rewrite err
func Recover(ctx context.Context, err *error) {
	e := recover()
	if e == nil {
		return
	}
	tmp := fmt.Errorf("%v", e)
	if err != nil {
		*err = tmp
	}
	panicLoc := IdentifyPanicLoc()
	LoggerCtx(ctx).Error("catch panic!!!",
		"error", tmp,
		"panic_location", panicLoc,
		"stacktrace", string(debug.Stack()))
}

// Recovery common recovery function
func Recovery(ctx context.Context) {
	e := recover()
	if e == nil {
		return
	}
	err := fmt.Errorf("%v", e)
	panicLoc := IdentifyPanicLoc()
	LoggerCtx(ctx).Error("catch panic!!!",
		"error", err,
		"panic_location", panicLoc,
		"stacktrace", string(debug.Stack()))
}

// IdentifyPanicLoc get panic location
func IdentifyPanicLoc() string {
	var name, file string
	var line int
	pc := make([]uintptr, 16)

	_ = runtime.Callers(3, pc)
	frames := runtime.CallersFrames(pc)
	for i := 0; i < 16; i++ {
		if frames == nil {
			break
		}
		frame, hasMore := frames.Next()
		fn := runtime.FuncForPC(frame.PC)
		if fn == nil {
			break
		}
		file, line = frame.File, frame.Line
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
		if !hasMore {
			break
		}
	}
	switch {
	case name != "":
		return fmt.Sprintf("%v:%v", name, line)
	case file != "":
		return fmt.Sprintf("%v:%v", file, line)
	}
	return fmt.Sprintf("pc:%x", pc)
}
