// Package errext provides a pure, protocol-agnostic business error model.
//
// Key features:
//   - Driven by numeric Error Codes with built-in i18n cascade fallback.
//   - Zero-overhead built-in stack trace capture.
//   - Separation of user-facing localized messages (LocalizedMsg) and backend debugging traces (Error/Cause).
package errext

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// Code is an integer business error code.
// Each application defines its own code constants using iota.
//
// Note: It is recommended to avoid using 0 when defining specific business codes,
// as 0 usually represents OK/Success in many RPC systems (e.g., gRPC).
type Code int32

type codeEntry struct {
	msg        string
	statusCode StatusCode
}

var (
	registry   = make(map[Code]codeEntry)
	registryMu sync.RWMutex
)

// Register associates a Code with its default message and options.
// The msg serves as the developer-defined technical fallback when no i18n
// translation is available. It panics on duplicate registration.
// Typically called during package init.
func Register(code Code, msg string, opts ...Option) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[code]; exists {
		panic(fmt.Sprintf("errext: duplicate registration for code %d", code))
	}

	entry := codeEntry{
		msg:        msg,
		statusCode: StatusCodeInternal,
	}
	cfg := &optionConfig{statusCode: &entry.statusCode}
	for _, opt := range opts {
		opt(cfg)
	}
	registry[code] = entry
}

func lookupEntry(code Code) codeEntry {
	registryMu.RLock()
	defer registryMu.RUnlock()
	if entry, ok := registry[code]; ok {
		return entry
	}
	return codeEntry{statusCode: StatusCodeInternal}
}

// BizError is the public interface for all business errors.
//
// Implementations support the standard errors.Is/As/Unwrap chain.
type BizError interface {
	error
	fmt.Formatter

	// Code returns the business error code.
	Code() Code

	// Msg returns the registered default message (technical fallback).
	Msg() string

	// StatusCode returns the protocol-agnostic status code.
	StatusCode() StatusCode

	// Cause returns the underlying wrapped error, if any.
	Cause() error

	// StackTrace returns the captured call stack frames, if any.
	StackTrace() *StackTrace

	// WithMsg returns a copy of this BizError with the default message overridden.
	WithMsg(msg string) BizError
}

// bizError implements BizError.
type bizError struct {
	code       Code
	msg        string
	statusCode StatusCode
	cause      error
	stack      *StackTrace
}

func (e *bizError) Code() Code              { return e.code }
func (e *bizError) Msg() string             { return e.msg }
func (e *bizError) StatusCode() StatusCode  { return e.statusCode }
func (e *bizError) Cause() error            { return e.cause }
func (e *bizError) StackTrace() *StackTrace { return e.stack }

// Error returns the internal debug string for logging.
// Format: [code:msg] cause  or  [code:msg] when no cause.
// This is NOT the user-facing message — use LocalizedMsg for that.
func (e *bizError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("[%d:%s] %v", e.code, e.msg, e.cause)
	}
	return fmt.Sprintf("[%d:%s]", e.code, e.msg)
}

// Unwrap enables standard errors.Is/As chain traversal.
func (e *bizError) Unwrap() error { return e.cause }

// Is matches by business error code so that wrapped copies match the original.
func (e *bizError) Is(target error) bool {
	var t BizError
	if errors.As(target, &t) {
		return e.code == t.Code()
	}
	return false
}

// Format implements fmt.Formatter for stack trace printing.
//
//	%s, %v  → same as Error()
//	%+v     → Error() followed by stack trace
func (e *bizError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, e.Error())
			io.WriteString(s, e.stack.String())
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

// WithMsg returns a shallow copy with the default message overridden.
func (e *bizError) WithMsg(msg string) BizError {
	cp := *e
	cp.msg = msg
	return &cp
}

// --- Option ---

type optionConfig struct {
	statusCode *StatusCode
}

// Option configures metadata during Register.
type Option func(*optionConfig)

// WithStatusCode sets the protocol-agnostic status code for a registered Code.
func WithStatusCode(sc StatusCode) Option {
	return func(c *optionConfig) { *c.statusCode = sc }
}

// --- Constructors ---

// newBizError is the internal constructor. skip controls how many frames
// to skip for stack capture so the stack top always points to the caller.
//
// When the cause chain already contains a BizError, the result is flattened:
//   - The outer Code/Msg/StatusCode win (re-classification).
//   - The original (inner) stack trace is preserved (first fault site).
//   - The inner BizError's own cause becomes the new cause, skipping the
//     intermediate "[code:msg]" text that would otherwise duplicate.
//   - Message strategy is outer-overrides-inner. No contextual text is appended.
func newBizError(code Code, cause error, skip int) *bizError {
	entry := lookupEntry(code)

	var innerBizErr BizError
	if cause != nil && errors.As(cause, &innerBizErr) {
		return &bizError{
			code:       code,
			msg:        entry.msg,
			statusCode: entry.statusCode,
			cause:      innerBizErr.Cause(),
			stack:      innerBizErr.StackTrace(),
		}
	}

	return &bizError{
		code:       code,
		msg:        entry.msg,
		statusCode: entry.statusCode,
		cause:      cause,
		stack:      captureStack(skip),
	}
}

// New creates a BizError for the given code, capturing the current call stack.
// The default message and status code are looked up from the registry.
//
// Note: This method captures a snapshot of the call stack internally, which has a minor performance cost.
// Avoid using errors constructed by this method in hot path control flows (e.g., as a break condition).
func New(code Code) BizError {
	return newBizError(code, nil, 4)
}

// Wrap creates a BizError for the given code, wrapping a cause error
// and capturing the current call stack.
//
// Note: This method captures a snapshot of the call stack internally, which has a minor performance cost.
// Avoid using errors constructed by this method in hot path control flows (e.g., as a break condition).
func Wrap(code Code, cause error) BizError {
	return newBizError(code, cause, 4)
}

// WrapStr creates a BizError for the given code, wrapping a plain string as cause
// and capturing the current call stack. Convenience for on-the-spot error messages.
//
// Note: This method captures a snapshot of the call stack internally, which has a minor performance cost.
// Avoid using errors constructed by this method in hot path control flows (e.g., as a break condition).
func WrapStr(code Code, msg string) BizError {
	return newBizError(code, errors.New(msg), 4)
}

// --- Matching helpers ---

// IsCode reports whether any BizError in the error chain has the given code.
// This is a lightweight check — no stack capture, no allocation.
func IsCode(err error, code Code) bool {
	var be BizError
	if errors.As(err, &be) {
		return be.Code() == code
	}
	return false
}

// IsBizError reports whether any error in the chain is a BizError.
func IsBizError(err error) bool {
	var be BizError
	return errors.As(err, &be)
}

// AsBizError extracts the outermost BizError from the error chain.
// Returns (bizErr, true) if found, or (nil, false) otherwise.
func AsBizError(err error) (BizError, bool) {
	var be BizError
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}
