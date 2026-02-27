package errext

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

const (
	CodeUserNotFound     Code = 10001
	CodePermissionDenied Code = 10002
	CodeInternalError    Code = 10003
	CodeInvalidRequest   Code = 10004
)

func init() {
	Register(CodeUserNotFound, "用户不存在", WithStatusCode(StatusCodeNotFound))
	Register(CodePermissionDenied, "无权限访问", WithStatusCode(StatusCodePermissionDenied))
	Register(CodeInternalError, "服务繁忙", WithStatusCode(StatusCodeInternal))
	Register(CodeInvalidRequest, "请求参数无效", WithStatusCode(StatusCodeInvalidArgument))
}

// --- Register ---

func TestRegisterPanicsOnDuplicate(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("duplicate Register should panic")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "duplicate") {
			t.Fatalf("panic message should mention duplicate, got: %s", msg)
		}
	}()
	Register(CodeUserNotFound, "duplicate")
}

// --- New ---

func TestNewFromCode(t *testing.T) {
	live := New(CodeUserNotFound)
	if live.Code() != CodeUserNotFound {
		t.Fatalf("expected code %d, got %d", CodeUserNotFound, live.Code())
	}
	if live.Msg() != "用户不存在" {
		t.Fatalf("expected msg from registry, got %s", live.Msg())
	}
	if live.StatusCode() != StatusCodeNotFound {
		t.Fatalf("expected StatusCodeNotFound, got %v", live.StatusCode())
	}
	if live.StackTrace() == nil {
		t.Fatal("New() should capture stack trace")
	}
	if live.Cause() != nil {
		t.Fatal("New() should have no cause")
	}
}

func TestNewUnregisteredCode(t *testing.T) {
	const unregistered Code = 99999
	live := New(unregistered)
	if live.Code() != unregistered {
		t.Fatalf("expected code %d, got %d", unregistered, live.Code())
	}
	if live.Msg() != "" {
		t.Fatalf("unregistered code should have empty msg, got: %s", live.Msg())
	}
	if live.StatusCode() != StatusCodeInternal {
		t.Fatalf("unregistered code should default to Internal, got: %v", live.StatusCode())
	}
}

// --- Error() format ---

func TestErrorOutputWithCause(t *testing.T) {
	live := Wrap(CodeUserNotFound, fmt.Errorf("db error"))
	errStr := live.Error()
	// Should be [10001:用户不存在] db error
	if !strings.Contains(errStr, "10001") {
		t.Fatalf("Error() should contain code, got: %s", errStr)
	}
	if !strings.Contains(errStr, "用户不存在") {
		t.Fatalf("Error() should contain msg for debugging, got: %s", errStr)
	}
	if !strings.Contains(errStr, "db error") {
		t.Fatalf("Error() should contain cause, got: %s", errStr)
	}
}

func TestErrorOutputWithoutCause(t *testing.T) {
	live := New(CodeUserNotFound)
	errStr := live.Error()
	// Should be [10001:用户不存在]
	if !strings.Contains(errStr, "10001") || !strings.Contains(errStr, "用户不存在") {
		t.Fatalf("Error() without cause should show code:msg, got: %s", errStr)
	}
}

// --- Wrap ---

func TestWrapWithCause(t *testing.T) {
	dbErr := fmt.Errorf("pq: duplicate key")
	live := Wrap(CodeUserNotFound, dbErr)

	if live.StackTrace() == nil {
		t.Fatal("Wrap() should capture stack trace")
	}
	if live.Cause() != dbErr {
		t.Fatalf("cause mismatch: got %v", live.Cause())
	}
}

func TestWrapNilCause(t *testing.T) {
	live := Wrap(CodeUserNotFound, nil)
	if live.StackTrace() == nil {
		t.Fatal("Wrap(nil) should still capture stack")
	}
	if live.Cause() != nil {
		t.Fatal("Wrap(nil) should have no cause")
	}
}

// --- WrapStr ---

func TestWrapStr(t *testing.T) {
	live := WrapStr(CodeUserNotFound, "user 12345 not found in db")
	if live.Cause() == nil {
		t.Fatal("WrapStr should create a cause")
	}
	if live.Cause().Error() != "user 12345 not found in db" {
		t.Fatalf("cause should be the string, got: %v", live.Cause())
	}
	if live.StackTrace() == nil {
		t.Fatal("WrapStr should capture stack")
	}
}

// Verify WrapStr stack points to the caller, not inside errext internals.
func TestWrapStrStackPointsToCaller(t *testing.T) {
	_, _, callerLine, _ := runtime.Caller(0)
	live := WrapStr(CodeUserNotFound, "test") // callerLine + 1

	st := live.StackTrace()
	if st == nil {
		t.Fatal("should have stack")
	}
	stackStr := st.String()
	expected := fmt.Sprintf("verify_test.go:%d", callerLine+1)
	if !strings.Contains(stackStr, expected) {
		t.Fatalf("stack should point to caller line %s, got:\n%s", expected, stackStr)
	}
}

// Same check for New.
func TestNewStackPointsToCaller(t *testing.T) {
	_, _, callerLine, _ := runtime.Caller(0)
	live := New(CodeUserNotFound) // callerLine + 1

	st := live.StackTrace()
	if st == nil {
		t.Fatal("should have stack")
	}
	stackStr := st.String()
	expected := fmt.Sprintf("verify_test.go:%d", callerLine+1)
	if !strings.Contains(stackStr, expected) {
		t.Fatalf("stack should point to caller line %s, got:\n%s", expected, stackStr)
	}
}

// Same check for Wrap.
func TestWrapStackPointsToCaller(t *testing.T) {
	_, _, callerLine, _ := runtime.Caller(0)
	live := Wrap(CodeUserNotFound, fmt.Errorf("err")) // callerLine + 1

	st := live.StackTrace()
	if st == nil {
		t.Fatal("should have stack")
	}
	stackStr := st.String()
	expected := fmt.Sprintf("verify_test.go:%d", callerLine+1)
	if !strings.Contains(stackStr, expected) {
		t.Fatalf("stack should point to caller line %s, got:\n%s", expected, stackStr)
	}
}

// --- WithMsg ---

func TestWithMsg(t *testing.T) {
	live := New(CodeUserNotFound)
	custom := live.WithMsg("自定义提示")

	if custom.Msg() != "自定义提示" {
		t.Fatalf("WithMsg should override, got: %s", custom.Msg())
	}
	if live.Msg() != "用户不存在" {
		t.Fatalf("original should be unchanged, got: %s", live.Msg())
	}
	if custom.Code() != CodeUserNotFound {
		t.Fatalf("code should be preserved, got: %d", custom.Code())
	}
	if custom.StackTrace() == nil {
		t.Fatal("WithMsg should preserve stack")
	}
}

func TestWithMsgChained(t *testing.T) {
	live := Wrap(CodeInvalidRequest, fmt.Errorf("field 'name' is required")).
		WithMsg("请填写姓名")

	if live.Msg() != "请填写姓名" {
		t.Fatalf("chained WithMsg should override, got: %s", live.Msg())
	}
	if live.Code() != CodeInvalidRequest {
		t.Fatalf("code should be preserved, got: %d", live.Code())
	}
	if live.Cause() == nil {
		t.Fatal("cause should be preserved")
	}
}

// --- errors.Is / errors.As ---

func TestErrorsIs(t *testing.T) {
	a := New(CodeUserNotFound)
	b := New(CodeUserNotFound)
	if !errors.Is(a, b) {
		t.Fatal("errors.Is should match same code")
	}
	c := New(CodePermissionDenied)
	if errors.Is(a, c) {
		t.Fatal("errors.Is should not match different code")
	}
}

func TestErrorsIsWithFmtErrfWrapping(t *testing.T) {
	live := New(CodeUserNotFound)
	wrapped := fmt.Errorf("get user: %w", live)
	target := New(CodeUserNotFound)
	if !errors.Is(wrapped, target) {
		t.Fatal("errors.Is should traverse fmt.Errorf chain")
	}
}

func TestErrorsAs(t *testing.T) {
	live := Wrap(CodeUserNotFound, fmt.Errorf("db error"))
	wrapped := fmt.Errorf("handler: %w", live)

	var be BizError
	if !errors.As(wrapped, &be) {
		t.Fatal("errors.As should find BizError in chain")
	}
	if be.Code() != CodeUserNotFound {
		t.Fatalf("expected CodeUserNotFound, got %d", be.Code())
	}
}

// --- IsCode ---

func TestIsCode(t *testing.T) {
	live := Wrap(CodeUserNotFound, fmt.Errorf("db error"))
	wrapped := fmt.Errorf("handler: %w", live)

	if !IsCode(wrapped, CodeUserNotFound) {
		t.Fatal("IsCode should find code in chain")
	}
	if IsCode(wrapped, CodePermissionDenied) {
		t.Fatal("IsCode should not match different code")
	}
	if IsCode(fmt.Errorf("plain"), CodeUserNotFound) {
		t.Fatal("IsCode should return false for non-BizError")
	}
	if IsCode(nil, CodeUserNotFound) {
		t.Fatal("IsCode(nil) should return false")
	}
}

// --- IsBizError ---

func TestIsBizError(t *testing.T) {
	live := New(CodeUserNotFound)
	if !IsBizError(live) {
		t.Fatal("IsBizError should return true")
	}
	if !IsBizError(fmt.Errorf("wrapped: %w", live)) {
		t.Fatal("IsBizError should traverse chain")
	}
	if IsBizError(fmt.Errorf("plain error")) {
		t.Fatal("IsBizError should return false for plain errors")
	}
	if IsBizError(nil) {
		t.Fatal("IsBizError(nil) should return false")
	}
}

// --- Format ---

func TestFormatVerbosePrintsStack(t *testing.T) {
	live := Wrap(CodeUserNotFound, fmt.Errorf("db timeout"))
	output := fmt.Sprintf("%+v", live)

	if !strings.Contains(output, "10001") {
		t.Fatalf("verbose output should contain code, got: %s", output)
	}
	if !strings.Contains(output, "db timeout") {
		t.Fatalf("verbose output should contain cause, got: %s", output)
	}
	if !strings.Contains(output, "verify_test.go:") {
		t.Fatalf("verbose output should contain stack frames with file path, got: %s", output)
	}
	if !strings.Contains(output, "verify_test.go") {
		t.Fatalf("verbose output should reference this test file, got: %s", output)
	}
}

func TestFormatRegularDoesNotPrintStack(t *testing.T) {
	live := New(CodeUserNotFound)
	regular := fmt.Sprintf("%v", live)
	verbose := fmt.Sprintf("%+v", live)

	if strings.Contains(regular, "verify_test.go:") {
		t.Fatalf("regular format should not contain stack, got: %s", regular)
	}
	if !strings.Contains(verbose, "verify_test.go:") {
		t.Fatalf("verbose format should contain stack, got: %s", verbose)
	}
}

// --- StatusCode ---

func TestStatusCodeHTTPMapping(t *testing.T) {
	cases := []struct {
		sc   StatusCode
		http int
	}{
		{StatusCodeOK, 200},
		{StatusCodeNotFound, 404},
		{StatusCodeUnauthenticated, 401},
		{StatusCodePermissionDenied, 403},
		{StatusCodeInternal, 500},
		{StatusCodeInvalidArgument, 400},
		{StatusCodeResourceExhausted, 429},
	}
	for _, c := range cases {
		if got := c.sc.HTTPStatus(); got != c.http {
			t.Errorf("StatusCode(%d).HTTPStatus() = %d, want %d", c.sc, got, c.http)
		}
	}
}

// --- i18n / LocalizedMsg ---

func loadTestI18n() {
	LoadI18nMap("en", map[Code]map[string]string{
		CodeUserNotFound: {
			"en":    "User not found",
			"zh-CN": "抱歉，您要找的用户不存在。",
			"ja-JP": "ユーザーが存在しません。",
		},
		CodePermissionDenied: {
			"en": "Permission denied",
		},
	})
}

func resetI18n() {
	LoadI18nMap("", nil)
}

func TestLocalizedMsg_ExactMatch(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	msg := LocalizedMsg(New(CodeUserNotFound), "ja-JP")
	if msg != "ユーザーが存在しません。" {
		t.Fatalf("expected exact ja-JP translation, got: %s", msg)
	}
}

func TestLocalizedMsg_FallbackLang(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	msg := LocalizedMsg(New(CodeUserNotFound), "ko-KR")
	if msg != "User not found" {
		t.Fatalf("expected fallback to globalFallbackLang (en), got: %s", msg)
	}
}

func TestLocalizedMsg_RegisterFallback(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	msg := LocalizedMsg(New(CodeInternalError), "en")
	if msg != "服务繁忙" {
		t.Fatalf("expected Register fallback msg, got: %s", msg)
	}
}

func TestLocalizedMsg_NoI18nLoaded(t *testing.T) {
	msg := LocalizedMsg(New(CodeUserNotFound), "en")
	if msg != "用户不存在" {
		t.Fatalf("without i18n data, should fallback to Register msg, got: %s", msg)
	}
}

func TestLocalizedMsg_UnregisteredCode(t *testing.T) {
	const unknown Code = 99998
	msg := LocalizedMsg(New(unknown), "en")
	expected := ""
	if msg != expected {
		t.Fatalf("expected %q, got: %s", expected, msg)
	}
}

func TestLocalizedMsg_NonBizError(t *testing.T) {
	plain := fmt.Errorf("disk full")
	msg := LocalizedMsg(plain, "en")
	if msg != "disk full" {
		t.Fatalf("non-BizError should return Error(), got: %s", msg)
	}
}

func TestLocalizedMsg_NilError(t *testing.T) {
	msg := LocalizedMsg(nil, "en")
	if msg != "" {
		t.Fatalf("nil error should return empty string, got: %s", msg)
	}
}

func TestLocalizedMsg_WrappedBizError(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	inner := New(CodeUserNotFound)
	wrapped := fmt.Errorf("handler: %w", inner)
	msg := LocalizedMsg(wrapped, "zh-CN")
	if msg != "抱歉，您要找的用户不存在。" {
		t.Fatalf("should traverse error chain, got: %s", msg)
	}
}

// --- LocalizedMsgByCode ---

func TestLocalizedMsgByCode_ExactMatch(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	msg := LocalizedMsgByCode(CodeUserNotFound, "zh-CN")
	if msg != "抱歉，您要找的用户不存在。" {
		t.Fatalf("expected exact zh-CN, got: %s", msg)
	}
}

func TestLocalizedMsgByCode_FallbackLang(t *testing.T) {
	loadTestI18n()
	defer resetI18n()

	msg := LocalizedMsgByCode(CodeUserNotFound, "ko-KR")
	if msg != "User not found" {
		t.Fatalf("expected fallback to en, got: %s", msg)
	}
}

func TestLocalizedMsgByCode_RegisterFallback(t *testing.T) {
	msg := LocalizedMsgByCode(CodeUserNotFound, "en")
	if msg != "用户不存在" {
		t.Fatalf("without i18n, should fallback to Register msg, got: %s", msg)
	}
}

func TestLocalizedMsgByCode_UnregisteredCode(t *testing.T) {
	const unknown Code = 99997
	msg := LocalizedMsgByCode(unknown, "en")
	expected := ""
	if msg != expected {
		t.Fatalf("expected %q, got: %s", expected, msg)
	}
}

func TestLoadI18nMap_DeepCopyIsolation(t *testing.T) {
	data := map[Code]map[string]string{
		CodeUserNotFound: {
			"en": "User not found",
		},
	}
	LoadI18nMap("en", data)
	defer resetI18n()

	// Mutate caller-owned map after loading; errext should keep its own snapshot.
	data[CodeUserNotFound]["en"] = "MUTATED"
	if msg := LocalizedMsgByCode(CodeUserNotFound, "en"); msg != "User not found" {
		t.Fatalf("i18n registry should be isolated from caller mutations, got: %s", msg)
	}
}

// --- AsBizError ---

func TestAsBizError(t *testing.T) {
	live := New(CodeUserNotFound)
	wrapped := fmt.Errorf("handler: %w", live)

	be, ok := AsBizError(wrapped)
	if !ok {
		t.Fatal("AsBizError should find BizError in chain")
	}
	if be.Code() != CodeUserNotFound {
		t.Fatalf("expected CodeUserNotFound, got %d", be.Code())
	}

	_, ok = AsBizError(fmt.Errorf("plain"))
	if ok {
		t.Fatal("AsBizError should return false for plain error")
	}

	_, ok = AsBizError(nil)
	if ok {
		t.Fatal("AsBizError(nil) should return false")
	}
}

// --- Middleware-style errors.As usage (replaces old FromError tests) ---

func TestMiddlewareExtractsOutermostBizError(t *testing.T) {
	dbErr := fmt.Errorf("record not found")
	inner := Wrap(CodeUserNotFound, dbErr)
	mid := fmt.Errorf("fetch user profile: %w", inner)
	outer := Wrap(CodeInternalError, mid)

	// Middleware uses errors.As → gets outermost (flattened) BizError
	var be BizError
	if !errors.As(outer, &be) {
		t.Fatal("errors.As should find BizError")
	}
	if be.Code() != CodeInternalError {
		t.Fatalf("should get outermost code, got %d", be.Code())
	}
	if be.Msg() != "服务繁忙" {
		t.Fatalf("should get outermost msg, got %s", be.Msg())
	}

	// Cause is flattened to the innermost non-BizError cause
	if be.Cause() != dbErr {
		t.Fatalf("cause should be flattened to innermost, got %v", be.Cause())
	}

	// Stack is inherited from the inner BizError (first fault site)
	if be.StackTrace() != inner.StackTrace() {
		t.Fatal("stack should be inherited from inner BizError")
	}

	// Error() output should be flat — only one [code:msg] prefix
	errStr := outer.Error()
	if strings.Count(errStr, "[") != 1 {
		t.Fatalf("flattened Error() should have exactly one code prefix, got: %s", errStr)
	}
}
