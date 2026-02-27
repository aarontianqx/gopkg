# errext Package

Provides a pure, protocol-agnostic business error model for Go applications, with built-in i18n cascade fallback and zero-overhead stack trace capture.

## Key Features

- **Numeric Error Codes:** Error types are driven by integer codes (`Code`), avoiding fragile string matching.
- **Protocol-Agnostic StatusCode:** Each code binds a `StatusCode` aligned with gRPC/Connect canonical codes (0–16), with built-in HTTP mapping. No RPC framework dependency.
- **Built-in i18n Cascade Fallback:** `LocalizedMsg` resolves user-facing messages through a 3-level cascade — exact locale → global fallback language → Register default.
- **Zero-Boilerplate Stack Traces:** Captured automatically at the error construction site (`New`, `Wrap`), with precise frame-skipping.
- **Flat BizError Wrapping:** When wrapping an existing `BizError`, the outer code/msg/status wins while the inner (first fault) stack trace is preserved, producing a clean single-layer error.

## Usage

### 1. Code Registration

Define business codes and register their fallback messages during `init`:

```go
package domain

import "github.com/aarontianqx/gopkg/errext"

const (
    CodeUserNotFound errext.Code = 10001
    CodeInvalidAuth  errext.Code = 10002
)

func init() {
    errext.Register(CodeUserNotFound, "user not found", errext.WithStatusCode(errext.StatusCodeNotFound))
    errext.Register(CodeInvalidAuth, "auth expired", errext.WithStatusCode(errext.StatusCodeUnauthenticated))
}
```

### 2. Service Layer

```go
func GetUser(id string) error {
    user, err := db.Find(id)
    if err != nil {
        return errext.Wrap(CodeUserNotFound, err)
    }
    return nil
}
```

### 3. Loading i18n Translations

Load translations at application startup or on config hot-reload. The caller deserializes its own config source (YAML, JSON, config center, etc.) — errext carries no serialization dependency.

```go
errext.LoadI18nMap("en", map[errext.Code]map[string]string{
    10001: {
        "en":    "User not found",
        "zh-CN": "抱歉，您要找的用户不存在。",
        "ja-JP": "ユーザーが存在しません。",
    },
})
```

### 4. Gateway / Middleware

```go
func ErrorMiddleware(c *gin.Context) {
    c.Next()

    if len(c.Errors) > 0 {
        err := c.Errors.Last().Err
        lang := c.GetHeader("Accept-Language")

        if bizErr, ok := errext.AsBizError(err); ok {
            log.Errorf("biz error: %+v", err) // %+v expands full stack trace
            c.JSON(bizErr.StatusCode().HTTPStatus(), gin.H{
                "code": bizErr.Code(),
                "msg":  errext.LocalizedMsg(err, lang),
            })
            return
        }

        log.Errorf("unexpected: %+v", err)
        c.JSON(500, gin.H{"code": -1, "msg": "Service Unavailable"})
    }
}
```

### Cascade Fallback

Both `LocalizedMsg(err, lang)` and `LocalizedMsgByCode(code, lang)` resolve user-facing text through the same 3-level cascade:

| Priority | Source | Example |
|----------|--------|---------|
| 1 | i18n registry: exact `(code, lang)` | `"ユーザーが存在しません。"` |
| 2 | i18n registry: `(code, globalFallbackLang)` | `"User not found"` |
| 3 | `Register` default message | `"user not found"` |

- `LocalizedMsg` accepts an `error` — if it is not a `BizError`, `err.Error()` is returned as-is.
- `LocalizedMsgByCode` accepts a `Code` directly, avoiding error construction and stack capture overhead. Useful for rendering stored codes or building static payloads.
- If all lookups miss, the returned message is empty string.
