package errext

import (
	"errors"
	"sync"
)

var (
	globalFallbackLang string
	i18nRegistry       = make(map[Code]map[string]string)
	i18nMu             sync.RWMutex
)

func cloneI18nMap(data map[Code]map[string]string) map[Code]map[string]string {
	if len(data) == 0 {
		return make(map[Code]map[string]string)
	}
	cloned := make(map[Code]map[string]string, len(data))
	for code, translations := range data {
		if len(translations) == 0 {
			cloned[code] = make(map[string]string)
			continue
		}
		copiedTranslations := make(map[string]string, len(translations))
		for lang, msg := range translations {
			copiedTranslations[lang] = msg
		}
		cloned[code] = copiedTranslations
	}
	return cloned
}

// LoadI18nMap replaces the entire i18n translation table atomically.
// fallbackLang is used when the requested language has no translation for a code.
//
// The caller is responsible for deserializing its config source (YAML, JSON, etc.)
// into the flat map structure, keeping this package free of serialization dependencies.
func LoadI18nMap(fallbackLang string, data map[Code]map[string]string) {
	i18nMu.Lock()
	defer i18nMu.Unlock()

	globalFallbackLang = fallbackLang
	i18nRegistry = cloneI18nMap(data)
}

func lookupI18nMsg(code Code, lang string) (string, bool) {
	i18nMu.RLock()
	defer i18nMu.RUnlock()

	translations, ok := i18nRegistry[code]
	if !ok {
		return "", false
	}
	if msg, hit := translations[lang]; hit {
		return msg, true
	}
	if globalFallbackLang != "" {
		if msg, hit := translations[globalFallbackLang]; hit {
			return msg, true
		}
	}
	return "", false
}

// LocalizedMsgByCode returns the localized message for a Code directly,
// without constructing an error or capturing a stack trace.
// Useful for read-only scenarios such as rendering stored codes
// or building static payloads (e.g., streaming events, DB reads).
//
// Resolution follows a 3-level cascade fallback:
//  1. Exact match in i18n registry for (code, lang).
//  2. Fallback language match for (code, globalFallbackLang).
//  3. Default message from Register.
//
// If all lookups miss, empty string is returned.
func LocalizedMsgByCode(code Code, lang string) string {
	if msg, ok := lookupI18nMsg(code, lang); ok {
		return msg
	}

	return lookupEntry(code).msg
}

// LocalizedMsg returns the user-facing message for err in the given language.
// If err is not a BizError, its Error() string is returned as-is.
// See LocalizedMsgByCode for the cascade fallback strategy.
func LocalizedMsg(err error, lang string) string {
	if err == nil {
		return ""
	}
	var bizErr BizError
	if !errors.As(err, &bizErr) {
		return err.Error()
	}
	return LocalizedMsgByCode(bizErr.Code(), lang)
}
