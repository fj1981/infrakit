package cygin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/gin-gonic/gin"
)

/* ---------- 错误码 ---------- */
const (
	ErrCodeInternal     = 101001
	ErrCodeNotFound     = 101404
	ErrCodeInvalidParam = 201001
)

/* ---------- 错误结构 ---------- */
type Error struct {
	Code    int      // Read-only: error code
	Details []string // Read-only: detailed context
	Status  int      // Read-only: HTTP status
}

// Msg returns message in specified language (e.g. "zh", "en")
func (e *Error) Msg(lang string) string {
	lang = normLang(lang)

	// 1. Check override
	overrideMu.RLock()
	defer overrideMu.RUnlock()
	if fn, ok := overrideMsg[e.Code]; ok && fn != nil {
		return fn(lang)
	}

	// 2. Check built-in registry
	if info, ok := registry[e.Code]; ok && info.Message != nil {
		return info.Message(lang)
	}
	return "unknown error"
}

// WithDetailf returns a new Error with formatted detail
func (e *Error) WithDetailf(format string, a ...any) *Error {
	e2 := *e
	e2.Details = append(e2.Details, fmt.Sprintf(format, a...))
	return &e2
}

// WithDetail returns a new Error with detail
func (e *Error) WithDetail(details string) *Error {
	e2 := *e
	e2.Details = append(e2.Details, details)
	return &e2
}

// Log logs the error at INFO level
func (e *Error) Log() *Error {
	msg := e.Msg("en") // 日志用英文，避免乱码 & 便于日志分析
	if len(e.Details) > 0 {
		cylog.Skip(2).Errorf("cygin error: code=%d msg=%s details=%s", e.Code, msg, strings.Join(e.Details, ","))
	} else {
		cylog.Skip(2).Errorf("cygin error: code=%d msg=%s", e.Code, msg)
	}
	return e // 继续链式调用
}

// Response returns a serializable map for HTTP response
func (e *Error) Response(lang string) any {
	msg := e.Msg(lang)
	resp := map[string]any{
		"code":   e.Code,
		"msg":    msg,
		"status": e.Status,
	}
	if len(e.Details) > 0 {
		resp["details"] = e.Details
	}
	return resp
}

// Error implements error interface. For logging only, no i18n.
func (e *Error) Error() string {
	if len(e.Details) > 0 {
		return fmt.Sprintf("cygin.error: code=%d detail=%s", e.Code, strings.Join(e.Details, ","))
	}
	return fmt.Sprintf("cygin.error: code=%d", e.Code)
}

// MarshalJSON customizes JSON output (used in c.JSON)
func (e *Error) MarshalJSON() ([]byte, error) {
	// Default to English in JSON if no context available
	return json.Marshal(e.Response("en"))
}

/* ---------- 内置 registry ---------- */
type entry struct {
	Message func(lang string) string
	Status  int
}

var registry = map[int]entry{
	ErrCodeInternal: {
		Status: http.StatusInternalServerError,
		Message: func(lang string) string {
			if lang == "zh" {
				return "服务器内部错误"
			}
			return "Internal Server Error"
		},
	},
	ErrCodeNotFound: {
		Status: http.StatusNotFound,
		Message: func(lang string) string {
			if lang == "zh" {
				return "资源不存在"
			}
			return "Not Found"
		},
	},
	ErrCodeInvalidParam: {
		Status: http.StatusBadRequest,
		Message: func(lang string) string {
			if lang == "zh" {
				return "参数无效"
			}
			return "Invalid Parameter"
		},
	},
}

/* ---------- 外部覆盖表（并发安全）---------- */
var (
	overrideMu  sync.RWMutex
	overrideMsg = map[int]func(lang string) string{}
)

// RegErrMsg allows overriding error message function for a code
func RegErrMsg(code int, fn func(lang string) string) {
	overrideMu.Lock()
	defer overrideMu.Unlock()
	overrideMsg[code] = fn
}

// RegErrMsgStatic registers static messages for multiple languages
// Usage: RegErrMsgStatic(10001, "zh", "用户不存在", "en", "User not found")
func RegErrMsgStatic(code int, kvs ...string) {
	if len(kvs)%2 != 0 {
		panic("cygin: RegErrMsgStatic: invalid args, need key-value pairs")
	}
	msgMap := make(map[string]string, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		lang := normLang(kvs[i])
		msgMap[lang] = kvs[i+1]
	}
	RegErrMsg(code, func(lang string) string {
		lang = normLang(lang)
		if msg, ok := msgMap[lang]; ok {
			return msg
		}
		return msgMap["en"] // fallback to English
	})
}

// RegisterMessages registers multiple error codes at once
// Usage:
//
//	cygin.RegisterMessages(map[int]map[string]string{
//	    10001: {"zh": "登录失败", "en": "Login failed"},
//	})
func RegisterMessages(msgs map[int]map[string]string) {
	for code, langMap := range msgs {
		RegErrMsg(code, func(lang string) string {
			lang = normLang(lang)
			if msg, ok := langMap[lang]; ok {
				return msg
			}
			if msg, ok := langMap["en"]; ok {
				return msg
			}
			return "Unknown error"
		})
	}
}

/* ---------- 工厂函数 ---------- */
func NewError(code int, status ...int) *Error {
	st := http.StatusOK
	if len(status) > 0 {
		st = status[0]
	} else if ent, ok := registry[code]; ok {
		st = ent.Status
	}
	return &Error{Code: code, Status: st}
}

func WrapError(err error, code int, status ...int) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		e.Code = code
		if len(status) > 0 {
			e.Status = status[0]
		}
		return e
	}
	return NewError(code, status...).WithDetail(err.Error())
}

/* ---------- 工具 ---------- */
func FromCtx(c *gin.Context) string {
	if l := c.Query("lang"); l != "" {
		return normLang(l)
	}
	if al := c.GetHeader("Accept-Language"); al != "" {
		// Parse primary language from Accept-Language header
		parts := strings.Split(al, ",")
		if len(parts) > 0 {
			primary := strings.TrimSpace(parts[0])
			// Remove q=0.9 etc.
			if semi := strings.Split(primary, ";"); len(semi) > 0 {
				langPart := strings.Split(strings.TrimSpace(semi[0]), "-")
				if len(langPart) > 0 {
					return normLang(langPart[0])
				}
			}
		}
	}
	return "en"
}

// normLang normalizes language code to "zh" or "en"
func normLang(l string) string {
	lang := strings.ToLower(strings.TrimSpace(l))
	switch {
	case strings.HasPrefix(lang, "zh"):
		return "zh"
	default:
		return "en"
	}
}
