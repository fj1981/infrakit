package cylog

import (
	"context"
	"io"
	"log/slog"

	"github.com/fj1981/infrakit/pkg/cyutil"
)

type apacheHandler struct {
	w    io.Writer
	opts slog.HandlerOptions
}

func (h *apacheHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *apacheHandler) Handle(_ context.Context, r slog.Record) error {
	buf := make([]byte, 0, 1024)
	buf = append(buf, r.Time.Format("[2006-01-02 15:04:05.000 -0700")...)

	// Add color based on log level
	levelStr := cyutil.PadStart(r.Level.String(), 7, " ")
	coloredLevel := levelStr
	switch r.Level {
	case slog.LevelDebug:
		coloredLevel = "\033[36m" + levelStr + "\033[0m" // Cyan
	case slog.LevelInfo:
		coloredLevel = "\033[32m" + levelStr + "\033[0m" // Green
	case slog.LevelWarn:
		coloredLevel = "\033[33m" + levelStr + "\033[0m" // Yellow
	case slog.LevelError:
		coloredLevel = "\033[31m" + levelStr + "\033[0m" // Red
	}

	buf = append(buf, " |"+coloredLevel...)

	// Add source information if available
	if h.opts.AddSource && r.PC != 0 {
		// If ReplaceAttr is provided, use it to format the source information
		if h.opts.ReplaceAttr != nil {
			srcAttr := h.opts.ReplaceAttr(nil, slog.Any(slog.SourceKey, &slog.Source{
				Function: "unknown",
				File:     "unknown",
				Line:     0,
			}))

			// Only append if the attribute wasn't removed by ReplaceAttr
			if srcAttr.Key != "" {
				buf = append(buf, "| "...)
				buf = append(buf, srcAttr.Value.String()...)
			}
		}
	}
	buf = append(buf, "] "...)
	buf = append(buf, r.Message...)

	r.Attrs(func(a slog.Attr) bool {
		// Apply ReplaceAttr if provided
		if h.opts.ReplaceAttr != nil {
			a = h.opts.ReplaceAttr(nil, a)
			// Skip if attribute was removed by ReplaceAttr
			if a.Key == "" {
				return true
			}
		}

		// Handle empty keys or !BADKEY
		if a.Key == "" || a.Key == "!BADKEY" {
			// Just print the value without a key prefix
			buf = append(buf, ' ')
			buf = append(buf, a.Value.String()...)
		} else {
			// Normal
			// key=value format
			if a.Key == "msg" {
				if pa, ok := a.Value.Any().(printfAttr); ok {
					buf = append(buf, []byte(pa.LogValue().String())...)
					return true
				}
			}
			buf = append(buf, ' ')
			buf = append(buf, a.Key...)
			buf = append(buf, '=')

			// Check if the value implements LogValuer and use its LogValue method
			if a.Value.Kind() == slog.KindAny {
				if logValuer, ok := a.Value.Any().(interface{ LogValue() slog.Value }); ok {
					buf = append(buf, logValuer.LogValue().String()...)
				} else {
					buf = append(buf, a.Value.String()...)
				}
			} else {
				buf = append(buf, a.Value.String()...)
			}
		}
		buf = append(buf, ' ')
		return true
	})
	buf = append(buf, '\n')
	_, err := h.w.Write(buf)
	return err
}

func (h *apacheHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *apacheHandler) WithGroup(string) slog.Handler      { return h }

func NewApacheHandler(w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	handler := &apacheHandler{w: w}
	if opts != nil {
		handler.opts = *opts
	} else {
		// Default to LevelInfo if no level is specified
		handler.opts.Level = slog.LevelInfo
	}
	return handler
}
