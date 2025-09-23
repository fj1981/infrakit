package cyutil

import (
	"fmt"
	"strings"
	"time"
)

// FormatDuration formats a time.Duration into a human-readable string
// with appropriate units (ns, µs, ms, s) based on the duration's magnitude.
// This is useful for logging request latencies in a more readable format.
func FormatDuration(d time.Duration) (r string) {
	defer func() {
		if r == "" {
			r = "0ns"
		}
		r = strings.TrimSpace(r)
	}()
	if d < time.Microsecond {
		r = fmt.Sprintf("%dns", d.Nanoseconds())
		return
	}
	if d < time.Millisecond {
		r = fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/float64(time.Microsecond))
		return
	}
	if d < time.Second {
		r = fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/float64(time.Millisecond))
		return
	}
	if d < time.Minute {
		r = fmt.Sprintf("%.2fs", float64(d.Nanoseconds())/float64(time.Second))
		return
	}
	if d < time.Hour {
		r = fmt.Sprintf("%.2fm", float64(d.Nanoseconds())/float64(time.Minute))
		return
	}
	r = fmt.Sprintf("%.2fh", float64(d.Nanoseconds())/float64(time.Hour))
	return
}

func ToTimestamp(tm time.Time) int64 {
	return tm.Unix()
}
