package termui

import (
	"fmt"
	"strconv"
	"time"
)

// The formatting helpers here exist because a status panel is read at a glance
// and a log line is read on purpose. "1468006" is fine in a log, where anything
// consuming it wants the digits; on a panel it is a number nobody can size
// without counting, so the panel says "1.4 MiB" and the log still says the
// digits. Neither replaces the other.

// Count renders n with thousands separators.
func Count(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := false
	if s[0] == '-' {
		neg, s = true, s[1:]
	}
	head := len(s) % 3
	if head == 0 {
		head = 3
	}
	out := s[:head]
	for i := head; i < len(s); i += 3 {
		out += "," + s[i:i+3]
	}
	if neg {
		return "-" + out
	}
	return out
}

// Bytes renders a byte count in binary units, to three significant figures'
// worth of unit.
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	v, exp := float64(n), 0
	for v >= unit && exp < 4 {
		v /= unit
		exp++
	}
	suffix := [...]string{"B", "KiB", "MiB", "GiB", "TiB"}[exp]
	if v < 10 {
		return fmt.Sprintf("%.2f %s", v, suffix)
	}
	if v < 100 {
		return fmt.Sprintf("%.1f %s", v, suffix)
	}
	return fmt.Sprintf("%.0f %s", v, suffix)
}

// Elapsed renders a running duration as h:mm:ss, which is the form a session
// length is read in. time.Duration's own "1h23m45.678s" moves the digits
// sideways every second, and a panel that redraws four times a second must not
// do that.
func Elapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int64(d / time.Second)
	return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
}

// Short renders a small duration to three significant figures' worth of unit,
// the same way the capture summary renders write latency.
func Short(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(time.Microsecond).String()
	case d >= time.Microsecond:
		return d.Round(10 * time.Nanosecond).String()
	default:
		return d.String()
	}
}

// Rate renders a messages-per-second figure at a fixed number of characters'
// worth of precision, so a panel row does not change width as the rate moves
// between 9 and 10.
func Rate(v float64) string {
	switch {
	case v <= 0:
		return "0/s"
	case v < 10:
		return fmt.Sprintf("%.1f/s", v)
	default:
		return fmt.Sprintf("%.0f/s", v)
	}
}
