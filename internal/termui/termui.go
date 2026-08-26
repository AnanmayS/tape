// Package termui is the presentation layer: the handful of primitives the
// commands use to draw on a terminal, and the rules for when they may.
//
// It is deliberately small, and it is not a framework. There is no widget tree,
// no event loop and no alternate screen. Every function here is pure — values
// in, a string out — except the one that moves the cursor, which is the only
// thing that has to know it is talking to a terminal at all. That shape is what
// makes the drawing testable without a tty, and testing it without a tty is the
// point: the same code has to produce readable plain text when its output is a
// pipe.
//
// Three rules govern everything in here.
//
// Nothing decorative may reach a pipe. Escape codes are emitted only when the
// output is a character device and the environment has not asked otherwise, so
// `tape verify | grep` and `tape replay > file` see exactly the bytes they saw
// before this package existed.
//
// Red means the data is untrustworthy. It is reserved for gaps and errors and
// is used for nothing else, because invariant 2 — gaps are never silent — is
// the one thing a glance at a screen has to be able to answer.
//
// A missing capability degrades, it does not fail. No terminal width is 80
// columns; no UTF-8 locale is an ASCII ramp; no colour is plain text; no
// terminal at all is whatever the command printed before.
package termui

import (
	"io"
	"os"
	"strconv"
	"strings"
)

// Width bounds. DefaultWidth is what an unknown terminal is assumed to be, and
// the clamps keep a chart from collapsing into nothing or stretching across a
// maximised 4K window where nobody can follow a row that long.
const (
	DefaultWidth = 80
	MinWidth     = 24
	MaxWidth     = 200
)

// Caps is what the output can do. It is a value, so a test constructs the
// terminal it wants to render for rather than needing one.
type Caps struct {
	// TTY reports whether the output is a character device. It is the switch
	// between drawing and printing: false means the command falls back to the
	// output it had before, unchanged.
	TTY bool

	// Color reports whether escape codes may be emitted.
	Color bool

	// Unicode reports whether the box and block characters are safe to use.
	Unicode bool

	// Width is the terminal width in columns, already clamped.
	Width int
}

// Detect reports what f can do, reading the environment for everything the
// file itself cannot answer.
//
// Width comes from COLUMNS rather than an ioctl. A syscall would be more
// accurate and would cost either a dependency or a build-tagged file per
// platform, and the thing being sized is a status panel: eighty columns when
// the shell did not say is a panel that is narrower than the window, which is
// the harmless direction to be wrong in.
func Detect(f *os.File) Caps {
	return detect(isTerminal(f), os.Getenv)
}

// detect is Detect with its two inputs handed in, which is the form the tests
// use.
func detect(tty bool, env func(string) string) Caps {
	c := Caps{
		TTY:     tty,
		Color:   tty,
		Unicode: utf8Locale(env),
		Width:   parseWidth(env("COLUMNS")),
	}
	// The NO_COLOR convention: set and non-empty disables colour, whatever the
	// value is. TERM=dumb is the older way of saying the same thing.
	if env("NO_COLOR") != "" || env("TERM") == "dumb" {
		c.Color = false
	}
	return c
}

// isTerminal reports whether f is a character device. This is the whole of the
// tty detection, and it is a mode bit rather than a dependency.
func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// utf8Locale is the cheap heuristic for whether block characters will render:
// the locale environment, most specific variable first. Nothing here is set on
// a bare CI container, and the answer there is no, which is the safe way round
// — an ASCII ramp on a UTF-8 terminal is ugly, and mojibake in a log nobody can
// re-run is worse.
func utf8Locale(env func(string) string) bool {
	for _, key := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		v := env(key)
		if v == "" {
			continue
		}
		u := strings.ToUpper(v)
		return strings.Contains(u, "UTF-8") || strings.Contains(u, "UTF8")
	}
	return false
}

func parseWidth(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return DefaultWidth
	}
	return clampWidth(n)
}

func clampWidth(n int) int {
	if n < MinWidth {
		return MinWidth
	}
	if n > MaxWidth {
		return MaxWidth
	}
	return n
}

// Plain returns c with colour off. It is what a -no-color flag does, and what
// writing to a file does when the caller decided a tty elsewhere.
func (c Caps) Plain() Caps {
	c.Color = false
	return c
}

// Color is one of the few colours this package uses. The palette is small on
// purpose: every additional colour is a thing a reader has to learn, and the
// only one that has to be learned instantly is red.
type Color uint8

const (
	// ColorNone leaves the text alone.
	ColorNone Color = iota

	// ColorRed means this data is untrustworthy: a gap, a drop, an error. It is
	// used for nothing else.
	ColorRed

	// ColorGreen is a buy side, and a continuity that is intact.
	ColorGreen

	// ColorYellow is a reseed or a warning: not a loss, but a boundary nothing
	// is continuous across.
	ColorYellow

	// ColorCyan is structural chrome — headings, keys, the parts of a line that
	// are labels rather than data.
	ColorCyan

	// ColorDim is text that is present but not the point.
	ColorDim
)

// sgr is the escape sequence that turns a colour on.
func (c Color) sgr() string {
	switch c {
	case ColorRed:
		return "\x1b[31m"
	case ColorGreen:
		return "\x1b[32m"
	case ColorYellow:
		return "\x1b[33m"
	case ColorCyan:
		return "\x1b[36m"
	case ColorDim:
		return "\x1b[2m"
	default:
		return ""
	}
}

const sgrReset = "\x1b[0m"
const sgrBold = "\x1b[1m"

// Paint wraps s in col, or returns it unchanged when colour is off.
//
// Painting changes the string's length without changing its width, so a caller
// that is aligning columns must pad first and paint the padded string. Every
// call site in this repo does; there is no width-aware padding helper here
// because adding one would suggest the other order works.
func (c Caps) Paint(col Color, s string) string {
	if !c.Color || col == ColorNone || s == "" {
		return s
	}
	return col.sgr() + s + sgrReset
}

// Bold wraps s in col and bold. It is for the things that must not be scrolled
// past: a gap banner, a non-zero gap count.
func (c Caps) Bold(col Color, s string) string {
	if !c.Color || s == "" {
		return s
	}
	return sgrBold + col.sgr() + s + sgrReset
}

// Ramps, lowest level first. Both are exactly RampLevels long so that a level
// computed for one is valid for the other.
var (
	blockRamp = []rune("▁▂▃▄▅▆▇█")
	asciiRamp = []rune("_.-~=+*#")
)

// RampLevels is how many levels a chart glyph can take.
const RampLevels = 8

// ramp is the glyph set this terminal draws charts with.
func (c Caps) ramp() []rune {
	if c.Unicode {
		return blockRamp
	}
	return asciiRamp
}

// Level maps v in [0, max] onto a ramp level in [0, RampLevels-1].
//
// Zero is its own level and anything above zero is not: a bucket holding one
// message must not render identically to a bucket holding none, because the
// difference between "quiet" and "nothing arrived" is exactly the difference
// this project cares about. Values outside the range are clamped rather than
// rejected — a chart is not the place to fail.
func Level(v, max float64) int {
	if v <= 0 || max <= 0 {
		return 0
	}
	if v >= max {
		return RampLevels - 1
	}
	// Ceiling, so the smallest non-zero value still reaches level 1.
	lvl := int(v/max*float64(RampLevels-1)) + 1
	if lvl > RampLevels-1 {
		return RampLevels - 1
	}
	return lvl
}

// Glyph is the character for a ramp level.
func (c Caps) Glyph(level int) rune {
	r := c.ramp()
	if level < 0 {
		level = 0
	}
	if level >= len(r) {
		level = len(r) - 1
	}
	return r[level]
}

// LegendRamp is the ramp itself, for a legend line.
func (c Caps) LegendRamp() string { return string(c.ramp()) }

// Sparkline renders values as one row of exactly min(len(values), width) cells,
// scaled against the largest value present.
//
// When there are more values than cells the most recent ones win: this draws a
// rate over the last few seconds, and the few seconds that matter are the ones
// just gone. An empty series renders as an empty string; the caller pads, which
// keeps the decision about what an empty panel row looks like with the panel.
func (c Caps) Sparkline(values []float64, width int) string {
	if width <= 0 || len(values) == 0 {
		return ""
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}
	max := 0.0
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	var b strings.Builder
	b.Grow(len(values) * 4)
	for _, v := range values {
		b.WriteRune(c.Glyph(Level(v, max)))
	}
	return b.String()
}

// SparkMax is the scale a Sparkline of values would use: the largest value in
// the cells it would draw. It is here so a caller can label the axis without
// recomputing which values got drawn.
func SparkMax(values []float64, width int) float64 {
	if width <= 0 || len(values) == 0 {
		return 0
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}
	max := 0.0
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

// Bar renders v against max as a meter width cells wide. v is clamped into
// [0, max]; a max of zero or less draws an empty meter rather than dividing by
// it.
func (c Caps) Bar(v, max float64, width int) string {
	if width <= 0 {
		return ""
	}
	full, empty := '#', '-'
	if c.Unicode {
		full, empty = '█', '░'
	}
	n := 0
	if max > 0 && v > 0 {
		if v >= max {
			n = width
		} else {
			n = int(v / max * float64(width))
			// Any non-zero occupancy shows at least one cell, for the same
			// reason Level treats zero as its own case.
			if n == 0 {
				n = 1
			}
		}
	}
	return strings.Repeat(string(full), n) + strings.Repeat(string(empty), width-n)
}

// Rule is a horizontal line n cells wide.
func (c Caps) Rule(n int) string {
	if n <= 0 {
		return ""
	}
	r := "-"
	if c.Unicode {
		r = "─"
	}
	return strings.Repeat(r, n)
}

// HeavyRule is the line a banner is built from: heavier than Rule, so a gap
// banner does not read as a section divider.
func (c Caps) HeavyRule(n int) string {
	if n <= 0 {
		return ""
	}
	r := "="
	if c.Unicode {
		r = "━"
	}
	return strings.Repeat(r, n)
}

// Pad right-pads s to n columns, or truncates it to n. It counts runes, which
// is right for everything this package draws — the glyphs are all one column
// wide and none of them combine.
func Pad(s string, n int) string {
	if n <= 0 {
		return ""
	}
	w := len([]rune(s))
	if w == n {
		return s
	}
	if w > n {
		return Truncate(s, n)
	}
	return s + strings.Repeat(" ", n-w)
}

// Truncate cuts s to at most n columns, marking the cut when there is room for
// a marker. It must be given text that carries no escape codes.
func Truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n == 1 {
		return string(r[:1])
	}
	return string(r[:n-1]) + "…"
}

// Frame redraws a block of lines in place, by moving the cursor back up over
// the block it drew last time and clearing each line as it rewrites it.
//
// It never clears the screen. A capture session that ran for an hour is a thing
// whose scrollback is worth keeping — the log lines before it started, the
// command that started it — and a full-screen redraw throws all of that away
// for a panel that is eight lines tall.
//
// When the output is not a terminal a Frame is an append-only writer: each
// draw is printed below the last, with no escape codes at all.
type Frame struct {
	w    io.Writer
	caps Caps
	n    int
}

// NewFrame returns a Frame drawing to w.
func NewFrame(w io.Writer, caps Caps) *Frame {
	return &Frame{w: w, caps: caps}
}

// Draw replaces the previously drawn block with lines.
func (f *Frame) Draw(lines []string) error {
	var b strings.Builder
	if !f.caps.TTY {
		for _, ln := range lines {
			b.WriteString(ln)
			b.WriteByte('\n')
		}
		_, err := io.WriteString(f.w, b.String())
		return err
	}

	if f.n > 0 {
		b.WriteString("\x1b[")
		b.WriteString(strconv.Itoa(f.n))
		b.WriteByte('A')
	}
	for _, ln := range lines {
		b.WriteString("\r\x1b[2K")
		b.WriteString(ln)
		b.WriteByte('\n')
	}
	// A frame that shrank would otherwise leave the tail of the last one on
	// screen, which is worse than a blank line: it would be stale numbers
	// sitting under fresh ones.
	for i := len(lines); i < f.n; i++ {
		b.WriteString("\r\x1b[2K\n")
	}
	if extra := f.n - len(lines); extra > 0 {
		b.WriteString("\x1b[")
		b.WriteString(strconv.Itoa(extra))
		b.WriteByte('A')
	}
	f.n = len(lines)
	_, err := io.WriteString(f.w, b.String())
	return err
}

// HideCursor parks the cursor out of the way for the duration of the drawing.
// It is a no-op off a terminal. Every caller must pair it with ShowCursor,
// including on the signal path — a shell left with an invisible cursor is a
// shell somebody has to reset.
func (f *Frame) HideCursor() { f.escape("\x1b[?25l") }

// ShowCursor undoes HideCursor.
func (f *Frame) ShowCursor() { f.escape("\x1b[?25h") }

func (f *Frame) escape(s string) {
	if f.caps.TTY {
		io.WriteString(f.w, s)
	}
}
