package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/termui"
)

// The live dashboard replaces a scrolling log with a panel that redraws in
// place. It is a different presentation of numbers that already existed; it
// adds nothing to what a session measures and nothing to what a session costs.
//
// Two things about it are deliberate.
//
// It redraws by moving the cursor up over its own block, never by clearing the
// screen. A capture that has been running for an hour sits under whatever the
// terminal was doing before it started, and that scrollback belongs to the
// person who started it.
//
// It never hides a warning. While the panel is up, log lines below WARN are
// suppressed — they are the progress lines the panel replaces — but every
// warning and error is buffered and shown inside it, and the counts beside them
// (gaps, decode errors, exchange errors) come straight out of the session's own
// counters. A gap in particular is drawn in red and stays drawn: invariant 2
// says a gap is never silent, and a display that let one scroll past would be
// the quietest place yet for one to hide.

// labelWidth lines the panel's labels up with the summary printed by verify and
// stat, so the three commands read as one program.
const labelWidth = 12

// warnLines is how many recent warnings the panel shows. Enough to see a burst
// starting; not so many that the panel becomes the log it replaced.
const warnLines = 3

// liveDash draws a capture session. It is driven from one goroutine — the one
// that owns the Progress channel — and holds no lock of its own.
type liveDash struct {
	caps  termui.Caps
	frame *termui.Frame
	title string
	warn  *warnBuffer

	rates   []float64
	last    capture.Progress
	hasLast bool
}

func newLiveDash(w io.Writer, caps termui.Caps, title string, warn *warnBuffer) *liveDash {
	return &liveDash{
		caps:  caps,
		frame: termui.NewFrame(w, caps),
		title: title,
		warn:  warn,
	}
}

// run draws every sample that arrives, and returns when the channel closes.
func (d *liveDash) run(ch <-chan capture.Progress) {
	d.frame.HideCursor()
	defer d.frame.ShowCursor()
	for p := range ch {
		d.observe(p)
		d.draw(p)
	}
}

// observe folds a sample into the rate history. The instantaneous rate is a
// difference between two samples, which is why the history lives here and not
// in the capture package: the session counts, the display differentiates.
func (d *liveDash) observe(p capture.Progress) {
	if d.hasLast {
		if secs := p.At.Sub(d.last.At).Seconds(); secs > 0 {
			d.rates = append(d.rates, float64(p.Messages-d.last.Messages)/secs)
			if len(d.rates) > termui.MaxWidth {
				d.rates = d.rates[len(d.rates)-termui.MaxWidth:]
			}
		}
	}
	d.last, d.hasLast = p, true
}

// draw renders one frame.
func (d *liveDash) draw(p capture.Progress) { d.frame.Draw(d.render(p)) }

// finish draws the last frame from the session's own summary, which is the
// authoritative count, and leaves it on screen.
func (d *liveDash) finish(sum capture.Summary) {
	final := sum.Progress()
	// The summary has no current file — there isn't one, the last was closed —
	// so the panel keeps naming the last file the session wrote to.
	final.File = d.last.File
	final.QueueCapacity = d.last.QueueCapacity
	d.frame.Draw(d.render(final))
	d.frame.ShowCursor()
}

// seg is one run of text in a panel row, with the colour it is drawn in.
// Building a row out of segments is what keeps painting and width apart:
// escape codes have length but no width, so a row is truncated by trimming
// segments before any of them is painted.
type seg struct {
	text string
	col  termui.Color
	bold bool
}

func plain(s string) seg                { return seg{text: s} }
func dim(s string) seg                  { return seg{text: s, col: termui.ColorDim} }
func tint(c termui.Color, s string) seg { return seg{text: s, col: c} }
func alarm(s string) seg                { return seg{text: s, col: termui.ColorRed, bold: true} }

// body is how many columns a row has after its label.
func (d *liveDash) body() int {
	n := d.caps.Width - labelWidth - 2
	if n < 8 {
		n = 8
	}
	return n
}

// field is a label and its value, which belong together or not at all. A row
// too wide for the terminal drops whole fields off its right-hand end rather
// than cutting one in half, so a narrow panel reads as fewer facts rather than
// as a truncated sentence. The leftmost field is the exception: it is the point
// of the row and is truncated rather than dropped.
type field []seg

func group(segs ...seg) field { return segs }

func (f field) width() int {
	n := 0
	for _, s := range f {
		n += len([]rune(s.text))
	}
	return n
}

// row assembles a labelled row within the terminal's width rather than letting
// it wrap. A wrapped row would push the whole panel down by a line, which the
// next redraw would then fail to cover.
func (d *liveDash) row(label string, fields ...field) string {
	var b strings.Builder
	b.WriteString("  ")
	b.WriteString(termui.Pad(label, labelWidth))
	budget := d.body()
	for i, f := range fields {
		if f.width() > budget && i > 0 {
			break
		}
		for _, s := range f {
			if budget <= 0 {
				break
			}
			t := s.text
			if len([]rune(t)) > budget {
				t = termui.Truncate(t, budget)
			}
			budget -= len([]rune(t))
			if s.bold {
				b.WriteString(d.caps.Bold(s.col, t))
			} else {
				b.WriteString(d.caps.Paint(s.col, t))
			}
		}
	}
	return b.String()
}

// render is the whole panel, as pure a function as a clock allows: sample in,
// lines out. It is what the tests call.
func (d *liveDash) render(p capture.Progress) []string {
	lines := []string{d.caps.Paint(termui.ColorCyan, termui.Truncate(d.title, d.caps.Width))}

	lines = append(lines, d.row("elapsed",
		group(plain(termui.Pad(termui.Elapsed(p.Elapsed()), 12))),
		group(dim("messages "), plain(termui.Pad(termui.Count(p.Messages), 10))),
		group(dim("avg "), plain(termui.Rate(p.MessagesPerSecond()))),
	))

	lines = append(lines, d.rateRow())

	lines = append(lines, d.row("written",
		group(plain(termui.Pad(termui.Bytes(p.Bytes), 12))),
		group(dim("records "), plain(termui.Pad(termui.Count(p.Records), 11))),
		group(dim("rotations "), plain(fmt.Sprint(p.Rotations))),
	))

	file := []seg{dim(fileLabel(p.File))}
	if p.FilePending {
		file = append(file, dim("  (buffering; not on disk yet)"))
	}
	lines = append(lines, d.row("file", file))
	lines = append(lines, d.queueRow(p))
	lines = append(lines, d.gapRow(p))
	if p.Gaps > 0 {
		lines = append(lines, d.gapBanner(p))
	}
	lines = append(lines, d.warnRows()...)
	return lines
}

// gapBanner is a whole line of the panel given over to saying that this window
// is no longer worth anything. It appears the moment the count leaves zero and
// stays for the rest of the session, because invariant 2 is not a statistic —
// it is the difference between data and something that looks like data.
func (d *liveDash) gapBanner(p capture.Progress) string {
	text := fmt.Sprintf(" %s %s — MESSAGES ARE MISSING FROM THIS WINDOW ",
		termui.Count(p.Gaps), plural(int(p.Gaps), "GAP", "GAPS"))
	rule := (d.caps.Width - len([]rune(text))) / 2
	if rule < 1 {
		return d.caps.Bold(termui.ColorRed, termui.Truncate(strings.TrimSpace(text), d.caps.Width))
	}
	line := d.caps.HeavyRule(rule) + text + d.caps.HeavyRule(d.caps.Width-rule-len([]rune(text)))
	return d.caps.Bold(termui.ColorRed, line)
}

// rateRow is the sparkline: the last few seconds of receive rate, scaled
// against the tallest of them.
func (d *liveDash) rateRow() string {
	// Room for "  now NNN/s  peak NNN/s" beside the line.
	const labels = 22
	w := d.body() - labels
	if w < 8 {
		w = 8
	}
	spark := d.caps.Sparkline(d.rates, w)
	if spark == "" {
		return d.row("rate", group(dim("(waiting for the second sample)")))
	}
	return d.row("rate",
		group(plain(termui.Pad(spark, w))),
		group(dim("  now "), plain(termui.Rate(d.rates[len(d.rates)-1]))),
		group(dim("  peak "), plain(termui.Rate(termui.SparkMax(d.rates, w)))),
	)
}

// queueRow is the backpressure picture: how full the reader-to-writer queue is,
// and what a write costs. Depth says the writer fell behind; the latency tail
// says whether it can catch up. See the capture package comment.
func (d *liveDash) queueRow(p capture.Progress) string {
	const barWidth = 16
	fields := []field{group(
		plain(d.caps.Bar(float64(p.QueueDepth), float64(p.QueueCapacity), barWidth)),
		plain(fmt.Sprintf(" %d/%d", p.QueueDepth, p.QueueCapacity)),
	)}
	if l := p.WriteLatency; l.Count > 0 {
		fields = append(fields,
			group(dim("   write p50 "), plain(termui.Short(l.P50))),
			group(dim(" p99 "), plain(termui.Short(l.P99))),
		)
	}
	return d.row("queue", fields...)
}

// gapRow is the row this whole panel exists for. Zero gaps is one quiet
// character; anything else is red, bold, and says in words what the number
// means, because a count nobody can interpret at a glance is a count that gets
// glanced past.
func (d *liveDash) gapRow(p capture.Progress) string {
	count := tint(termui.ColorGreen, "0")
	if p.Gaps > 0 {
		count = alarm(termui.Count(p.Gaps))
	}
	fields := []field{
		group(count),
		group(dim("   reseeds "), plain(fmt.Sprint(p.Reseeds))),
		group(dim("   stale "), plain(termui.Count(p.StaleMessages))),
		group(dim("   decode "), errCount(p.DecodeErrors)),
		group(dim("   exchange "), errCount(p.ExchangeErrors)),
	}
	if p.Dropped > 0 {
		fields = append(fields, group(dim("   dropped "), alarm(termui.Count(p.Dropped))))
	}
	return d.row("gaps", fields...)
}

func errCount(n int64) seg {
	if n > 0 {
		return tint(termui.ColorRed, termui.Count(n))
	}
	return plain("0")
}

// warnRows shows the last few warnings the session logged. They are suppressed
// from the terminal while the panel is up, so this is where they have to be.
func (d *liveDash) warnRows() []string {
	if d.warn == nil {
		return nil
	}
	recent := d.warn.recent(warnLines)
	if len(recent) == 0 {
		return nil
	}
	rows := make([]string, 0, len(recent))
	for i, w := range recent {
		label := ""
		if i == 0 {
			label = "recent"
		}
		rows = append(rows, d.row(label, group(alarm("! "), plain(w))))
	}
	return rows
}

// fileLabel shortens a tape path to the part that changes. The root and the
// partition prefix are the same for every file in a session and are already in
// the startup line; the window is what a reader is checking.
func fileLabel(path string) string {
	if path == "" {
		return "(none open)"
	}
	dir, base := filepath.Split(path)
	parts := strings.Split(strings.Trim(filepath.ToSlash(dir), "/"), "/")
	if n := len(parts); n >= 2 {
		return parts[n-2] + "/" + parts[n-1] + "/" + base
	}
	return path
}

// warnBuffer holds the last few warnings a session logged, for the panel to
// show. It is written by whichever goroutine logged and read by the goroutine
// drawing, so it takes a lock — off the write path entirely, since a warning is
// by definition the rare case.
type warnBuffer struct {
	mu   sync.Mutex
	live bool
	ring []string
}

// hold puts the buffer in charge: while it is held, records below WARN are
// dropped and warnings are kept here instead of being printed.
func (b *warnBuffer) hold() { b.mu.Lock(); b.live = true; b.mu.Unlock() }

// release hands logging back to the underlying handler, so the session summary
// prints normally once the panel has stopped redrawing.
func (b *warnBuffer) release() { b.mu.Lock(); b.live = false; b.mu.Unlock() }

func (b *warnBuffer) recent(n int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.ring) <= n {
		return append([]string(nil), b.ring...)
	}
	return append([]string(nil), b.ring[len(b.ring)-n:]...)
}

// take records r if the panel is holding the terminal, and reports whether the
// underlying handler should be skipped.
func (b *warnBuffer) take(r slog.Record) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.live {
		return false
	}
	if r.Level >= slog.LevelWarn {
		b.ring = append(b.ring, formatRecord(r))
		if len(b.ring) > 64 {
			b.ring = b.ring[len(b.ring)-64:]
		}
	}
	return true
}

// formatRecord renders a log record as one line for the panel. It is the
// message and its own attributes; the logger-wide attributes (feed, product)
// are already in the panel's title.
func formatRecord(r slog.Record) string {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})
	return b.String()
}

// liveHandler is a slog.Handler that lets a warnBuffer intercept records while
// the panel owns the terminal, and is the base handler the rest of the time.
type liveHandler struct {
	base slog.Handler
	buf  *warnBuffer
}

func newLiveLogger(base slog.Handler, buf *warnBuffer) *slog.Logger {
	return slog.New(&liveHandler{base: base, buf: buf})
}

func (h *liveHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.base.Enabled(ctx, l)
}

func (h *liveHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.buf.take(r) {
		return nil
	}
	return h.base.Handle(ctx, r)
}

func (h *liveHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &liveHandler{base: h.base.WithAttrs(as), buf: h.buf}
}

func (h *liveHandler) WithGroup(name string) slog.Handler {
	return &liveHandler{base: h.base.WithGroup(name), buf: h.buf}
}

// liveTitle is the panel's first line: what is being captured, in what format,
// to where. It is the startup log line, which the panel suppresses.
func liveTitle(feedName, product string, format capture.Format, dir, store string) string {
	parts := []string{"tape capture", feedName + " " + product, string(format), "→ " + dir}
	if store != "" {
		parts = append(parts, "→ "+store)
	}
	return strings.Join(parts, "   ")
}

// progressBuffer is the depth of the channel between the session and the panel.
// One: the display wants the newest sample, and a queue of stale ones would
// only make it lag behind the session it is describing.
const progressBuffer = 1

// liveInterval is how often the panel redraws.
const liveInterval = 250 * time.Millisecond
