package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/termui"
)

var dashStart = time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)

func sampleProgress() capture.Progress {
	return capture.Progress{
		At:            dashStart.Add(83 * time.Second),
		Started:       dashStart,
		Messages:      5231,
		Records:       5240,
		Bytes:         1468006,
		Rotations:     1,
		Reseeds:       1,
		QueueDepth:    15,
		QueueCapacity: 4096,
		MaxQueueDepth: 37,
		File:          "data/v1/symbol=BTC-USD/date=2026-08-25/hour=14/140000.tape",
		WriteLatency: capture.Latency{
			Count: 5240,
			P50:   6100 * time.Nanosecond,
			P99:   41 * time.Microsecond,
		},
	}
}

// feedDash pushes n samples through a dashboard so the rate history is
// populated, and returns it.
func feedDash(caps termui.Caps, w io.Writer, p capture.Progress, n int) *liveDash {
	d := newLiveDash(w, caps, "tape capture   coinbase BTC-USD   columnar   → data", nil)
	for i := range n {
		s := p
		s.At = p.Started.Add(time.Duration(i) * 250 * time.Millisecond)
		s.Messages = int64(i * 16)
		d.observe(s)
	}
	return d
}

func TestDashRenderNeverEmitsEscapesWithoutColor(t *testing.T) {
	caps := termui.Caps{TTY: true, Color: false, Unicode: true, Width: 80}
	d := feedDash(caps, io.Discard, sampleProgress(), 40)

	p := sampleProgress()
	p.Gaps = 3 // the loudest row there is, and still no escape codes
	p.DecodeErrors = 2
	for _, line := range d.render(p) {
		if strings.Contains(line, "\x1b") {
			t.Fatalf("colour off still emitted an escape code: %q", line)
		}
	}
}

func TestDashRenderFitsTheTerminal(t *testing.T) {
	p := sampleProgress()
	p.Gaps = 3
	for _, w := range []int{termui.MinWidth, 40, 60, 80, 120, termui.MaxWidth} {
		caps := termui.Caps{TTY: true, Unicode: true, Width: w}
		d := feedDash(caps, io.Discard, p, 60)
		for i, line := range d.render(p) {
			if n := len([]rune(line)); n > w {
				t.Errorf("width %d: line %d is %d columns: %q", w, i, n, line)
			}
		}
	}
}

func TestDashGapRowIsUnmissable(t *testing.T) {
	caps := termui.Caps{TTY: true, Color: true, Unicode: true, Width: 90}
	d := feedDash(caps, io.Discard, sampleProgress(), 10)

	clean := d.render(sampleProgress())[6]
	if strings.Contains(clean, "UNTRUSTWORTHY") {
		t.Fatalf("a session with no gaps was called untrustworthy: %q", clean)
	}
	if strings.Contains(clean, "\x1b[31m") {
		t.Fatalf("a session with no gaps painted something red: %q", clean)
	}

	if n := len(d.render(sampleProgress())); n != 7 {
		t.Fatalf("a clean session drew %d rows, want 7 with no banner among them", n)
	}

	p := sampleProgress()
	p.Gaps = 3
	rows := d.render(p)
	if len(rows) != 8 {
		t.Fatalf("a session with gaps drew %d rows, want the 7 plus a banner", len(rows))
	}
	if !strings.Contains(rows[6], "\x1b[1m\x1b[31m3\x1b[0m") {
		t.Fatalf("the gap count was not drawn in bold red: %q", rows[6])
	}

	banner := rows[7]
	if !strings.Contains(stripSGR(banner), "3 GAPS — MESSAGES ARE MISSING FROM THIS WINDOW") {
		t.Fatalf("the banner does not say what happened: %q", stripSGR(banner))
	}
	if !strings.HasPrefix(banner, "\x1b[1m\x1b[31m") {
		t.Fatalf("the banner is not bold red: %q", banner)
	}
	if n := len([]rune(stripSGR(banner))); n != caps.Width {
		t.Fatalf("the banner is %d columns, want the full %d", n, caps.Width)
	}

	one := sampleProgress()
	one.Gaps = 1
	if b := stripSGR(d.render(one)[7]); !strings.Contains(b, "1 GAP —") {
		t.Fatalf("one gap was announced in the plural: %q", b)
	}
}

func TestDashAsciiFallback(t *testing.T) {
	caps := termui.Caps{TTY: true, Unicode: false, Width: 80}
	d := feedDash(caps, io.Discard, sampleProgress(), 40)
	p := sampleProgress()
	p.Gaps = 1
	for _, line := range d.render(p) {
		if strings.ContainsAny(line, "▁▂▃▄▅▆▇█░←") {
			t.Fatalf("a non-UTF-8 terminal was sent block characters: %q", line)
		}
	}
}

func TestDashQueueRowShowsTheCeiling(t *testing.T) {
	caps := termui.Caps{TTY: true, Unicode: true, Width: 100}
	d := feedDash(caps, io.Discard, sampleProgress(), 4)
	row := stripSGR(d.queueRow(sampleProgress()))
	if !strings.Contains(row, "15/4096") {
		t.Fatalf("queue row does not show depth against its ceiling: %q", row)
	}
	if !strings.Contains(row, "p50") || !strings.Contains(row, "p99") {
		t.Fatalf("queue row does not show the write latency: %q", row)
	}
}

func TestDashRateRowWaitsForTwoSamples(t *testing.T) {
	caps := termui.Caps{TTY: true, Unicode: true, Width: 80}
	d := newLiveDash(io.Discard, caps, "t", nil)
	if !strings.Contains(d.rateRow(), "waiting") {
		t.Fatalf("a rate was drawn from one sample: %q", d.rateRow())
	}
	d.observe(sampleProgress())
	if !strings.Contains(d.rateRow(), "waiting") {
		t.Fatalf("a rate was drawn from one sample: %q", d.rateRow())
	}
	p := sampleProgress()
	p.At = p.At.Add(time.Second)
	p.Messages += 64
	d.observe(p)
	row := d.rateRow()
	if strings.Contains(row, "waiting") {
		t.Fatalf("two samples still drew no rate: %q", row)
	}
	if !strings.Contains(row, "64/s") {
		t.Fatalf("the rate between the two samples is not on the row: %q", row)
	}
}

func TestDashFinalFrameComesFromTheSummary(t *testing.T) {
	var buf bytes.Buffer
	caps := termui.Caps{TTY: true, Unicode: true, Width: 90}
	d := feedDash(caps, &buf, sampleProgress(), 8)
	d.draw(sampleProgress())
	buf.Reset()

	d.finish(capture.Summary{
		Started:  dashStart,
		Ended:    dashStart.Add(120 * time.Second),
		Messages: 9999,
		Records:  10001,
		Gaps:     2,
	})
	out := stripSGR(buf.String())
	if !strings.Contains(out, "9,999") {
		t.Fatalf("the final frame does not carry the summary's count: %q", out)
	}
	if !strings.Contains(out, "2 GAPS — MESSAGES ARE MISSING FROM THIS WINDOW") {
		t.Fatalf("the final frame lost the gaps: %q", out)
	}
	// The file the session was last writing to survives into the final frame,
	// even though a finished summary has no file open.
	if !strings.Contains(out, "140000.tape") {
		t.Fatalf("the final frame forgot the last file: %q", out)
	}
	if !strings.HasSuffix(buf.String(), "\x1b[?25h") {
		t.Fatal("the final frame did not give the cursor back")
	}
}

func TestDashOffATerminalDrawsPlainLines(t *testing.T) {
	var buf bytes.Buffer
	caps := termui.Caps{TTY: false, Width: 80}
	d := feedDash(caps, &buf, sampleProgress(), 4)
	d.draw(sampleProgress())
	if strings.Contains(buf.String(), "\x1b") {
		t.Fatalf("an escape code reached a non-terminal writer: %q", buf.String())
	}
}

func TestFileLabel(t *testing.T) {
	cases := map[string]string{
		"": "(none open)",
		"data/v1/symbol=BTC-USD/date=2026-08-25/hour=14/140000.tape": "date=2026-08-25/hour=14/140000.tape",
		"140000.tape": "140000.tape",
	}
	for in, want := range cases {
		if got := fileLabel(in); got != want {
			t.Errorf("fileLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWarnBufferHoldsAndReleases(t *testing.T) {
	var out bytes.Buffer
	buf := &warnBuffer{}
	log := newLiveLogger(
		slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo}),
		buf)

	// Before the panel takes over, everything prints as it always did.
	log.Info("capture starting", "dir", "data")
	if !strings.Contains(out.String(), "capture starting") {
		t.Fatalf("a log line before the panel was swallowed: %q", out.String())
	}

	buf.hold()
	out.Reset()
	log.Info("progress", "messages", 5000)
	log.Warn("sequence gap", "expected", 100, "got", 749)
	log.Error("undecodable frame", "err", "unexpected end of JSON input")
	if out.Len() != 0 {
		t.Fatalf("the panel was drawn over by a log line: %q", out.String())
	}

	recent := buf.recent(warnLines)
	if len(recent) != 2 {
		t.Fatalf("buffered %d records, want the warning and the error: %v", len(recent), recent)
	}
	if !strings.Contains(recent[0], "sequence gap") || !strings.Contains(recent[0], "expected=100") {
		t.Fatalf("the warning lost its attributes: %q", recent[0])
	}
	if strings.Contains(strings.Join(recent, " "), "progress") {
		t.Fatal("an INFO line was buffered as if it were a warning")
	}

	// And the ring keeps only the last few.
	for i := range 10 {
		log.Warn("sequence gap", "n", i)
	}
	if got := buf.recent(warnLines); len(got) != warnLines {
		t.Fatalf("recent gave %d lines, want %d", len(got), warnLines)
	}

	buf.release()
	out.Reset()
	log.Info("session summary", "messages", 5000)
	if !strings.Contains(out.String(), "session summary") {
		t.Fatal("the summary did not print once the panel let go")
	}
}

func TestWarnRowsAppearInThePanel(t *testing.T) {
	buf := &warnBuffer{}
	buf.hold()
	log := newLiveLogger(slog.NewTextHandler(io.Discard, nil), buf)
	log.Warn("sequence gap", "missing", 649)

	caps := termui.Caps{TTY: true, Unicode: true, Width: 100}
	d := newLiveDash(io.Discard, caps, "t", buf)
	lines := d.render(sampleProgress())
	joined := stripSGR(strings.Join(lines, "\n"))
	if !strings.Contains(joined, "sequence gap missing=649") {
		t.Fatalf("a warning did not reach the panel that suppressed it:\n%s", joined)
	}
}

func TestLiveHandlerEnabledDelegates(t *testing.T) {
	buf := &warnBuffer{}
	h := &liveHandler{base: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}), buf: buf}
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("the wrapper enabled a level its base handler does not")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("the wrapper disabled a level its base handler enables")
	}
	if _, ok := h.WithAttrs(nil).(*liveHandler); !ok {
		t.Fatal("WithAttrs dropped the wrapper, so the panel would be drawn over")
	}
	if _, ok := h.WithGroup("g").(*liveHandler); !ok {
		t.Fatal("WithGroup dropped the wrapper, so the panel would be drawn over")
	}
}

// stripSGR removes colour sequences so a test can assert on what is readable.
func stripSGR(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' && s[i] != 'A' && s[i] != 'K' && s[i] != 'h' && s[i] != 'l' {
				i++
			}
			i++
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}
