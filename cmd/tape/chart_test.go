package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/replay"
	"github.com/AnanmayS/tape/internal/tapefile"
	"github.com/AnanmayS/tape/internal/termui"
)

var chartStart = time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)

func chartAt(d time.Duration) time.Time { return chartStart.Add(d) }

func msg(d time.Duration, file int) replay.Record {
	return replay.Record{
		Kind:     replay.KindMessage,
		Position: replay.Position{FileIndex: file},
		Event:    event.Event{RecvTime: chartAt(d)},
	}
}

func gapAt(d time.Duration, file int) replay.Record {
	return replay.Record{
		Kind:     replay.KindGap,
		Position: replay.Position{FileIndex: file},
		Gap:      tapefile.Gap{At: chartAt(d), Expected: 100, Got: 749},
	}
}

func reseedAt(d time.Duration, file int, opening bool) replay.Record {
	return replay.Record{
		Kind:     replay.KindReseed,
		Position: replay.Position{FileIndex: file},
		Reseed:   tapefile.Reseed{At: chartAt(d), Reason: "subscribed"},
		Opening:  opening,
	}
}

// a window of steady traffic across two files, with a gap and a reseed in the
// middle of the second one.
func brokenWindow() *chart {
	c := newChart()
	c.add(reseedAt(0, 0, true))
	for i := range 60 {
		c.add(msg(time.Duration(i)*time.Second, 0))
	}
	c.add(gapAt(60*time.Second, 1))
	c.add(reseedAt(60*time.Second+time.Millisecond, 1, false))
	for i := 61; i < 120; i++ {
		c.add(msg(time.Duration(i)*time.Second, 1))
	}
	return c
}

func TestChartEmptyDrawsNothing(t *testing.T) {
	c := newChart()
	if !c.empty() {
		t.Fatal("a fresh chart is not empty")
	}
	var buf bytes.Buffer
	c.print(&buf, termui.Caps{TTY: true, Unicode: true, Width: 80})
	if buf.Len() != 0 {
		t.Fatalf("an empty chart drew %q", buf.String())
	}
}

func TestChartOpeningReseedIsNotAWarning(t *testing.T) {
	c := newChart()
	c.add(reseedAt(0, 0, true))
	for i := range 10 {
		c.add(msg(time.Duration(i)*time.Second, 0))
	}
	_, marks := c.tl.Columns(10)
	for i, m := range marks {
		if m == termui.MarkReseed {
			t.Fatalf("column %d marks the reseed that opens the window as a break", i)
		}
	}
	// It still gets the file boundary, since it is the first record of file 0.
	if marks[0] != termui.MarkFile {
		t.Fatalf("the window's first file is not marked: %v", marks)
	}
}

func TestChartMarksGapsWhereTheyHappened(t *testing.T) {
	c := brokenWindow()
	density, marks := c.tl.Columns(20)

	// The gap is at 60s of a 119s window: column floor(60/119*20) = 10.
	if marks[10] != termui.MarkGap {
		t.Fatalf("the gap is not in the column its timestamp belongs to: %v", marks)
	}
	// And it outranks the reseed and the file boundary that share the column.
	count := 0
	for _, m := range marks {
		if m == termui.MarkGap {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("%d gap columns, want 1: %v", count, marks)
	}
	// A gap record is a hole, not a message: it must not add to density.
	var total int64
	for _, n := range density {
		total += n
	}
	if total != 119 {
		t.Fatalf("density holds %d, want the 119 messages and nothing else", total)
	}
}

func TestChartFilesAreMarkedOnceEach(t *testing.T) {
	c := brokenWindow()
	if len(c.seen) != 2 {
		t.Fatalf("marked %d files, want 2", len(c.seen))
	}
	// Drawn wide enough that the two boundaries cannot share a column: the
	// first record of each file is marked, and no other record is.
	_, marks := c.tl.Columns(120)
	files := 0
	for _, m := range marks {
		if m == termui.MarkFile {
			files++
		}
	}
	if files != 1 {
		t.Fatalf("%d visible file marks; the second is under the gap that outranks it", files)
	}
	if marks[0] != termui.MarkFile {
		t.Fatal("the window's first file is not marked at its first column")
	}
}

func TestChartPrintsGapsInRed(t *testing.T) {
	var buf bytes.Buffer
	brokenWindow().print(&buf, termui.Caps{TTY: true, Color: true, Unicode: true, Width: 90})
	out := buf.String()

	if !strings.Contains(out, "\x1b[1m\x1b[31m") {
		t.Fatalf("the gap was not drawn in red:\n%s", out)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("the chart is %d lines, want density, marks, axis and legend:\n%s", len(lines), out)
	}
	if !strings.HasPrefix(lines[0], "  shape       ") {
		t.Fatalf("the chart does not use the summary's label column: %q", lines[0])
	}
	plain := stripSGR(out)
	if !strings.Contains(plain, "!") {
		t.Fatalf("no gap marker under the density row:\n%s", plain)
	}
	if !strings.Contains(plain, "^") {
		t.Fatalf("no reseed marker:\n%s", plain)
	}
	if !strings.Contains(plain, "14:00:00") || !strings.Contains(plain, "14:01:59") {
		t.Fatalf("the axis does not label both ends of the window:\n%s", plain)
	}
	if !strings.Contains(plain, "gap") || !strings.Contains(plain, "reseed") || !strings.Contains(plain, "file") {
		t.Fatalf("the legend does not explain the markers:\n%s", plain)
	}
}

func TestChartPrintsNoEscapesWithoutColor(t *testing.T) {
	var buf bytes.Buffer
	brokenWindow().print(&buf, termui.Caps{TTY: true, Color: false, Unicode: true, Width: 90})
	if strings.Contains(buf.String(), "\x1b") {
		t.Fatalf("colour off still emitted an escape code:\n%q", buf.String())
	}
}

func TestChartFitsTheTerminal(t *testing.T) {
	for _, w := range []int{termui.MinWidth, 40, 80, 132, termui.MaxWidth} {
		var buf bytes.Buffer
		brokenWindow().print(&buf, termui.Caps{TTY: true, Unicode: true, Width: w})
		for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
			if n := len([]rune(line)); n > w && w > termui.MinWidth+labelWidth+2 {
				t.Errorf("width %d: a chart line is %d columns: %q", w, n, line)
			}
		}
	}
}

func TestChartFlagsDoNotDrawOffATerminal(t *testing.T) {
	var f chartFlags
	f.on = true
	if f.drawing(termui.Caps{TTY: false, Width: 80}) {
		t.Fatal("the chart would have been drawn into a pipe")
	}
	if !f.drawing(termui.Caps{TTY: true, Width: 80}) {
		t.Fatal("the chart refused to draw on a terminal")
	}
	f.on = false
	if f.drawing(termui.Caps{TTY: true, Width: 80}) {
		t.Fatal("-chart=false still drew")
	}
}
