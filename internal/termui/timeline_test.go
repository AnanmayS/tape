package termui

import (
	"strings"
	"testing"
	"time"
)

var base = time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

func TestTimelineEmpty(t *testing.T) {
	var tl Timeline
	if !tl.Empty() {
		t.Fatal("a fresh timeline is not empty")
	}
	if tl.Span() != 0 {
		t.Fatalf("span %s on an empty timeline", tl.Span())
	}
	d, m := tl.Columns(10)
	if len(d) != 10 || len(m) != 10 {
		t.Fatalf("empty timeline gave %d density and %d mark columns, want 10 each", len(d), len(m))
	}
	for i := range d {
		if d[i] != 0 || m[i] != 0 {
			t.Fatalf("column %d of an empty timeline is not empty", i)
		}
	}
}

func TestTimelineIgnoresZeroTimes(t *testing.T) {
	var tl Timeline
	tl.Add(time.Time{})
	tl.Mark(time.Time{}, MarkGap)
	if !tl.Empty() {
		t.Fatal("a zero time was recorded; it would stretch the axis to the epoch")
	}
}

func TestTimelineBuckets(t *testing.T) {
	var tl Timeline
	// One message a second for a hundred seconds.
	for i := range 100 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	if got, want := tl.Span(), 99*time.Second; got != want {
		t.Fatalf("span %s, want %s", got, want)
	}

	density, _ := tl.Columns(10)
	var total int64
	for _, n := range density {
		total += n
	}
	if total != 100 {
		t.Fatalf("columns hold %d messages, want 100: bucketing lost some", total)
	}
	// Uniform arrivals: every column should hold roughly a tenth.
	for i, n := range density {
		if n < 8 || n > 12 {
			t.Errorf("column %d holds %d of a uniform 100, want about 10", i, n)
		}
	}
}

func TestTimelineDensityFollowsTheData(t *testing.T) {
	var tl Timeline
	// Busy for the first ten seconds, quiet for the next ninety.
	for i := range 1000 {
		tl.Add(at(time.Duration(i) * 10 * time.Millisecond))
	}
	for i := range 9 {
		tl.Add(at(time.Duration(10+i*10) * time.Second))
	}
	density, _ := tl.Columns(10)
	if density[0] <= density[5] {
		t.Fatalf("the busy end is not the tall end: %v", density)
	}

	caps := Caps{Unicode: true, Width: 80}
	rows := tl.Render(caps, 40)
	if len(rows) != 4 {
		t.Fatalf("Render gave %d rows, want density, marks, axis, legend", len(rows))
	}
	if !strings.HasPrefix(rows[0], "█") {
		t.Fatalf("the busiest column is not full: %q", rows[0])
	}
}

func TestTimelineGapLandsInItsColumn(t *testing.T) {
	var tl Timeline
	// A hundred seconds of traffic with a gap three quarters of the way in.
	for i := range 100 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	tl.Mark(at(75*time.Second), MarkGap)

	_, marks := tl.Columns(10)
	// 75s of a 99s span, ten columns: floor(75/99*10) = 7.
	for i, m := range marks {
		want := Mark(0)
		if i == 7 {
			want = MarkGap
		}
		if m != want {
			t.Errorf("column %d has mark %d, want %d", i, m, want)
		}
	}

	t.Run("and at the very end", func(t *testing.T) {
		var tl Timeline
		tl.Add(at(0))
		tl.Add(at(100 * time.Second))
		tl.Mark(at(100*time.Second), MarkGap)
		_, marks := tl.Columns(10)
		if marks[9] != MarkGap {
			t.Fatalf("a gap at the last instant did not land in the last column: %v", marks)
		}
	})

	t.Run("and at the very start", func(t *testing.T) {
		var tl Timeline
		tl.Add(at(0))
		tl.Add(at(100 * time.Second))
		tl.Mark(at(0), MarkGap)
		_, marks := tl.Columns(10)
		if marks[0] != MarkGap {
			t.Fatalf("a gap at the first instant did not land in the first column: %v", marks)
		}
	})
}

func TestTimelineGapOutranksOtherMarks(t *testing.T) {
	var tl Timeline
	for i := range 10 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	// A file boundary and a reseed beside the gap, all in one column.
	tl.Mark(at(5*time.Second), MarkFile)
	tl.Mark(at(5*time.Second+10*time.Millisecond), MarkGap)
	tl.Mark(at(5*time.Second+20*time.Millisecond), MarkReseed)

	_, marks := tl.Columns(9)
	found := false
	for _, m := range marks {
		if m == MarkGap {
			found = true
		}
	}
	if !found {
		t.Fatalf("the gap was hidden behind another mark: %v", marks)
	}
}

func TestTimelineGapInAnEmptyColumnIsStillDrawn(t *testing.T) {
	var tl Timeline
	// Traffic at both ends, silence in the middle — which is what a gap looks
	// like — and the gap record sitting in the silence.
	for i := range 20 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	for i := range 20 {
		tl.Add(at(time.Duration(80+i) * time.Second))
	}
	tl.Mark(at(50*time.Second), MarkGap)

	const width = 30
	density, marks := tl.Columns(width)
	col := -1
	for i, m := range marks {
		if m == MarkGap {
			col = i
		}
	}
	if col < 0 {
		t.Fatal("no gap column")
	}
	if density[col] != 0 {
		t.Fatalf("column %d was meant to be the empty one, holds %d", col, density[col])
	}

	rows := tl.Render(Caps{Unicode: true}, width)
	cells := []rune(stripEscapes(rows[0]))
	if cells[col] == ' ' {
		t.Fatalf("the gap column rendered as a space: %q", string(cells))
	}
	if !strings.Contains(rows[1], "!") {
		t.Fatalf("the marker row has no gap marker: %q", rows[1])
	}
}

func TestTimelineCoarsensWithoutLosingCount(t *testing.T) {
	var tl Timeline
	// Ten thousand messages over an hour: far more than maxBuckets at the base
	// resolution, so the accumulator has to halve repeatedly.
	const n = 10000
	for i := range n {
		tl.Add(at(time.Duration(i) * 360 * time.Millisecond))
	}
	if len(tl.counts) > maxBuckets {
		t.Fatalf("%d buckets, want at most %d", len(tl.counts), maxBuckets)
	}
	if tl.res <= baseRes {
		t.Fatalf("resolution stayed at %s; nothing coarsened", tl.res)
	}
	density, _ := tl.Columns(60)
	var total int64
	for _, c := range density {
		total += c
	}
	if total != n {
		t.Fatalf("coarsening lost messages: %d of %d survived", total, n)
	}
}

func TestTimelineOutOfOrderIsClamped(t *testing.T) {
	var tl Timeline
	tl.Add(at(time.Second))
	tl.Add(at(0)) // 1s earlier than the first seen
	tl.Add(at(2 * time.Second))

	if !tl.first.Equal(at(0)) {
		t.Fatalf("first time is %s, want the true minimum", tl.first)
	}
	density, _ := tl.Columns(4)
	var total int64
	for _, c := range density {
		total += c
	}
	if total != 3 {
		t.Fatalf("an out-of-order event was dropped: %d of 3", total)
	}
}

func TestTimelineRenderPlainHasNoEscapes(t *testing.T) {
	var tl Timeline
	for i := range 50 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	tl.Mark(at(20*time.Second), MarkGap)
	tl.Mark(at(30*time.Second), MarkReseed)
	tl.Mark(at(0), MarkFile)

	for _, row := range tl.Render(Caps{Unicode: true, TTY: true}, 40) {
		if strings.Contains(row, "\x1b") {
			t.Fatalf("colour off still emitted an escape code: %q", row)
		}
	}
	// And the ASCII caps must not reach for block characters.
	for _, row := range tl.Render(Caps{}, 40) {
		if strings.ContainsAny(row, "▁▂▃▄▅▆▇█│─") {
			t.Fatalf("ascii caps drew a block character: %q", row)
		}
	}
}

func TestTimelineRenderWidths(t *testing.T) {
	var tl Timeline
	for i := range 200 {
		tl.Add(at(time.Duration(i) * time.Second))
	}
	tl.Mark(at(100*time.Second), MarkGap)

	caps := Caps{Unicode: true}
	for _, w := range []int{1, 5, MinWidth, 40, 80, MaxWidth} {
		rows := tl.Render(caps, w)
		want := w
		for i, row := range rows {
			if n := len([]rune(row)); n > want {
				t.Errorf("width %d: row %d is %d columns wide, want at most %d: %q",
					w, i, n, want, row)
			}
		}
	}
}

// stripEscapes removes SGR sequences so a rendered row can be indexed by column.
func stripEscapes(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' {
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
