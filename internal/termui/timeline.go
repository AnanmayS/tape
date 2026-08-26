package termui

import (
	"strings"
	"time"
)

// A timeline answers the question a summary cannot: not "how many messages are
// in this window" but "what shape is it". A window that lost its connection for
// ten seconds has the same counts as a healthy one either side of the hole, and
// the hole is the whole story. Drawing density against time puts it where it
// happened, and puts the gap marker under it.
//
// The accumulator streams. A window is read once, records arrive one at a time,
// and the span is not known until the last one — so buckets start fine and
// coarsen by halving whenever they would outgrow maxBuckets. The result is a
// histogram whose resolution is always within a factor of two of the finest one
// that fits, at a fixed memory cost, from a single pass.

const (
	// maxBuckets bounds the accumulator. It is far more than any terminal can
	// draw; the surplus is what keeps the final halving from being visible when
	// buckets are folded down into columns.
	maxBuckets = 4096

	// baseRes is the finest bucket. A three-minute window coarsens to 32ms, a
	// day to 32 seconds — in both cases many times finer than one column.
	baseRes = time.Millisecond
)

// Mark is something worth pointing at on the timeline. The values are ordered
// by how much they matter: when several land in one column, the highest wins,
// so a gap is never hidden behind a file boundary that happened beside it.
type Mark uint8

const (
	// MarkFile is where a file in the window starts.
	MarkFile Mark = iota + 1

	// MarkReseed is a resubscription: not a loss, but a boundary nothing is
	// continuous across.
	MarkReseed

	// MarkGap is a stored gap record. Messages are missing here and no backfill
	// exists, which makes the window untrustworthy from this column on.
	MarkGap
)

type markAt struct {
	at   time.Time
	kind Mark
}

// Timeline accumulates event times into equal-width buckets and draws them.
//
// The zero value is ready to use.
type Timeline struct {
	res    time.Duration
	origin time.Time // the instant bucket 0 begins
	counts []int64
	total  int64

	first, last time.Time
	marks       []markAt
}

// Add records one event at t. A zero time is ignored: a record with no clock on
// it has no position on a timeline, and putting it at the epoch would stretch
// the axis across fifty years to accommodate it.
//
// An event before the first one seen is clamped into bucket 0. Records arrive
// in replay order, which is exchange order, while this axis is receive time, so
// a handful of events can arrive a few tens of milliseconds out of sequence.
// That is far below one column and the clamp is invisible; the axis labels come
// from the true first and last times, not from the bucket origin.
func (t *Timeline) Add(at time.Time) {
	if at.IsZero() {
		return
	}
	if t.counts == nil {
		t.res, t.origin = baseRes, at
		t.first, t.last = at, at
		t.counts = make([]int64, 1, 64)
	}
	if at.Before(t.first) {
		t.first = at
	}
	if at.After(t.last) {
		t.last = at
	}

	idx := int(at.Sub(t.origin) / t.res)
	if idx < 0 {
		idx = 0
	}
	for idx >= maxBuckets {
		t.coarsen()
		idx = int(at.Sub(t.origin) / t.res)
	}
	for len(t.counts) <= idx {
		t.counts = append(t.counts, 0)
	}
	t.counts[idx]++
	t.total++
}

// Mark records a landmark at t. Marks do not contribute to density; a gap
// record is a hole in the data, not a message in it.
func (t *Timeline) Mark(at time.Time, kind Mark) {
	if at.IsZero() {
		return
	}
	t.marks = append(t.marks, markAt{at: at, kind: kind})
	if t.first.IsZero() || at.Before(t.first) {
		t.first = at
	}
	if at.After(t.last) {
		t.last = at
	}
}

// coarsen halves the resolution by folding each pair of buckets into one.
func (t *Timeline) coarsen() {
	n := (len(t.counts) + 1) / 2
	for i := range n {
		v := t.counts[2*i]
		if 2*i+1 < len(t.counts) {
			v += t.counts[2*i+1]
		}
		t.counts[i] = v
	}
	t.counts = t.counts[:n]
	t.res *= 2
}

// Empty reports whether nothing has been added. An empty timeline has no shape
// to draw and the caller should print nothing rather than an empty chart.
func (t *Timeline) Empty() bool { return t.total == 0 && len(t.marks) == 0 }

// Span is the wall-clock time the timeline covers.
func (t *Timeline) Span() time.Duration {
	if t.first.IsZero() || t.last.IsZero() {
		return 0
	}
	return t.last.Sub(t.first)
}

// Columns folds the buckets into width columns of density, and returns the
// mark, if any, occupying each column.
//
// It is separate from Render because the bucketing is the part worth testing on
// its own: that a gap lands in the column its timestamp belongs to is a claim
// about arithmetic, not about escape codes.
func (t *Timeline) Columns(width int) (density []int64, marks []Mark) {
	if width <= 0 {
		return nil, nil
	}
	density = make([]int64, width)
	marks = make([]Mark, width)
	if t.Empty() {
		return density, marks
	}

	for i, n := range t.counts {
		if n == 0 {
			continue
		}
		density[t.column(t.origin.Add(time.Duration(i)*t.res), width)] += n
	}
	for _, m := range t.marks {
		c := t.column(m.at, width)
		if m.kind > marks[c] {
			marks[c] = m.kind
		}
	}
	return density, marks
}

// column is where instant at falls on an axis width columns wide.
func (t *Timeline) column(at time.Time, width int) int {
	span := t.Span()
	if span <= 0 {
		return 0
	}
	c := int(float64(at.Sub(t.first)) / float64(span) * float64(width))
	if c < 0 {
		return 0
	}
	if c >= width {
		return width - 1
	}
	return c
}

// Render draws the timeline as four lines, each at most width columns wide:
// density, marks, the time axis, and a legend. The caller supplies any
// indentation; nothing here knows what it is being printed beside.
func (t *Timeline) Render(caps Caps, width int) []string {
	if width <= 0 {
		width = MinWidth
	}
	density, marks := t.Columns(width)

	var max int64
	for _, n := range density {
		if n > max {
			max = n
		}
	}

	var bars, row strings.Builder
	for i, n := range density {
		cell := " "
		if n > 0 {
			cell = string(caps.Glyph(Level(float64(n), float64(max))))
		}
		// A gap column is red whether or not any message landed in it. A gap
		// that fell in an otherwise empty column is the most important one on
		// the chart, and it would be the one drawn as a space.
		if marks[i] == MarkGap {
			if n == 0 {
				cell = string(caps.Glyph(RampLevels - 1))
			}
			cell = caps.Bold(ColorRed, cell)
		}
		bars.WriteString(cell)
		row.WriteString(caps.markCell(marks[i]))
	}

	return []string{
		bars.String(),
		strings.TrimRight(row.String(), " "),
		t.axis(caps, width),
		t.legend(caps, width, max),
	}
}

func (c Caps) markCell(m Mark) string {
	switch m {
	case MarkGap:
		return c.Bold(ColorRed, "!")
	case MarkReseed:
		return c.Paint(ColorYellow, "^")
	case MarkFile:
		if c.Unicode {
			return c.Paint(ColorDim, "│")
		}
		return c.Paint(ColorDim, "|")
	default:
		return " "
	}
}

// axis labels the two ends of the window. Only the clock time is shown: the
// date is already in the summary above the chart and in the file names, and
// repeating it here would cost a third of the axis.
func (t *Timeline) axis(caps Caps, width int) string {
	const clock = "15:04:05"
	left := t.first.UTC().Format(clock)
	right := t.last.UTC().Format(clock)
	fill := width - len(left) - len(right)
	if fill < 1 {
		return Truncate(left+" "+right, width)
	}
	return left + caps.Paint(ColorDim, caps.Rule(fill)) + right
}

func (t *Timeline) legend(caps Caps, width int, max int64) string {
	per := t.Span() / time.Duration(width)
	parts := []string{
		caps.LegendRamp() + " 1-" + Count(max) + " per " + Short(per.Round(time.Millisecond)),
	}
	if t.hasMark(MarkGap) {
		parts = append(parts, "! gap")
	}
	if t.hasMark(MarkReseed) {
		parts = append(parts, "^ reseed")
	}
	if t.hasMark(MarkFile) {
		if caps.Unicode {
			parts = append(parts, "│ file")
		} else {
			parts = append(parts, "| file")
		}
	}
	return caps.Paint(ColorDim, Truncate(strings.Join(parts, "   "), width))
}

func (t *Timeline) hasMark(kind Mark) bool {
	for _, m := range t.marks {
		if m.kind == kind {
			return true
		}
	}
	return false
}
