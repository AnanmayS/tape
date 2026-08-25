package capture

import (
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
)

// A skipped sequence number must produce a gap record in the file, not just a
// log line, and must show up in the session summary.
func TestGapRecordsLandInTheFile(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1000,
		Count:     20,
		SkipAfter: map[int]uint64{4: 7, 12: 100},
		Now:       stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})

	if sum.Gaps != 2 {
		t.Fatalf("summary gaps = %d, want 2", sum.Gaps)
	}
	if got.gaps != 2 {
		t.Fatalf("gap records on disk = %d, want 2", got.gaps)
	}
	if got.messages != 20 {
		t.Fatalf("messages on disk = %d, want 20; a gap must not cost a message", got.messages)
	}

	// The gap record must say exactly what was missing.
	want := []struct{ expected, got uint64 }{
		{1005, 1012}, // frames 0..4 are 1000..1004, then 7 are skipped
		{1020, 1120},
	}
	for i, w := range want {
		g := got.gapRecords[i]
		if g.Expected != w.expected || g.Got != w.got {
			t.Fatalf("gap %d = expected %d got %d, want expected %d got %d",
				i, g.Expected, g.Got, w.expected, w.got)
		}
		if g.At.IsZero() {
			t.Fatalf("gap %d has no wall-clock time", i)
		}
	}
}

// A severed connection must leave both a reseed record (the book was rebuilt)
// and a gap record (continuity across the break cannot be proven).
func TestSeveredConnectionLeavesReseedAndGap(t *testing.T) {
	s := &feed.Synthetic{
		ProductID:  "BTC-USD",
		Mode:       feed.SeqContiguous,
		StartSeq:   1,
		Count:      10,
		SeverAfter: map[int]bool{5: true},
		SkipAfter:  map[int]uint64{5: 40}, // messages lost while disconnected
		Now:        stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})

	if sum.Reseeds != 2 {
		t.Fatalf("reseeds = %d, want 2 (subscribe plus reconnect)", sum.Reseeds)
	}
	if got.reseeds != 2 {
		t.Fatalf("reseed records on disk = %d, want 2", got.reseeds)
	}
	if sum.Gaps != 1 || got.gaps != 1 {
		t.Fatalf("gaps: summary=%d disk=%d, want 1 each", sum.Gaps, got.gaps)
	}
	g := got.gapRecords[0]
	if g.Expected != 7 || g.Got != 47 {
		t.Fatalf("gap = expected %d got %d, want expected 7 got 47", g.Expected, g.Got)
	}
	if got.messages != 10 {
		t.Fatalf("messages on disk = %d, want 10", got.messages)
	}
}

// A monotonic feed skips sequence numbers as a matter of course, so a
// mid-connection skip must not be reported -- but a discontinuity across a
// reconnect must be, because there it cannot be told apart from real loss.
func TestMonotonicFeedReportsOnlyReconnectDiscontinuity(t *testing.T) {
	clock := time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC)

	quiet := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqMonotonic,
		StartSeq:  1000,
		Count:     20,
		Step:      130, // as the live matches channel behaves
		Now:       stepClock(clock, time.Second),
	}
	sum, got := runCapture(t, quiet, Config{Window: time.Hour, FlushInterval: time.Hour})
	if sum.Gaps != 0 || got.gaps != 0 {
		t.Fatalf("monotonic skipping produced %d gaps; it must produce none", sum.Gaps)
	}

	severed := &feed.Synthetic{
		ProductID:  "BTC-USD",
		Mode:       feed.SeqMonotonic,
		StartSeq:   1000,
		Count:      20,
		Step:       130,
		SeverAfter: map[int]bool{9: true},
		Now:        stepClock(clock, time.Second),
	}
	sum, got = runCapture(t, severed, Config{Window: time.Hour, FlushInterval: time.Hour})
	if sum.Gaps != 1 || got.gaps != 1 {
		t.Fatalf("a reconnect on a monotonic feed must record a gap: summary=%d disk=%d",
			sum.Gaps, got.gaps)
	}
}

// The gap record must be written before the message that revealed it, so a
// replayer meets the break before it meets the data that resumed after it.
func TestGapRecordPrecedesTheResumingMessage(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     6,
		SkipAfter: map[int]uint64{2: 5},
		Now:       stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, _ := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})
	order := recordOrder(t, sum.Files)

	// reseed, 3 messages, gap, then the rest.
	want := []string{"reseed", "message", "message", "message", "gap", "message", "message", "message"}
	if len(order) != len(want) {
		t.Fatalf("record order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("record order = %v, want %v", order, want)
		}
	}
}

// A capture whose sequences are perfect must write no gap records at all.
func TestNoGapsWhenSequencesAreContiguous(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     200,
		Now:       stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})
	if sum.Gaps != 0 || got.gaps != 0 {
		t.Fatalf("clean feed produced gaps: summary=%d disk=%d", sum.Gaps, got.gaps)
	}
	if got.messages != 200 {
		t.Fatalf("messages = %d, want 200", got.messages)
	}
}
