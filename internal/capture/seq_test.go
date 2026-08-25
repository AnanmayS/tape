package capture

import (
	"testing"

	"github.com/AnanmayS/tape/internal/feed"
)

const btc = "BTC-USD"

func expectClean(t *testing.T, tr *seqTracker, seq uint64) {
	t.Helper()
	a, stale := tr.observe(btc, seq)
	if stale || a.Kind != AnomalyNone {
		t.Fatalf("seq %d: expected no anomaly, got %+v stale=%v", seq, a, stale)
	}
}

func expectAnomaly(t *testing.T, tr *seqTracker, seq uint64, kind AnomalyKind, wantExpected uint64) {
	t.Helper()
	a, stale := tr.observe(btc, seq)
	if stale {
		t.Fatalf("seq %d: expected %v, got stale", seq, kind)
	}
	if a.Kind != kind {
		t.Fatalf("seq %d: kind = %v, want %v", seq, a.Kind, kind)
	}
	if a.Expected != wantExpected || a.Got != seq {
		t.Fatalf("seq %d: anomaly = %+v, want expected=%d got=%d", seq, a, wantExpected, seq)
	}
}

func TestContiguousFeedReportsEverySkip(t *testing.T) {
	tr := newSeqTracker(feed.SeqContiguous)
	expectClean(t, tr, 100)
	expectClean(t, tr, 101)
	expectAnomaly(t, tr, 105, AnomalyGap, 102)
	// After a gap the tracker resumes from where the feed actually is, so one
	// gap does not cascade into a gap on every following message.
	expectClean(t, tr, 106)
}

func TestMonotonicFeedDoesNotInventGaps(t *testing.T) {
	tr := newSeqTracker(feed.SeqMonotonic)
	expectClean(t, tr, 134905859981)
	// A real jump observed on the live matches channel. It is not a gap: the
	// sequence belongs to the product's full channel.
	expectClean(t, tr, 134905860111)
	expectClean(t, tr, 134905860400)
}

func TestRegressionIsReportedOnBothModes(t *testing.T) {
	for _, mode := range []feed.SeqMode{feed.SeqContiguous, feed.SeqMonotonic} {
		tr := newSeqTracker(mode)
		expectClean(t, tr, 100)
		expectClean(t, tr, 101)
		expectAnomaly(t, tr, 99, AnomalyRegression, 102)
		// A duplicate must not drag the water mark backwards, or the next
		// legitimate message would look like a gap.
		if mode == feed.SeqContiguous {
			expectClean(t, tr, 102)
		}
	}
}

func TestFirstSightingIsNeverAGap(t *testing.T) {
	tr := newSeqTracker(feed.SeqContiguous)
	expectClean(t, tr, 999999)
}

// Coinbase documents that after a fresh subscription, messages at or below the
// snapshot's sequence may arrive and should be discarded. They are stale, not
// gaps -- but the message itself is still stored by the caller.
func TestStaleMessageAfterReseed(t *testing.T) {
	tr := newSeqTracker(feed.SeqMonotonic)
	expectClean(t, tr, 200)
	expectClean(t, tr, 210)
	tr.reseed()

	a, stale := tr.observe(btc, 205)
	if !stale {
		t.Fatalf("expected stale, got %+v", a)
	}
	if a.Kind != AnomalyNone {
		t.Fatalf("a stale message is not an anomaly, got %v", a.Kind)
	}
	// The water mark did not move backwards.
	expectClean(t, tr, 211)
}

// A reconnect on a monotonic feed cannot prove it lost nothing, so any
// discontinuity across it is recorded even though the same skip mid-connection
// would not be.
func TestReconnectDiscontinuityIsReportedEvenOnMonotonicFeeds(t *testing.T) {
	tr := newSeqTracker(feed.SeqMonotonic)
	expectClean(t, tr, 500)
	expectClean(t, tr, 600) // ordinary monotonic skip, not reported
	tr.reseed()
	expectAnomaly(t, tr, 900, AnomalyReconnectGap, 601)
	// Back to normal rules afterwards.
	expectClean(t, tr, 1200)
}

func TestReconnectWithoutDiscontinuityIsClean(t *testing.T) {
	tr := newSeqTracker(feed.SeqContiguous)
	expectClean(t, tr, 10)
	tr.reseed()
	expectClean(t, tr, 11)
}

func TestSequencesAreTrackedPerProduct(t *testing.T) {
	tr := newSeqTracker(feed.SeqContiguous)
	if a, _ := tr.observe("BTC-USD", 100); a.Kind != AnomalyNone {
		t.Fatal(a)
	}
	if a, _ := tr.observe("ETH-USD", 5000); a.Kind != AnomalyNone {
		t.Fatalf("a different product's sequence must not be judged against BTC's: %+v", a)
	}
	if a, _ := tr.observe("BTC-USD", 101); a.Kind != AnomalyNone {
		t.Fatal(a)
	}
	if a, _ := tr.observe("ETH-USD", 5002); a.Kind != AnomalyGap {
		t.Fatalf("ETH gap not reported: %+v", a)
	}
}

// A product first seen after a reseed has no history to be discontinuous with.
func TestReseedBeforeFirstSighting(t *testing.T) {
	tr := newSeqTracker(feed.SeqContiguous)
	tr.reseed()
	expectClean(t, tr, 42)
	expectClean(t, tr, 43)
}

// The tracker must never return "nothing happened" for a sequence it did not
// account for. This walks a wide space of transitions and asserts that every
// advance is either clean-and-contiguous, stale, or reported.
func TestNoSilentAdvancePastAMissingSequence(t *testing.T) {
	for _, mode := range []feed.SeqMode{feed.SeqContiguous, feed.SeqMonotonic} {
		for _, reseeded := range []bool{false, true} {
			for _, next := range []uint64{98, 99, 100, 101, 102, 150} {
				tr := newSeqTracker(mode)
				expectClean(t, tr, 100)
				if reseeded {
					tr.reseed()
				}
				a, stale := tr.observe(btc, next)

				switch {
				case stale:
					if next > 100 {
						t.Fatalf("mode=%v reseeded=%v next=%d: only sequences at or below the water mark can be stale", mode, reseeded, next)
					}
				case a.Kind != AnomalyNone:
					// Reported: fine, whatever it was.
				case next == 101:
					// The only silent advance permitted.
				case mode == feed.SeqMonotonic && !reseeded && next > 101:
					// A monotonic feed skipping ahead mid-connection proves
					// nothing, and is documented as unreportable.
				default:
					t.Fatalf("mode=%v reseeded=%v: advanced from 100 to %d silently", mode, reseeded, next)
				}
			}
		}
	}
}

func TestAnomalyKindStrings(t *testing.T) {
	for k, want := range map[AnomalyKind]string{
		AnomalyNone:         "none",
		AnomalyGap:          "gap",
		AnomalyReconnectGap: "reconnect_gap",
		AnomalyRegression:   "regression",
	} {
		if k.String() != want {
			t.Errorf("%d.String() = %q, want %q", k, k.String(), want)
		}
	}
}
