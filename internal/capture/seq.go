package capture

import (
	"fmt"

	"github.com/AnanmayS/tape/internal/feed"
)

// Anomaly is a sequence-number event worth recording in the stream.
type Anomaly struct {
	// Expected is the sequence number that should have come next.
	Expected uint64

	// Got is the sequence number that actually arrived.
	Got uint64

	// Kind says what went wrong.
	Kind AnomalyKind
}

// AnomalyKind classifies a sequence anomaly.
type AnomalyKind uint8

const (
	// AnomalyNone means nothing was wrong.
	AnomalyNone AnomalyKind = iota

	// AnomalyGap is a skipped sequence number on a feed that promised
	// contiguity: messages were lost.
	AnomalyGap

	// AnomalyReconnectGap is a discontinuity across a reconnect. On a
	// monotonic feed this cannot be distinguished from the feed's normal
	// skipping, which is precisely why it is recorded: after a reconnect,
	// continuity is unprovable and the window is untrustworthy.
	AnomalyReconnectGap

	// AnomalyRegression is a sequence number at or below one already seen
	// outside a reseed: a duplicate or an out-of-order delivery.
	AnomalyRegression
)

func (k AnomalyKind) String() string {
	switch k {
	case AnomalyNone:
		return "none"
	case AnomalyGap:
		return "gap"
	case AnomalyReconnectGap:
		return "reconnect_gap"
	case AnomalyRegression:
		return "regression"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

// seqTracker follows the sequence number of each product and reports anomalies.
//
// The rules differ by what the feed guarantees, because pretending otherwise
// produces either false alarms or silent losses:
//
//   - Contiguous feeds: every sequence number appears, so any skip is lost data.
//
//   - Monotonic feeds (Coinbase matches, where the sequence belongs to the
//     product's busier full channel): a skip proves nothing on its own, so
//     skips are not reported. What is reported is regression, and any
//     discontinuity across a reconnect.
//
// A reseed suspends the feed's normal rules for one message per product.
// Coinbase's documented behaviour is that messages at or below the snapshot's
// sequence may arrive after a fresh subscription and should be discarded, so
// those are reported as stale rather than as anomalies. Anything above
// last+1 after a reseed is a discontinuity regardless of the feed's mode: a
// reconnect cannot promise it lost nothing.
//
// There is deliberately no branch here that advances past a missing sequence
// without returning something. Advancing quietly is the bug.
type seqTracker struct {
	mode     feed.SeqMode
	last     map[string]uint64
	seen     map[string]bool
	reseeded map[string]bool
}

func newSeqTracker(mode feed.SeqMode) *seqTracker {
	return &seqTracker{
		mode:     mode,
		last:     make(map[string]uint64),
		seen:     make(map[string]bool),
		reseeded: make(map[string]bool),
	}
}

// reseed marks every product as freshly subscribed. The next message for each
// is judged against the reseed rules rather than the feed's normal ones.
func (t *seqTracker) reseed() {
	for p := range t.seen {
		t.reseeded[p] = true
	}
	// A product first seen after this reseed is simply a first sighting, so
	// nothing needs recording for products not yet in seen.
}

// observe reports what the arrival of seq for product means.
//
// stale is true for a message the exchange re-sent from before the current
// snapshot: not an anomaly, and not something that should advance the tracker.
// The caller still stores the message; append-only means nothing is discarded.
func (t *seqTracker) observe(product string, seq uint64) (a Anomaly, stale bool) {
	last, ok := t.last[product]
	if !ok {
		t.last[product] = seq
		t.seen[product] = true
		delete(t.reseeded, product)
		return Anomaly{}, false
	}

	if t.reseeded[product] {
		delete(t.reseeded, product)
		if seq <= last {
			// Documented Coinbase behaviour after a fresh snapshot. Keep the
			// higher water mark.
			return Anomaly{}, true
		}
		t.last[product] = seq
		if seq != last+1 {
			return Anomaly{Expected: last + 1, Got: seq, Kind: AnomalyReconnectGap}, false
		}
		return Anomaly{}, false
	}

	switch {
	case seq <= last:
		// Do not move the water mark backwards; a duplicate must not make the
		// next real message look like a gap.
		return Anomaly{Expected: last + 1, Got: seq, Kind: AnomalyRegression}, false

	case seq != last+1 && t.mode == feed.SeqContiguous:
		t.last[product] = seq
		return Anomaly{Expected: last + 1, Got: seq, Kind: AnomalyGap}, false

	default:
		// Monotonic feed skipping ahead, or a contiguous feed advancing by one.
		t.last[product] = seq
		return Anomaly{}, false
	}
}
