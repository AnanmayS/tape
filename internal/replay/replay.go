// Package replay reads stored tape files back in a fixed total order.
//
// The whole project rests on one property: replaying the same stored window
// twice produces byte-identical output. Everything in this package exists to
// make that true and to make a violation loud rather than subtle.
//
// # The total order
//
// A window's records are delivered sorted on, in order:
//
//  1. exchange timestamp
//  2. sequence rank — records with no sequence sort before records with one
//  3. sequence number
//  4. channel, compared byte-wise
//  5. arrival index — (index of the file in the window's sorted file list,
//     ordinal of the record inside that file)
//
// Every one of those comes from bytes on disk or from a record's position, so
// the order is the same on every machine. The arrival index is unique across a
// window, which makes the key a strict total order: there is no residual tie
// for a sort to break by map iteration, goroutine arrival or luck.
//
// Two sub-rules cover the records that do not carry a full key of their own,
// and they are rules rather than accidents:
//
// Records with no sequence. Every level2_batch message and every control frame
// arrives without one. Absence is not sequence zero — treating it as zero would
// sort a book update as if it preceded every trade ever. Such records get rank
// 0 and are grouped ahead of sequenced records sharing the same exchange
// timestamp, so a book update stamped at instant T is delivered before the
// sequenced trades stamped at T. The grouping is a convention; what matters is
// that it is written down, applies to stored fields only, and never changes.
//
// Records with no exchange timestamp. Coinbase snapshot and subscriptions
// frames carry none, and gap and reseed records carry only a local receive
// time, which is a different clock and must not be mixed into an exchange-time
// key. Such a record is pinned: it inherits the ordering content of the last
// record before it, in arrival order, that had ordering information of its own,
// and its own arrival index puts it immediately after that record. A reseed
// therefore lands exactly where the reconnect happened rather than at the front
// of the window, and the inheritance chain runs across file boundaries.
//
// # Ordering is streamed, and failure is loud
//
// A caller never has to hold a window in memory. Files are read in sorted order
// through a bounded reorder buffer: it holds at most DefaultReorderWindow
// records, or whatever WithReorderWindow sets, and every record it emits is
// checked against the last one emitted.
//
// Stored order is close to sorted order but not equal to it — a level2_batch
// update stamped at T can arrive after a match stamped at T+40ms — and the
// buffer absorbs exactly that. If a record ever emerges out of order, the
// buffer was too small for this window and Next returns ErrOutOfOrder. There is
// no path that emits a misordered stream quietly.
//
// # Gaps
//
// Reading past a discontinuity is the caller's decision, never the default. A
// gap record, or a reseed that is not the subscription opening the window,
// stops replay with an error naming it. WithContinueOnGap turns that into
// delivery: the gap and reseed records come back through the iterator like any
// other record and are listed by Discontinuities. Neither path can swallow one.
package replay

import (
	"time"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Kind classifies a replayed record. It mirrors the stored record types.
type Kind uint8

const (
	// KindMessage is a feed frame, decoded, with the raw bytes still attached.
	KindMessage Kind = iota + 1

	// KindGap is a stored sequence discontinuity.
	KindGap

	// KindReseed is a stored subscription boundary.
	KindReseed
)

func (k Kind) String() string {
	switch k {
	case KindMessage:
		return "message"
	case KindGap:
		return "gap"
	case KindReseed:
		return "reseed"
	default:
		return "unknown"
	}
}

// Position locates a record in the window it came from. File is relative to the
// window root so that the same window replayed from two different directories
// produces the same output.
type Position struct {
	// File is the record's file, relative to the window root.
	File string

	// FileIndex is that file's index in the window's sorted file list.
	FileIndex int

	// Record is the 0-based ordinal of the record inside the file.
	Record int64
}

// Record is one replayed record. Exactly one of Event, Gap and Reseed is
// meaningful, selected by Kind.
type Record struct {
	Kind Kind

	// Index is the record's 0-based position in the replayed output. It is a
	// property of the replay, not of the file, and it is what makes the output
	// order itself visible in the serialized stream.
	Index int64

	// Position is where the record was stored.
	Position Position

	// Event is the decoded frame, valid when Kind is KindMessage. Raw holds the
	// frame verbatim and stays authoritative.
	Event event.Event

	// DecodeError is set when the stored frame would not parse. The record is
	// still delivered: capture stored the frame because an unparseable frame is
	// still evidence, and replay will not be the place it disappears.
	DecodeError string

	// Gap is valid when Kind is KindGap.
	Gap tapefile.Gap

	// Reseed is valid when Kind is KindReseed.
	Reseed tapefile.Reseed

	// Opening marks the reseed that opens the window: a reseed with no message
	// record before it in arrival order. It is the subscription the window
	// starts from, not a break in continuity — there is nothing before it to be
	// discontinuous with — so it is delivered rather than treated as a gap.
	Opening bool
}

// Time is the record's local receive time: when this process read the frame off
// the socket, or when the gap or reseed was noticed. It is not an ordering key.
func (r Record) Time() time.Time {
	switch r.Kind {
	case KindMessage:
		return r.Event.RecvTime
	case KindGap:
		return r.Gap.At
	case KindReseed:
		return r.Reseed.At
	default:
		return time.Time{}
	}
}

// Channel is the ordering channel for the record. Gap and reseed records are
// structural rather than market data and share the control channel.
func (r Record) Channel() string {
	if r.Kind == KindMessage {
		return r.Event.Channel
	}
	return event.ChannelControl
}

// exchangeTime reports the record's own exchange timestamp, and whether it has
// one. A record without one is pinned to its arrival predecessor; see the
// package comment.
func (r Record) exchangeTime() (time.Time, bool) {
	if r.Kind != KindMessage {
		return time.Time{}, false
	}
	if r.Event.ExchangeTime.IsZero() {
		return time.Time{}, false
	}
	return r.Event.ExchangeTime, true
}
