package replay

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Canonical NDJSON is the serialized form of a replayed window: one JSON object
// per record, in replay order, each terminated by a single \n. It is what
// `tape replay` writes and what the determinism test hashes, so its definition
// is part of the invariant rather than a formatting preference.
//
// What makes it canonical:
//
//   - Field order is fixed by struct declaration order. Nothing here is a Go
//     map, so no field order is ever decided by map iteration.
//   - Which fields appear is decided by the record's kind alone, never by
//     whether a value happens to be zero, except where an explicitly optional
//     field is documented below.
//   - Times are RFC 3339 with nanoseconds, in UTC, or "" when absent. A time is
//     never re-derived from the local time zone.
//   - A missing sequence is null, not 0. The two mean different things and the
//     ordering rules treat them differently.
//   - HTML escaping is off, so the bytes of a raw frame survive unchanged
//     except for JSON whitespace compaction.
//   - Paths are relative to the window root, so replaying the same window from
//     two different directories produces identical bytes.
//
// Every field is either stored on disk or derived from a record's position.
// Nothing in the output depends on the machine, the clock or the run.

// timeText renders a timestamp canonically: RFC 3339 nanoseconds in UTC, or ""
// for the zero time.
func timeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// canonMessage is the serialized form of a message record.
type canonMessage struct {
	Index    int64  `json:"i"`
	Kind     string `json:"kind"`
	File     string `json:"file"`
	Record   int64  `json:"rec"`
	Recv     string `json:"recv"`
	Exchange string `json:"exchange_time"`
	Channel  string `json:"channel"`
	Type     string `json:"type"`
	Product  string `json:"product"`
	// Sequence is null for every record that carried none.
	Sequence *uint64 `json:"sequence"`
	Side     string  `json:"side"`
	Price    float64 `json:"price"`
	Size     float64 `json:"size"`
	// DecodeError appears only when the stored frame would not parse.
	DecodeError string `json:"decode_error,omitempty"`
	// Raw is the stored frame. It is emitted verbatim when it is valid JSON,
	// and base64 in RawB64 when it is not, so an unparseable frame is visible
	// in the output instead of corrupting it. Exactly one of the two appears.
	Raw    json.RawMessage `json:"raw,omitempty"`
	RawB64 string          `json:"raw_b64,omitempty"`
}

// canonGap is the serialized form of a gap record.
type canonGap struct {
	Index    int64  `json:"i"`
	Kind     string `json:"kind"`
	File     string `json:"file"`
	Record   int64  `json:"rec"`
	Recv     string `json:"recv"`
	Expected uint64 `json:"expected"`
	Got      uint64 `json:"got"`
	Missing  int64  `json:"missing"`
	// Dropped appears only on a gap capture caused by shedding load, where the
	// sequence numbers say nothing and the count is the whole story. It is
	// optional in the sense DecodeError and RawB64 are: a documented field that
	// appears exactly when the record has one, never as a zero standing in for
	// a measurement nobody made.
	Dropped uint64 `json:"dropped,omitempty"`
}

// canonReseed is the serialized form of a reseed record.
type canonReseed struct {
	Index int64  `json:"i"`
	Kind  string `json:"kind"`
	File  string `json:"file"`
	// Record is the ordinal within the file.
	Record  int64  `json:"rec"`
	Recv    string `json:"recv"`
	Reason  string `json:"reason"`
	Opening bool   `json:"opening"`
}

// CanonicalEncoder writes records as canonical NDJSON.
type CanonicalEncoder struct {
	enc *json.Encoder
}

// NewCanonicalEncoder returns an encoder writing to w. It does not buffer;
// wrap w in a bufio.Writer for throughput.
func NewCanonicalEncoder(w io.Writer) *CanonicalEncoder {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &CanonicalEncoder{enc: enc}
}

// Encode writes one record and its terminating newline.
func (e *CanonicalEncoder) Encode(rec Record) error {
	switch rec.Kind {
	case KindMessage:
		m := canonMessage{
			Index:       rec.Index,
			Kind:        rec.Kind.String(),
			File:        rec.Position.File,
			Record:      rec.Position.Record,
			Recv:        timeText(rec.Event.RecvTime),
			Exchange:    timeText(rec.Event.ExchangeTime),
			Channel:     rec.Event.Channel,
			Type:        rec.Event.Type,
			Product:     rec.Event.Product,
			Side:        rec.Event.Side,
			Price:       rec.Event.Price,
			Size:        rec.Event.Size,
			DecodeError: rec.DecodeError,
		}
		if rec.Event.HasSequence {
			seq := rec.Event.Sequence
			m.Sequence = &seq
		}
		if json.Valid(rec.Event.Raw) {
			m.Raw = json.RawMessage(rec.Event.Raw)
		} else {
			m.RawB64 = base64.StdEncoding.EncodeToString(rec.Event.Raw)
		}
		return e.enc.Encode(m)

	case KindGap:
		return e.enc.Encode(canonGap{
			Index:    rec.Index,
			Kind:     rec.Kind.String(),
			File:     rec.Position.File,
			Record:   rec.Position.Record,
			Recv:     timeText(rec.Gap.At),
			Expected: rec.Gap.Expected,
			Got:      rec.Gap.Got,
			Missing:  int64(rec.Gap.Got) - int64(rec.Gap.Expected),
			Dropped:  rec.Gap.Dropped,
		})

	case KindReseed:
		return e.enc.Encode(canonReseed{
			Index:   rec.Index,
			Kind:    rec.Kind.String(),
			File:    rec.Position.File,
			Record:  rec.Position.Record,
			Recv:    timeText(rec.Reseed.At),
			Reason:  rec.Reseed.Reason,
			Opening: rec.Opening,
		})

	default:
		return fmt.Errorf("replay: cannot encode record kind %d", rec.Kind)
	}
}
