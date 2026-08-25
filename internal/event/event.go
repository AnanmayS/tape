// Package event defines the decoded view of a feed message and the Coinbase
// wire decoding that produces it.
//
// Decoding is a convenience, never the record of truth. The capture path stores
// the raw frame bytes alongside every event; if the decoded view and the raw
// frame ever disagree, the raw frame wins. That is why Event carries Raw.
package event

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Channel names as this project subscribes to them.
const (
	ChannelLevel2  = "level2_batch"
	ChannelMatches = "matches"
	ChannelControl = "control"
)

// Event is the decoded view of one feed frame.
//
// Price and Size are parsed from the wire strings for convenience. The wire
// strings inside Raw remain authoritative; M5's columnar encoding will use
// scaled integers rather than these floats.
//
// Side, Price and Size are populated for trade-shaped messages (match,
// last_match). An l2update carries a list of changes rather than a single
// price level, so those fields stay zero and the changes live in Raw until a
// later milestone needs them decoded.
type Event struct {
	// Type is the exchange's message type verbatim: "match", "l2update",
	// "snapshot", "subscriptions", "error", "heartbeat", ...
	Type string

	// Channel is the subscription the message belongs to.
	Channel string

	// Product is the exchange's product id, e.g. "BTC-USD". Empty on control
	// messages that are not product-scoped.
	Product string

	// Sequence is the product's full-channel sequence number. HasSequence is
	// false when the message carries none, which is the case for every
	// level2_batch message.
	Sequence    uint64
	HasSequence bool

	// ExchangeTime is the timestamp the exchange stamped on the message; zero
	// if it carried none.
	ExchangeTime time.Time

	// RecvTime is when this process read the frame off the socket.
	RecvTime time.Time

	Side  string
	Price float64
	Size  float64

	// Raw is the frame exactly as it came off the wire.
	Raw []byte
}

// IsControl reports whether the message is protocol chatter rather than market
// data. Control messages are still captured; they are just not market events.
func (e Event) IsControl() bool {
	switch e.Type {
	case "subscriptions", "error", "heartbeat", "":
		return true
	default:
		return false
	}
}

// IsError reports whether the exchange rejected something.
func (e Event) IsError() bool { return e.Type == "error" }

// wire is the subset of the Coinbase Exchange WebSocket schema this project
// reads. Numeric market values arrive as strings; sequence and trade ids do not.
type wire struct {
	Type      string  `json:"type"`
	ProductID string  `json:"product_id"`
	Sequence  *uint64 `json:"sequence"`
	Time      string  `json:"time"`
	Side      string  `json:"side"`
	Price     string  `json:"price"`
	Size      string  `json:"size"`
	Message   string  `json:"message"`
	Reason    string  `json:"reason"`
}

// Decode parses a Coinbase frame into an Event, attaching recv as the local
// receive time and keeping raw verbatim.
//
// A frame that fails to parse is an error the caller must surface, not swallow:
// an undecodable frame means the feed changed shape under us.
func Decode(raw []byte, recv time.Time) (Event, error) {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return Event{Raw: raw, RecvTime: recv}, fmt.Errorf("event: decode frame: %w", err)
	}

	e := Event{
		Type:     w.Type,
		Channel:  channelFor(w.Type),
		Product:  w.ProductID,
		RecvTime: recv,
		Side:     w.Side,
		Raw:      raw,
	}
	if w.Sequence != nil {
		e.Sequence, e.HasSequence = *w.Sequence, true
	}
	if w.Time != "" {
		t, err := time.Parse(time.RFC3339Nano, w.Time)
		if err != nil {
			return e, fmt.Errorf("event: parse time %q: %w", w.Time, err)
		}
		e.ExchangeTime = t.UTC()
	}
	if w.Price != "" {
		v, err := strconv.ParseFloat(w.Price, 64)
		if err != nil {
			return e, fmt.Errorf("event: parse price %q: %w", w.Price, err)
		}
		e.Price = v
	}
	if w.Size != "" {
		v, err := strconv.ParseFloat(w.Size, 64)
		if err != nil {
			return e, fmt.Errorf("event: parse size %q: %w", w.Size, err)
		}
		e.Size = v
	}
	return e, nil
}

// ErrorText returns the exchange's complaint for an error frame, or "".
func ErrorText(raw []byte) string {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return ""
	}
	if w.Type != "error" {
		return ""
	}
	if w.Reason != "" {
		return w.Message + ": " + w.Reason
	}
	return w.Message
}

func channelFor(msgType string) string {
	switch msgType {
	case "match", "last_match":
		return ChannelMatches
	case "snapshot", "l2update":
		return ChannelLevel2
	default:
		return ChannelControl
	}
}
