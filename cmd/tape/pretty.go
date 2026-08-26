package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/AnanmayS/tape/internal/replay"
	"github.com/AnanmayS/tape/internal/termui"
)

// The pretty encoder is a second, separate rendering of a replayed window: one
// meant to be read with eyes rather than parsed.
//
// It is not a variant of the canonical form and shares no code with it. The
// canonical NDJSON is the serialized shape the determinism invariant is stated
// about — two replays of a window produce identical bytes, and there is a
// golden digest in CI that says so — and the only safe relationship between
// that and a display format is none. `tape replay` still writes canonical
// NDJSON by default and writes exactly the bytes it always did; -pretty is a
// different stream out of a different encoder.
//
// Two rules shape the output.
//
// Prices are the exchange's characters. PriceText and SizeText are printed, not
// the float64s beside them: a float cannot tell "80691.5" from "80691.50", and
// this project stores the decimal strings precisely so that nothing downstream
// has to invent a price the exchange never sent. A display is downstream.
//
// A gap cannot be scrolled past. Gap and reseed records are full-width banner
// lines rather than rows, so a stream read at speed still stops the eye where
// continuity broke. That is invariant 2 applied to the one place a person is
// actually looking.

// prettyEncoder writes records as aligned, coloured rows.
type prettyEncoder struct {
	w    io.Writer
	caps termui.Caps
	cols prettyLayout
	head bool
}

func newPrettyEncoder(w io.Writer, caps termui.Caps) *prettyEncoder {
	return &prettyEncoder{w: w, caps: caps, cols: layoutFor(caps.Width)}
}

// prettyLayout is which columns fit. A narrow terminal drops whole columns
// rather than wrapping, and the order they go in is the order they are least
// missed: the channel is implied by the type, and the type is implied by the
// presence of a side and a price.
type prettyLayout struct {
	channel bool
	typ     bool
	seq     bool
}

// Column widths. Time is fixed; the rest are padded to the widest value the
// feed actually produces, so the columns do not move between rows.
const (
	wTime    = 12 // 14:00:01.234
	wChannel = 12 // level2_batch
	wType    = 13 // subscriptions
	wSeq     = 13 // Coinbase full-channel sequence numbers run to twelve digits
	wSide    = 4  // sell
	wPrice   = 11
	wSize    = 12
)

// prettyMinWidth is the narrowest row the format can produce: time, side, price
// and size, with every column at its full width. It does not shrink below this.
// The columns that could be narrowed are the price and the size, and narrowing
// either means truncating a decimal the exchange sent — which is the one thing
// this project will not do to make something fit.
const prettyMinWidth = wTime + 1 + wSide + 1 + wPrice + 1 + wSize

func layoutFor(width int) prettyLayout {
	full := wTime + 1 + wChannel + 1 + wType + 1 + wSeq + 1 + wSide + 1 + wPrice + 1 + wSize
	l := prettyLayout{channel: true, typ: true, seq: true}
	if width >= full {
		return l
	}
	l.channel = false
	if width >= full-wChannel-1 {
		return l
	}
	l.seq = false
	if width >= full-wChannel-1-wSeq-1 {
		return l
	}
	l.typ = false
	return l
}

// header is the column heading, written once above the first record.
func (e *prettyEncoder) header() string {
	cells := []string{termui.Pad("time", wTime)}
	if e.cols.channel {
		cells = append(cells, termui.Pad("channel", wChannel))
	}
	if e.cols.typ {
		cells = append(cells, termui.Pad("type", wType))
	}
	if e.cols.seq {
		cells = append(cells, termui.Pad("sequence", wSeq))
	}
	cells = append(cells,
		termui.Pad("side", wSide),
		termui.Pad("price", wPrice),
		termui.Pad("size", wSize))
	return e.caps.Paint(termui.ColorDim, strings.TrimRight(strings.Join(cells, " "), " "))
}

// Encode writes one record.
func (e *prettyEncoder) Encode(rec replay.Record) error {
	if !e.head {
		e.head = true
		if _, err := fmt.Fprintln(e.w, e.header()); err != nil {
			return err
		}
	}
	line, err := e.line(rec)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(e.w, line)
	return err
}

func (e *prettyEncoder) line(rec replay.Record) (string, error) {
	switch rec.Kind {
	case replay.KindMessage:
		return e.message(rec), nil
	case replay.KindGap:
		return e.gapBanner(rec), nil
	case replay.KindReseed:
		return e.reseedBanner(rec), nil
	default:
		return "", fmt.Errorf("replay: cannot render record kind %d", rec.Kind)
	}
}

// message is one market data row. Every cell is padded before it is painted,
// because an escape code has length and no width.
func (e *prettyEncoder) message(rec replay.Record) string {
	ev := rec.Event
	var b strings.Builder

	b.WriteString(e.caps.Paint(termui.ColorDim, termui.Pad(clockOf(ev.ExchangeTime, ev.RecvTime), wTime)))
	if e.cols.channel {
		b.WriteByte(' ')
		b.WriteString(e.caps.Paint(termui.ColorDim, termui.Pad(ev.Channel, wChannel)))
	}
	if e.cols.typ {
		b.WriteByte(' ')
		b.WriteString(termui.Pad(ev.Type, wType))
	}
	if e.cols.seq {
		b.WriteByte(' ')
		seq := ""
		if ev.HasSequence {
			// No thousands separators: a sequence number is an identifier, and
			// grouping its digits makes it look like a quantity and costs four
			// columns that the digits themselves need.
			seq = strconv.FormatUint(ev.Sequence, 10)
		}
		b.WriteString(e.caps.Paint(termui.ColorDim, termui.Pad(seq, wSeq)))
	}

	b.WriteByte(' ')
	b.WriteString(e.caps.Paint(sideColor(ev.Side), termui.Pad(ev.Side, wSide)))
	b.WriteByte(' ')
	// The exchange's decimal characters, right-aligned so the point lines up
	// well enough to compare two rows at a glance.
	b.WriteString(e.caps.Paint(sideColor(ev.Side), padLeft(ev.PriceText, wPrice)))
	b.WriteByte(' ')
	b.WriteString(padLeft(ev.SizeText, wSize))

	if rec.DecodeError != "" {
		// A stored frame that will not parse is still delivered, and a display
		// that dropped the fact would be the place invariant 2 leaks.
		b.WriteString("  ")
		b.WriteString(e.caps.Bold(termui.ColorRed, "undecodable: "+rec.DecodeError))
	}
	return strings.TrimRight(b.String(), " ")
}

// sideColor is green for a buy and red for a sell.
//
// Red here does not mean untrustworthy, and it is the one place in this program
// where it does not. A trade side is the one convention every reader of market
// data already has, and spelling it any other way to protect the rule would
// cost more than the rule is worth. Everything structural stays red-for-broken.
func sideColor(side string) termui.Color {
	switch side {
	case "buy":
		return termui.ColorGreen
	case "sell":
		return termui.ColorRed
	default:
		return termui.ColorNone
	}
}

// gapBanner is a whole line saying the window broke here.
func (e *prettyEncoder) gapBanner(rec replay.Record) string {
	g := rec.Gap
	var text string
	if g.Dropped > 0 {
		text = fmt.Sprintf(" GAP  capture dropped %s frames  at %s ",
			termui.Count(int64(g.Dropped)), clockOf(time.Time{}, g.At))
	} else {
		text = fmt.Sprintf(" GAP  %s messages missing  expected %s, got %s  at %s ",
			termui.Count(int64(g.Got)-int64(g.Expected)),
			termui.Count(int64(g.Expected)), termui.Count(int64(g.Got)),
			clockOf(time.Time{}, g.At))
	}
	return e.caps.Bold(termui.ColorRed, e.banner(text))
}

// reseedBanner marks a subscription. The one that opens a window is not a break
// — there is nothing before it — so it is drawn quietly; any other one is a
// boundary nothing is continuous across, and is drawn like the warning it is.
func (e *prettyEncoder) reseedBanner(rec replay.Record) string {
	r := rec.Reseed
	if rec.Opening {
		text := fmt.Sprintf(" window opens  %s  at %s ", r.Reason, clockOf(time.Time{}, r.At))
		return e.caps.Paint(termui.ColorDim, e.banner(text))
	}
	text := fmt.Sprintf(" RESEED  the book was rebuilt here  %s  at %s ",
		r.Reason, clockOf(time.Time{}, r.At))
	return e.caps.Paint(termui.ColorYellow, e.banner(text))
}

// banner centres text in a full-width rule.
func (e *prettyEncoder) banner(text string) string {
	w := e.caps.Width
	n := len([]rune(text))
	if n >= w {
		return termui.Truncate(strings.TrimSpace(text), w)
	}
	left := (w - n) / 2
	return e.caps.HeavyRule(left) + text + e.caps.HeavyRule(w-left-n)
}

// clockOf renders the exchange's timestamp, falling back to the local receive
// time when the frame carried none. Milliseconds are as fine as a person reads;
// the canonical output keeps the nanoseconds.
func clockOf(exchange, recv time.Time) string {
	t := exchange
	if t.IsZero() {
		t = recv
	}
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("15:04:05.000")
}

// padLeft right-aligns s in n columns.
func padLeft(s string, n int) string {
	w := len([]rune(s))
	if w >= n {
		return termui.Truncate(s, n)
	}
	return strings.Repeat(" ", n-w) + s
}

// prettyWriter is what runReplay writes through when -pretty is given. It is a
// separate function from the canonical path on purpose: there is no branch
// inside the canonical encoder, and no way for a flag to reach it.
func prettyReplay(w *bufio.Writer, r *replay.Reader, caps termui.Caps) error {
	enc := newPrettyEncoder(w, caps)
	return drain(r, func(rec replay.Record) error { return enc.Encode(rec) })
}
