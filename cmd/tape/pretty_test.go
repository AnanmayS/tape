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

func tradeRecord(side, price, size string) replay.Record {
	return replay.Record{
		Kind: replay.KindMessage,
		Event: event.Event{
			Type:         "match",
			Channel:      event.ChannelMatches,
			Product:      "BTC-USD",
			Sequence:     134992184727,
			HasSequence:  true,
			ExchangeTime: chartAt(time.Second),
			RecvTime:     chartAt(time.Second + 3*time.Millisecond),
			Side:         side,
			PriceText:    price,
			SizeText:     size,
		},
	}
}

func renderOne(caps termui.Caps, rec replay.Record) string {
	var buf bytes.Buffer
	e := newPrettyEncoder(&buf, caps)
	if err := e.Encode(rec); err != nil {
		panic(err)
	}
	// Drop the header, which Encode writes once above the first record.
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	return lines[len(lines)-1]
}

var wideColor = termui.Caps{TTY: true, Color: true, Unicode: true, Width: 100}
var widePlain = termui.Caps{TTY: true, Color: false, Unicode: true, Width: 100}

func TestPrettyNeverEmitsEscapesWithColorOff(t *testing.T) {
	records := []replay.Record{
		tradeRecord("buy", "79038.31", "0.06331145"),
		tradeRecord("sell", "79038.30", "1.5"),
		gapAt(time.Minute, 0),
		reseedAt(time.Minute, 0, false),
		reseedAt(0, 0, true),
		{Kind: replay.KindMessage, DecodeError: "unexpected end of JSON input",
			Event: event.Event{Type: "match", RecvTime: chartAt(0)}},
	}
	var buf bytes.Buffer
	e := newPrettyEncoder(&buf, widePlain)
	for _, rec := range records {
		if err := e.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(buf.String(), "\x1b") {
		t.Fatalf("colour off still emitted an escape code:\n%q", buf.String())
	}

	// And with an ASCII locale, no block or box characters either.
	buf.Reset()
	ascii := newPrettyEncoder(&buf, termui.Caps{TTY: true, Width: 100})
	for _, rec := range records {
		if err := ascii.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	if strings.ContainsAny(buf.String(), "━─│█") {
		t.Fatalf("a non-UTF-8 terminal was sent box characters:\n%s", buf.String())
	}
}

func TestPrettyPrintsTheExchangesDecimals(t *testing.T) {
	// The trailing zero is the whole point: a float cannot tell 80691.5 from
	// 80691.50, and the display must not be where the distinction is lost.
	row := renderOne(widePlain, tradeRecord("buy", "80691.50", "0.00000001"))
	if !strings.Contains(row, "80691.50") {
		t.Fatalf("the exchange's price characters are not in the row: %q", row)
	}
	if strings.Contains(row, "80691.5 ") {
		t.Fatalf("the price was re-rendered from a float: %q", row)
	}
	if !strings.Contains(row, "0.00000001") {
		t.Fatalf("the size lost its digits: %q", row)
	}
}

func TestPrettySideColors(t *testing.T) {
	buy := renderOne(wideColor, tradeRecord("buy", "80691.50", "1"))
	sell := renderOne(wideColor, tradeRecord("sell", "80691.50", "1"))
	if !strings.Contains(buy, "\x1b[32mbuy") {
		t.Fatalf("a buy was not green: %q", buy)
	}
	if !strings.Contains(sell, "\x1b[31msell") {
		t.Fatalf("a sell was not red: %q", sell)
	}

	// A record with no side — every level2_batch update — is painted neither.
	l2 := replay.Record{Kind: replay.KindMessage, Event: event.Event{
		Type: "l2update", Channel: event.ChannelLevel2, ExchangeTime: chartAt(0)}}
	if row := renderOne(wideColor, l2); strings.Contains(row, "\x1b[32m") || strings.Contains(row, "\x1b[31m") {
		t.Fatalf("a book update was given a side colour: %q", row)
	}
}

func TestPrettyGapIsAFullWidthBanner(t *testing.T) {
	row := renderOne(wideColor, gapAt(time.Minute, 0))
	if !strings.HasPrefix(row, "\x1b[1m\x1b[31m") {
		t.Fatalf("the gap banner is not bold red: %q", row)
	}
	plain := stripSGR(row)
	if n := len([]rune(plain)); n != wideColor.Width {
		t.Fatalf("the gap banner is %d columns, want the full %d: %q", n, wideColor.Width, plain)
	}
	for _, want := range []string{"GAP", "649 messages missing", "expected 100", "got 749"} {
		if !strings.Contains(plain, want) {
			t.Fatalf("the gap banner does not say %q: %q", want, plain)
		}
	}
}

func TestPrettyDroppedGapSaysWhoLostThem(t *testing.T) {
	rec := replay.Record{Kind: replay.KindGap, Gap: tapefile.Gap{At: chartAt(0), Dropped: 4096}}
	plain := stripSGR(renderOne(widePlain, rec))
	if !strings.Contains(plain, "capture dropped 4,096 frames") {
		t.Fatalf("a drop was reported as a sequence gap: %q", plain)
	}
}

func TestPrettyReseedBanners(t *testing.T) {
	opening := stripSGR(renderOne(widePlain, reseedAt(0, 0, true)))
	if !strings.Contains(opening, "window opens") {
		t.Fatalf("the opening reseed is not labelled as the window's start: %q", opening)
	}
	if strings.Contains(opening, "RESEED") {
		t.Fatalf("the reseed that opens a window was reported as a break: %q", opening)
	}

	mid := renderOne(wideColor, reseedAt(time.Minute, 0, false))
	if !strings.Contains(mid, "\x1b[33m") {
		t.Fatalf("a mid-window reseed is not yellow: %q", mid)
	}
	if !strings.Contains(stripSGR(mid), "the book was rebuilt here") {
		t.Fatalf("the reseed banner does not say what it means: %q", stripSGR(mid))
	}
	if n := len([]rune(stripSGR(mid))); n != wideColor.Width {
		t.Fatalf("the reseed banner is %d columns, want %d", n, wideColor.Width)
	}
}

func TestPrettyKeepsAnUndecodableFrameVisible(t *testing.T) {
	rec := replay.Record{
		Kind:        replay.KindMessage,
		DecodeError: "unexpected end of JSON input",
		Event:       event.Event{RecvTime: chartAt(0)},
	}
	row := stripSGR(renderOne(widePlain, rec))
	if !strings.Contains(row, "undecodable: unexpected end of JSON input") {
		t.Fatalf("a frame that would not parse was rendered as an ordinary row: %q", row)
	}
}

func TestPrettyLayoutDropsColumnsRatherThanWrapping(t *testing.T) {
	full := layoutFor(200)
	if !full.channel || !full.typ || !full.seq {
		t.Fatalf("a wide terminal did not get every column: %+v", full)
	}
	if l := layoutFor(prettyMinWidth); l.channel || l.seq || l.typ {
		t.Fatalf("a very narrow terminal kept columns it has no room for: %+v", l)
	}
	if prettyMinWidth > termui.DefaultWidth {
		t.Fatalf("the minimum row is %d columns, wider than the assumed terminal", prettyMinWidth)
	}

	// Below prettyMinWidth there is nothing left to drop: the four remaining
	// columns are the record, and narrowing them would truncate a decimal.
	for _, w := range []int{prettyMinWidth, 60, 80, 100, termui.MaxWidth} {
		caps := termui.Caps{TTY: true, Unicode: true, Width: w}
		var buf bytes.Buffer
		e := newPrettyEncoder(&buf, caps)
		for _, rec := range []replay.Record{
			reseedAt(0, 0, true),
			tradeRecord("sell", "134992.31", "0.06331145"),
			gapAt(time.Minute, 0),
		} {
			if err := e.Encode(rec); err != nil {
				t.Fatal(err)
			}
		}
		for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
			if n := len([]rune(line)); n > w {
				t.Errorf("width %d: a row is %d columns: %q", w, n, line)
			}
		}
	}
}

func TestPrettyHeaderIsWrittenOnce(t *testing.T) {
	var buf bytes.Buffer
	e := newPrettyEncoder(&buf, widePlain)
	for range 3 {
		if err := e.Encode(tradeRecord("buy", "1", "1")); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), "sequence"); n != 1 {
		t.Fatalf("the header was written %d times, want 1:\n%s", n, buf.String())
	}
	if !strings.HasPrefix(buf.String(), "time ") {
		t.Fatalf("the stream does not open with the header:\n%s", buf.String())
	}
}

func TestPrettyRejectsAnUnknownKind(t *testing.T) {
	var buf bytes.Buffer
	e := newPrettyEncoder(&buf, widePlain)
	if err := e.Encode(replay.Record{}); err == nil {
		t.Fatal("a record of no kind was rendered as something")
	}
}
