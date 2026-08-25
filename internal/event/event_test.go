package event

import (
	"bytes"
	"testing"
	"time"
)

// Fixtures captured from a live wss://ws-feed.exchange.coinbase.com session on
// 2026-08-25 with channels level2_batch + matches, product BTC-USD.
const (
	matchFrame = `{"type":"match","trade_id":1081862675,"maker_order_id":"f07e510f-4edf-469d-b844-4f670f1e93f1","taker_order_id":"e6f50944-3e2a-439e-ae49-308300ba8296","side":"sell","size":"0.00000001","price":"80691.53","product_id":"BTC-USD","sequence":134905860111,"time":"2026-08-25T03:35:57.198335Z"}`

	lastMatchFrame = `{"type":"last_match","trade_id":1081862674,"maker_order_id":"f07e510f-4edf-469d-b844-4f670f1e93f1","taker_order_id":"55c8b85c-75ae-4cd5-9775-eba950fc4605","side":"sell","size":"0.00000001","price":"80691.53","product_id":"BTC-USD","sequence":134905859981,"time":"2026-08-25T03:35:57.096368Z"}`

	l2updateFrame = `{"type":"l2update","product_id":"BTC-USD","changes":[["buy","80688.08","0.59153273"],["sell","80745.16","0.20558981"]],"time":"2026-08-25T03:35:57.300000Z"}`

	snapshotFrame = `{"type":"snapshot","product_id":"BTC-USD","asks":[["80691.53","0.20084671"]],"bids":[["80688.08","0.59153273"]]}`

	subscriptionsFrame = `{"type":"subscriptions","channels":[{"name":"level2_50","product_ids":["BTC-USD"],"account_ids":null},{"name":"matches","product_ids":["BTC-USD"],"account_ids":null}]}`

	// Returned when subscribing to level2 without authentication. Recorded
	// live; it is why this project subscribes to level2_batch.
	errorFrame = `{"type":"error","message":"Failed to subscribe","reason":"level2, level3, and full channels now require authentication. https://docs.cloud.coinbase.com/exchange/docs/websocket-auth"}`
)

func TestDecodeMatch(t *testing.T) {
	recv := time.Date(2026, 8, 25, 3, 35, 57, 250000000, time.UTC)
	e, err := Decode([]byte(matchFrame), recv)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.Type != "match" {
		t.Errorf("Type = %q", e.Type)
	}
	if e.Channel != ChannelMatches {
		t.Errorf("Channel = %q, want %q", e.Channel, ChannelMatches)
	}
	if e.Product != "BTC-USD" {
		t.Errorf("Product = %q", e.Product)
	}
	if !e.HasSequence || e.Sequence != 134905860111 {
		t.Errorf("Sequence = %d has=%v", e.Sequence, e.HasSequence)
	}
	want := time.Date(2026, 8, 25, 3, 35, 57, 198335000, time.UTC)
	if !e.ExchangeTime.Equal(want) {
		t.Errorf("ExchangeTime = %v, want %v", e.ExchangeTime, want)
	}
	if !e.RecvTime.Equal(recv) {
		t.Errorf("RecvTime = %v, want %v", e.RecvTime, recv)
	}
	if e.Side != "sell" {
		t.Errorf("Side = %q", e.Side)
	}
	if e.Price != 80691.53 {
		t.Errorf("Price = %v", e.Price)
	}
	if e.Size != 0.00000001 {
		t.Errorf("Size = %v", e.Size)
	}
	if !bytes.Equal(e.Raw, []byte(matchFrame)) {
		t.Error("Raw was not preserved verbatim")
	}
	if e.IsControl() {
		t.Error("match should not be a control message")
	}
}

func TestDecodeLastMatchIsMatchesChannel(t *testing.T) {
	e, err := Decode([]byte(lastMatchFrame), time.Now())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.Channel != ChannelMatches {
		t.Errorf("Channel = %q, want %q", e.Channel, ChannelMatches)
	}
	if !e.HasSequence || e.Sequence != 134905859981 {
		t.Errorf("Sequence = %d has=%v", e.Sequence, e.HasSequence)
	}
}

// level2_batch messages carry no sequence number at all. Gap detection must be
// able to tell "no sequence" from "sequence zero".
func TestDecodeLevel2HasNoSequence(t *testing.T) {
	for _, frame := range []string{l2updateFrame, snapshotFrame} {
		e, err := Decode([]byte(frame), time.Now())
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if e.HasSequence {
			t.Errorf("%s: HasSequence = true, want false", e.Type)
		}
		if e.Channel != ChannelLevel2 {
			t.Errorf("%s: Channel = %q, want %q", e.Type, e.Channel, ChannelLevel2)
		}
		if e.Product != "BTC-USD" {
			t.Errorf("%s: Product = %q", e.Type, e.Product)
		}
		// A batched book update has no single price level.
		if e.Price != 0 || e.Size != 0 || e.Side != "" {
			t.Errorf("%s: expected empty side/price/size, got %q %v %v", e.Type, e.Side, e.Price, e.Size)
		}
	}
}

func TestDecodeControlFrames(t *testing.T) {
	e, err := Decode([]byte(subscriptionsFrame), time.Now())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !e.IsControl() || e.Channel != ChannelControl {
		t.Errorf("subscriptions: control=%v channel=%q", e.IsControl(), e.Channel)
	}

	e, err = Decode([]byte(errorFrame), time.Now())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !e.IsError() {
		t.Error("errorFrame should decode as an error")
	}
	got := ErrorText([]byte(errorFrame))
	if got == "" || got[:19] != "Failed to subscribe" {
		t.Errorf("ErrorText = %q", got)
	}
	if ErrorText([]byte(matchFrame)) != "" {
		t.Error("ErrorText on a non-error frame should be empty")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte(`not json`), time.Now()); err == nil {
		t.Fatal("expected an error for non-JSON input")
	}
	if _, err := Decode([]byte(`{"type":"match","time":"nope"}`), time.Now()); err == nil {
		t.Fatal("expected an error for an unparseable timestamp")
	}
	if _, err := Decode([]byte(`{"type":"match","price":"1o.5"}`), time.Now()); err == nil {
		t.Fatal("expected an error for an unparseable price")
	}
}

// Consecutive matches are not consecutive sequence numbers: the sequence is the
// product's full-channel sequence and the matches channel sees only a subset of
// it. This is recorded as a test because it is the reason gap detection on this
// feed checks monotonicity rather than contiguity.
func TestMatchSequencesAreNotContiguous(t *testing.T) {
	a, err := Decode([]byte(lastMatchFrame), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b, err := Decode([]byte(matchFrame), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if b.Sequence <= a.Sequence {
		t.Fatalf("sequences should increase: %d then %d", a.Sequence, b.Sequence)
	}
	if b.Sequence == a.Sequence+1 {
		t.Fatal("fixture no longer demonstrates the non-contiguous sequence gap")
	}
}
