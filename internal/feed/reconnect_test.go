package feed

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := newBackoff(500*time.Millisecond, 4*time.Second)
	b.jitter = func() float64 { return 0 } // lower bound of each window

	want := []time.Duration{
		250 * time.Millisecond,  // 500ms window
		500 * time.Millisecond,  // 1s
		1000 * time.Millisecond, // 2s
		2000 * time.Millisecond, // 4s
		2000 * time.Millisecond, // capped
		2000 * time.Millisecond,
	}
	for i, w := range want {
		if got := b.next(); got != w {
			t.Fatalf("attempt %d: %v, want %v", i, got, w)
		}
	}

	b.reset()
	if got := b.next(); got != 250*time.Millisecond {
		t.Fatalf("after reset: %v, want 250ms", got)
	}
}

// Jitter must spread attempts out without ever collapsing the delay to zero:
// a zero delay is a hot reconnect loop against a struggling exchange.
func TestBackoffJitterStaysInTheUpperHalf(t *testing.T) {
	b := newBackoff(time.Second, time.Minute)
	for _, j := range []float64{0, 0.25, 0.5, 0.999} {
		b.reset()
		b.jitter = func() float64 { return j }
		d := b.next()
		if d < 500*time.Millisecond || d >= time.Second {
			t.Fatalf("jitter %v produced %v, want [500ms, 1s)", j, d)
		}
	}
}

// fakeExchange speaks enough of the Coinbase protocol to exercise reconnection:
// it accepts a subscribe, sends a scripted burst, then drops the connection.
type fakeExchange struct {
	mu          sync.Mutex
	connections int
	subscribes  []string

	// framesPerConn is how many data frames each connection sends before the
	// server hangs up.
	framesPerConn int
}

func (f *fakeExchange) handler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ctx := r.Context()

	_, sub, err := conn.Read(ctx)
	if err != nil {
		conn.CloseNow()
		return
	}

	f.mu.Lock()
	f.connections++
	n := f.connections
	f.subscribes = append(f.subscribes, string(sub))
	f.mu.Unlock()

	for i := 0; i < f.framesPerConn; i++ {
		msg, _ := json.Marshal(map[string]any{
			"type":       "match",
			"product_id": "BTC-USD",
			"sequence":   n*1000 + i,
			"side":       "buy",
			"price":      "80000.00",
			"size":       "0.001",
			"time":       time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		})
		if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
			break
		}
	}
	// Hang up mid-stream, the way a real feed does.
	conn.CloseNow()
}

func (f *fakeExchange) stats() (int, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections, append([]string(nil), f.subscribes...)
}

// A severed connection must be re-established, re-subscribed, and marked in the
// stream with a reseed frame that says why.
func TestCoinbaseReconnectsAndResubscribes(t *testing.T) {
	fake := &fakeExchange{framesPerConn: 3}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	c := &Coinbase{
		URL:       "ws" + strings.TrimPrefix(srv.URL, "http"),
		ProductID: "BTC-USD",
		Channels:  []string{ChannelLevel2Batch, ChannelMatches},
		Log:       slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	out := make(chan Frame, 256)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, ChanSink(out)) }()

	var reseeds []Frame
	var data int
	deadline := time.After(8 * time.Second)
	// Wait for three subscriptions: the first plus two reconnects.
collect:
	for {
		select {
		case fr := <-out:
			if fr.Kind == KindReseed {
				reseeds = append(reseeds, fr)
				if len(reseeds) == 3 {
					break collect
				}
			} else {
				data++
			}
		case <-deadline:
			t.Fatalf("only saw %d reseeds and %d data frames before the deadline", len(reseeds), data)
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if reseeds[0].Reason != "subscribed" {
		t.Errorf("first reseed reason = %q, want %q", reseeds[0].Reason, "subscribed")
	}
	for i, fr := range reseeds[1:] {
		if !strings.HasPrefix(fr.Reason, "reconnect: ") {
			t.Errorf("reseed %d reason = %q, want a reconnect reason", i+1, fr.Reason)
		}
		if fr.Recv.IsZero() {
			t.Errorf("reseed %d has no timestamp", i+1)
		}
	}

	conns, subs := fake.stats()
	if conns < 3 {
		t.Fatalf("server saw %d connections, want at least 3", conns)
	}
	// Every reconnection must resubscribe; a reconnect that forgets to is a
	// socket that sits there silently receiving nothing.
	for i, s := range subs {
		if !strings.Contains(s, `"type":"subscribe"`) ||
			!strings.Contains(s, "level2_batch") ||
			!strings.Contains(s, "BTC-USD") {
			t.Fatalf("connection %d subscribe = %s", i, s)
		}
	}
	if data == 0 {
		t.Fatal("no data frames survived the reconnections")
	}
}

// Reconnection must not outlive the context.
func TestCoinbaseStopsReconnectingOnCancel(t *testing.T) {
	fake := &fakeExchange{framesPerConn: 1}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	c := &Coinbase{
		URL:       "ws" + strings.TrimPrefix(srv.URL, "http"),
		ProductID: "BTC-USD",
		Channels:  []string{ChannelMatches},
		Log:       slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Frame, 256)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, ChanSink(out)) }()

	<-out // first reseed
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a cancelled feed should return nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept reconnecting after cancel")
	}
}

// A dial failure must be retried, not fatal.
func TestCoinbaseRetriesDialFailures(t *testing.T) {
	c := &Coinbase{
		URL:       "ws://127.0.0.1:1", // nothing listens here
		ProductID: "BTC-USD",
		Channels:  []string{ChannelMatches},
		Log:       slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	out := make(chan Frame, 8)
	err := c.Run(ctx, ChanSink(out))
	if err != nil {
		t.Fatalf("Run should return nil when the context ends, got %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("a feed that never connected must emit no frames, got %d", len(out))
	}
}
