package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/websocket"
)

// Coinbase Exchange public feed. Frozen for v1: one exchange, one product, two
// channels, no configuration knob for more.
const (
	CoinbaseURL     = "wss://ws-feed.exchange.coinbase.com"
	CoinbaseProduct = "BTC-USD"

	// ChannelLevel2Batch is the batched level2 book. The unbatched level2
	// channel now requires authentication ("level2, level3, and full channels
	// now require authentication"), and this project takes no paid or
	// credentialed data, so the batched channel is the one available.
	ChannelLevel2Batch = "level2_batch"

	// ChannelMatches carries trades. Its sequence numbers are the product's
	// full-channel sequence, so they increase but skip; see SeqMonotonic.
	ChannelMatches = "matches"
)

const (
	// readTimeout bounds a single socket read. level2_batch for BTC-USD emits
	// several times a second, so silence this long means the connection is
	// dead even though TCP has not noticed.
	readTimeout = 30 * time.Second

	// dialTimeout bounds connect plus subscribe.
	dialTimeout = 20 * time.Second

	// readLimit must clear the level2 snapshot, which is over 1 MiB for
	// BTC-USD and grows with book depth.
	readLimit = 32 << 20
)

// Coinbase reads the public Coinbase Exchange WebSocket feed.
type Coinbase struct {
	URL       string
	ProductID string
	Channels  []string
	Log       *slog.Logger
}

// NewCoinbase returns a client for the frozen v1 feed.
func NewCoinbase(log *slog.Logger) *Coinbase {
	if log == nil {
		log = slog.Default()
	}
	return &Coinbase{
		URL:       CoinbaseURL,
		ProductID: CoinbaseProduct,
		Channels:  []string{ChannelLevel2Batch, ChannelMatches},
		Log:       log,
	}
}

func (c *Coinbase) Name() string { return "coinbase" }

func (c *Coinbase) Product() string { return c.ProductID }

// SeqMode reports SeqMonotonic.
//
// The matches channel reports the product's full-channel sequence number, but
// delivers only the match subset of that channel, so consecutive matches differ
// by more than one as a matter of course. Asserting contiguity here would
// manufacture a gap on nearly every message. What remains detectable is
// regression (a sequence at or below one already seen) and any discontinuity
// across a reconnect, where continuity cannot be proven at all.
func (c *Coinbase) SeqMode() SeqMode { return SeqMonotonic }

// Run connects, subscribes, and reads frames until ctx is done, reconnecting
// with exponential backoff for as long as the context lives.
//
// It does not give up on its own. A capture session ends when it is told to
// end; a feed that quit after N failures would hand back a window that looks
// complete and is not. Every reconnection puts a reseed frame in the stream,
// so a replayer can see that the book was rebuilt there.
func (c *Coinbase) Run(ctx context.Context, out chan<- Frame) error {
	b := newBackoff(backoffBase, backoffMax)
	reason := "subscribed"

	for {
		started := time.Now()
		err := c.session(ctx, out, reason)
		if ctx.Err() != nil {
			return nil
		}
		lasted := time.Since(started)
		if lasted >= stableFor {
			b.reset()
		}

		delay := b.next()
		c.Log.Warn("feed disconnected, reconnecting",
			"feed", c.Name(),
			"err", err,
			"connected_for", lasted.Round(time.Millisecond).String(),
			"retry_in", delay.Round(time.Millisecond).String())

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil
		}
		reason = "reconnect: " + errText(err)
	}
}

func errText(err error) string {
	if err == nil {
		return "connection closed"
	}
	return err.Error()
}

// session runs exactly one connection: dial, subscribe, read until failure.
// reason is carried into the reseed frame the subscription emits.
func (c *Coinbase) session(ctx context.Context, out chan<- Frame, reason string) error {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("feed: dial %s: %w", c.URL, err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(readLimit)

	sub, err := json.Marshal(subscribeMsg{
		Type:       "subscribe",
		ProductIDs: []string{c.ProductID},
		Channels:   c.Channels,
	})
	if err != nil {
		return fmt.Errorf("feed: encode subscribe: %w", err)
	}
	if err := conn.Write(dialCtx, websocket.MessageText, sub); err != nil {
		return fmt.Errorf("feed: send subscribe: %w", err)
	}

	c.Log.Info("subscribed",
		"feed", c.Name(),
		"url", c.URL,
		"product", c.ProductID,
		"channels", c.Channels,
		"reason", reason)

	// The subscription is a fresh book from here. Say so in the stream, not
	// just in the log, so a replayer knows where the book was rebuilt.
	if !send(ctx, out, Frame{
		Kind:   KindReseed,
		Recv:   time.Now().UTC(),
		Reason: reason,
	}) {
		return ctx.Err()
	}

	for {
		readCtx, rcancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		rcancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("feed: read: %w", err)
		}
		if !send(ctx, out, Frame{Kind: KindData, Raw: raw, Recv: time.Now().UTC()}) {
			return ctx.Err()
		}
	}
}

type subscribeMsg struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// send blocks until the frame is queued or ctx is done. Blocking is the current
// backpressure policy: when the writer falls behind, the reader waits rather
// than dropping data. M7 revisits this with a measured number.
func send(ctx context.Context, out chan<- Frame, f Frame) bool {
	select {
	case out <- f:
		return true
	case <-ctx.Done():
		return false
	}
}
