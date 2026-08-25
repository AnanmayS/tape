package feed

import (
	"context"
	"fmt"
	"time"
)

// Synthetic is a scripted feed used to exercise the capture path without a
// network. It emits real Coinbase-shaped match frames so the production decoder
// and gap detector are the ones under test, not a stand-in.
//
// It is not a test file so that other packages' tests can use it.
type Synthetic struct {
	// ProductID is stamped into every frame.
	ProductID string

	// Mode is the sequence guarantee the feed claims to offer.
	Mode SeqMode

	// StartSeq is the sequence number of the first frame.
	StartSeq uint64

	// Count is how many data frames to emit.
	Count int

	// Step is the normal sequence increment between consecutive frames. One
	// for a contiguous feed; larger to imitate the matches channel, where the
	// sequence belongs to a busier stream.
	Step uint64

	// SkipAfter[i] additionally advances the sequence by n after frame i,
	// imitating messages that were lost rather than merely not delivered on
	// this channel.
	SkipAfter map[int]uint64

	// SeverAfter[i] drops the connection after frame i and resubscribes,
	// emitting a reseed frame.
	SeverAfter map[int]bool

	// Delay paces emission. Zero runs flat out.
	Delay time.Duration

	// Now supplies frame timestamps. Tests pass a deterministic clock; nil
	// means time.Now in UTC.
	Now func() time.Time
}

func (s *Synthetic) Name() string    { return "synthetic" }
func (s *Synthetic) Product() string { return s.ProductID }
func (s *Synthetic) SeqMode() SeqMode {
	return s.Mode
}

func (s *Synthetic) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

// Run emits the scripted frames, then returns nil. It returns early with
// ctx.Err() if the context is cancelled.
func (s *Synthetic) Run(ctx context.Context, out chan<- Frame) error {
	step := s.Step
	if step == 0 {
		step = 1
	}

	if !send(ctx, out, Frame{Kind: KindReseed, Recv: s.now(), Reason: "subscribed"}) {
		return ctx.Err()
	}

	seq := s.StartSeq
	for i := 0; i < s.Count; i++ {
		if s.Delay > 0 {
			select {
			case <-time.After(s.Delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		recv := s.now()
		if !send(ctx, out, Frame{Kind: KindData, Raw: matchJSON(s.ProductID, seq, recv, i), Recv: recv}) {
			return ctx.Err()
		}

		seq += step
		if n, ok := s.SkipAfter[i]; ok {
			seq += n
		}
		if s.SeverAfter[i] {
			if !send(ctx, out, Frame{
				Kind:   KindReseed,
				Recv:   s.now(),
				Reason: "reconnect: connection reset by peer",
			}) {
				return ctx.Err()
			}
		}
	}
	return nil
}

// matchJSON builds a frame in the shape Coinbase actually sends.
func matchJSON(product string, seq uint64, ts time.Time, tradeID int) []byte {
	return fmt.Appendf(nil,
		`{"type":"match","trade_id":%d,"maker_order_id":"00000000-0000-0000-0000-%012d",`+
			`"taker_order_id":"00000000-0000-0000-0000-%012d","side":"%s","size":"0.00000001",`+
			`"price":"%d.53","product_id":%q,"sequence":%d,"time":%q}`,
		tradeID, tradeID, tradeID+1, sideFor(tradeID), 80000+tradeID, product, seq,
		ts.UTC().Format("2006-01-02T15:04:05.000000Z"))
}

func sideFor(i int) string {
	if i%2 == 0 {
		return "buy"
	}
	return "sell"
}
