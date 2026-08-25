// Package feed defines the source of market data frames and the clients that
// produce them.
//
// A feed is a reader: it pushes frames into a Sink and returns when the context
// is cancelled or it gives up. It does no decoding and no writing. Keeping the
// socket read and the disk write in separate goroutines with a queue between
// them is deliberate — that queue is where the backpressure policy lives, and
// the Sink interface is what keeps the policy out of the feed. See sink.go.
package feed

import (
	"context"
	"time"
)

// Kind distinguishes market data from the feed's own structural events.
type Kind uint8

const (
	// KindData is a frame received from the exchange.
	KindData Kind = iota

	// KindReseed says a fresh subscription just landed. Everything after it
	// is a rebuilt book, not a continuation of what came before.
	KindReseed
)

// Frame is one item handed from the reader to the writer.
type Frame struct {
	Kind Kind

	// Raw is the exchange frame, verbatim. Empty for KindReseed.
	Raw []byte

	// Recv is when the frame was read off the socket, or when the reseed
	// happened.
	Recv time.Time

	// Reason explains a KindReseed: what caused the resubscribe.
	Reason string
}

// SeqMode describes the guarantee a feed's sequence numbers carry. Choosing
// this correctly is the difference between detecting real gaps and drowning in
// false ones.
type SeqMode int

const (
	// SeqContiguous: every sequence number appears, so seq must equal last+1
	// and any skip is a lost message.
	SeqContiguous SeqMode = iota

	// SeqMonotonic: sequence numbers increase but skip, because the stream is
	// a subset of a larger sequenced stream. A skip proves nothing, so
	// continuity can only be trusted across an unbroken connection — which is
	// why a reconnect on such a feed must always be treated as a possible gap.
	SeqMonotonic
)

func (m SeqMode) String() string {
	if m == SeqContiguous {
		return "contiguous"
	}
	return "monotonic"
}

// Feed is a source of frames.
//
// Run pushes frames to out until ctx is cancelled, at which point it returns
// ctx.Err() or nil. It must not close out; the caller owns the sink. A Feed
// that handles its own reconnection returns only when it gives up entirely.
type Feed interface {
	// Name identifies the feed in logs.
	Name() string

	// Product is the exchange product id this feed carries.
	Product() string

	// SeqMode describes what the feed's sequence numbers guarantee.
	SeqMode() SeqMode

	// Run reads until ctx is done.
	Run(ctx context.Context, out Sink) error
}
