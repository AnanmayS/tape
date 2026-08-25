package feed

import "context"

// Sink is where a feed puts the frames it reads.
//
// It is an interface rather than a bare channel because what happens when the
// writer falls behind is a policy decision, and the send is the only place that
// decision can be made. A feed reads the socket and hands frames over; it has
// no opinion about whether a full queue should block the read, discard the
// frame, or grow. Package capture supplies the sink that carries that opinion,
// and M7's measurement is a measurement of these three implementations against
// each other.
type Sink interface {
	// Send hands one frame over, and reports whether the feed should keep
	// reading.
	//
	// False means the context ended, and nothing else. A policy that discards a
	// frame still returns true: the frame is accounted for — as a gap record in
	// the stream, never as silence — and the socket must keep being drained.
	Send(ctx context.Context, f Frame) bool
}

// ChanSink adapts a channel to a Sink with a blocking send. It is the plain
// handoff with no policy of its own, which makes it what the feed package's own
// tests read frames through.
type ChanSink chan Frame

// Send blocks until the frame is queued or ctx is done.
func (c ChanSink) Send(ctx context.Context, f Frame) bool {
	select {
	case c <- f:
		return true
	case <-ctx.Done():
		return false
	}
}

var _ Sink = ChanSink(nil)
