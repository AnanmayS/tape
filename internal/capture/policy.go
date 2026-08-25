package capture

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/AnanmayS/tape/internal/feed"
)

// Policy is what the reader does when the queue to the writer is full.
//
// The three options are the only three there are, and each is wrong in its own
// way. What decides between them is not which failure is nicest but which
// failure the rest of this project can still tell the truth about. See
// CLAUDE.md for the measurement and the decision it produced; what follows is
// what each one costs.
type Policy string

const (
	// PolicyBlock waits for room. Nothing is discarded here, so the loss — if
	// the writer never catches up — happens further upstream, in the kernel's
	// socket buffer and then at the exchange, which disconnects a consumer it
	// cannot deliver to. That path already ends in a reconnect, a reseed record
	// and a gap record, which is to say the one policy whose worst case is
	// already covered by invariant 2 without new machinery.
	//
	// It is the default.
	PolicyBlock Policy = "block"

	// PolicyDrop discards the frame and records it. Every dropped frame becomes
	// a gap record carrying the count, because a drop that is not in the stream
	// is a window that is missing messages and reads as complete — the exact
	// failure invariant 2 exists to prevent. Reseed frames are never dropped:
	// they are structural, they are rare, and losing one silently reclassifies
	// a rebuilt book as a continuous one.
	PolicyDrop Policy = "drop"

	// PolicyBuffer never blocks and never discards: frames past the channel's
	// capacity go into an unbounded queue in front of it. That converts a
	// throughput problem into a memory problem, which is only an improvement if
	// the overload is a burst. A sustained one ends at the OOM killer, and a
	// process killed with SIGKILL loses whatever the writer had not flushed —
	// so the policy that discards nothing is the one whose failure discards the
	// most, and without a record of it.
	PolicyBuffer Policy = "buffer"
)

// Policies is every policy, in the order the measurement reports them.
var Policies = []Policy{PolicyBlock, PolicyDrop, PolicyBuffer}

func (p Policy) valid() bool {
	switch p {
	case PolicyBlock, PolicyDrop, PolicyBuffer:
		return true
	default:
		return false
	}
}

// queue is the reader-to-writer handoff, with the policy attached to its send.
//
// It is a feed.Sink, so the feed pushes into it exactly as it pushed into a
// channel, and the writer receives from Recv exactly as it received from one.
// Everything that differs between the three policies is inside Send.
type queue struct {
	ch  chan feed.Frame
	pol Policy

	// overflow is the buffer policy's unbounded tail, oldest first from head.
	// Both fields belong to the reader goroutine alone; the writer learns its
	// length from depth, which is why that length is an atomic and the slice is
	// not shared.
	overflow []feed.Frame
	head     int

	buffered atomic.Int64 // len(overflow) - head
	pending  atomic.Int64 // dropped frames not yet written as a gap record
	dropped  atomic.Int64 // dropped frames over the session
}

var _ feed.Sink = (*queue)(nil)

func newQueue(pol Policy, depth int) *queue {
	return &queue{ch: make(chan feed.Frame, depth), pol: pol}
}

// Send hands a frame to the writer under this queue's policy. It returns false
// only when the context ended: a frame the policy discarded is accounted for,
// and the socket has to keep being drained either way.
func (q *queue) Send(ctx context.Context, fr feed.Frame) bool {
	switch q.pol {
	case PolicyDrop:
		// Structural frames are never candidates. A reseed says the book was
		// rebuilt; dropping one would leave a rebuilt book looking continuous,
		// which is a worse lie than the frames the policy is shedding.
		if fr.Kind != feed.KindData {
			return q.blockingSend(ctx, fr)
		}
		select {
		case q.ch <- fr:
			return true
		default:
			q.pending.Add(1)
			q.dropped.Add(1)
			return true
		}

	case PolicyBuffer:
		// Drain first, so the overflow is a tail on the channel and not a
		// parallel path: order is arrival order or the whole capture is wrong.
		q.drainOverflow()
		if q.buffered.Load() == 0 {
			select {
			case q.ch <- fr:
				return true
			default:
			}
		}
		q.push(fr)
		return true

	default:
		return q.blockingSend(ctx, fr)
	}
}

func (q *queue) blockingSend(ctx context.Context, fr feed.Frame) bool {
	select {
	case q.ch <- fr:
		return true
	case <-ctx.Done():
		return false
	}
}

func (q *queue) push(fr feed.Frame) {
	q.overflow = append(q.overflow, fr)
	q.buffered.Store(int64(len(q.overflow) - q.head))
}

// drainOverflow moves as much of the overflow into the channel as fits without
// waiting. The backing array is only released when the overflow empties
// completely, which is the honest shape of this policy: memory it took is
// memory it keeps until the burst is over.
func (q *queue) drainOverflow() {
	for q.head < len(q.overflow) {
		select {
		case q.ch <- q.overflow[q.head]:
			q.overflow[q.head] = feed.Frame{}
			q.head++
		default:
			q.buffered.Store(int64(len(q.overflow) - q.head))
			return
		}
	}
	q.overflow, q.head = q.overflow[:0], 0
	q.buffered.Store(0)
}

// close hands over whatever is still buffered and closes the channel, so a
// clean shutdown under the buffer policy writes every frame it accepted rather
// than discarding the tail it was holding.
//
// stop is closed when the writer has stopped receiving. Frames still held when
// that happens are counted as dropped: the session is failing, and a frame
// nobody wrote is a frame nobody wrote.
func (q *queue) close(stop <-chan struct{}) {
	for q.head < len(q.overflow) {
		select {
		case q.ch <- q.overflow[q.head]:
			q.overflow[q.head] = feed.Frame{}
			q.head++
		case <-stop:
			n := int64(len(q.overflow) - q.head)
			q.pending.Add(n)
			q.dropped.Add(n)
			q.overflow, q.head = nil, 0
			q.buffered.Store(0)
			close(q.ch)
			return
		}
	}
	q.overflow, q.head = nil, 0
	q.buffered.Store(0)
	close(q.ch)
}

// depth is how many frames are waiting: the channel plus anything the buffer
// policy is holding in front of it. Reporting only the channel would show a
// queue pinned at its capacity while the real one grew without limit.
func (q *queue) depth() int { return len(q.ch) + int(q.buffered.Load()) }

// takePending returns the drops not yet written into the stream and clears
// them, so that every drop is written exactly once.
func (q *queue) takePending() int64 { return q.pending.Swap(0) }

// dropCount is the session total.
func (q *queue) dropCount() int64 { return q.dropped.Load() }

func validatePolicy(p Policy) error {
	if !p.valid() {
		return fmt.Errorf("capture: unknown backpressure policy %q (want %s, %s or %s)",
			p, PolicyBlock, PolicyDrop, PolicyBuffer)
	}
	return nil
}
