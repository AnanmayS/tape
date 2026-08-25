package capture

import (
	"context"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/tapefile"
)

func dataFrame(i int) feed.Frame {
	return feed.Frame{
		Kind: feed.KindData,
		Recv: time.Unix(0, int64(i)).UTC(),
		Raw:  []byte(`{"type":"heartbeat"}`),
	}
}

// The block policy waits. Nothing is discarded, and a send that cannot complete
// reports the context ending rather than quietly giving up on the frame.
func TestBlockPolicyWaitsAndDiscardsNothing(t *testing.T) {
	q := newQueue(PolicyBlock, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !q.Send(ctx, dataFrame(0)) {
		t.Fatal("first send should have taken the free slot")
	}

	blocked := make(chan bool, 1)
	go func() { blocked <- q.Send(ctx, dataFrame(1)) }()
	select {
	case <-blocked:
		t.Fatal("send into a full queue returned instead of waiting")
	case <-time.After(50 * time.Millisecond):
	}

	<-q.ch
	if ok := <-blocked; !ok {
		t.Fatal("send should have completed once there was room")
	}
	if q.dropCount() != 0 {
		t.Fatalf("block dropped %d frames", q.dropCount())
	}

	cancel()
	q.Send(ctx, dataFrame(2)) // fills the one free slot
	if q.Send(ctx, dataFrame(3)) {
		t.Fatal("a send that cannot complete under a dead context must report it")
	}
}

// The drop policy sheds data frames and counts every one, and the count is
// handed over exactly once: a drop reported twice would be a gap record for
// frames that were never lost, and one reported never would be invariant 2.
func TestDropPolicyCountsEveryDiscardExactlyOnce(t *testing.T) {
	q := newQueue(PolicyDrop, 2)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if !q.Send(ctx, dataFrame(i)) {
			t.Fatalf("send %d reported the context ended", i)
		}
	}
	if got, want := len(q.ch), 2; got != want {
		t.Fatalf("queue holds %d frames, want %d", got, want)
	}
	if got, want := q.dropCount(), int64(8); got != want {
		t.Fatalf("dropped %d, want %d", got, want)
	}
	if got, want := q.takePending(), int64(8); got != want {
		t.Fatalf("takePending = %d, want %d", got, want)
	}
	if got := q.takePending(); got != 0 {
		t.Fatalf("takePending returned %d a second time; drops must be handed over once", got)
	}
	if got, want := q.dropCount(), int64(8); got != want {
		t.Fatalf("session total became %d after taking pending, want %d", got, want)
	}
}

// A reseed is never a candidate for dropping. Losing one would leave a rebuilt
// book looking like a continuous one, which is worse than the frames the policy
// is shedding — so the drop policy blocks on it like any other.
func TestDropPolicyNeverDropsAReseed(t *testing.T) {
	q := newQueue(PolicyDrop, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.Send(ctx, dataFrame(0)) // the queue is now full
	reseed := feed.Frame{Kind: feed.KindReseed, Recv: time.Unix(0, 1).UTC(), Reason: "subscribed"}

	sent := make(chan bool, 1)
	go func() { sent <- q.Send(ctx, reseed) }()
	select {
	case <-sent:
		t.Fatal("the reseed was accepted instantly, so it was dropped")
	case <-time.After(50 * time.Millisecond):
	}

	<-q.ch
	if !<-sent {
		t.Fatal("the reseed should have gone in once there was room")
	}
	if got := <-q.ch; got.Kind != feed.KindReseed {
		t.Fatalf("received %v, want the reseed", got.Kind)
	}
	if q.dropCount() != 0 {
		t.Fatalf("drop policy dropped %d structural frames", q.dropCount())
	}
}

// The buffer policy discards nothing and reorders nothing: frames past the
// channel's capacity queue up in front of it and come out in arrival order.
func TestBufferPolicyGrowsAndKeepsOrder(t *testing.T) {
	q := newQueue(PolicyBuffer, 2)
	ctx := context.Background()

	const n = 50
	for i := 0; i < n; i++ {
		if !q.Send(ctx, dataFrame(i)) {
			t.Fatalf("send %d reported the context ended", i)
		}
	}
	if q.dropCount() != 0 {
		t.Fatalf("buffer dropped %d frames", q.dropCount())
	}
	if got := q.depth(); got != n {
		t.Fatalf("depth = %d, want every one of the %d frames", got, n)
	}

	// Draining is the reader's job, so it takes another send to move the
	// overflow along — exactly as it does in a capture, where frames keep
	// arriving. The order that comes out is the order that went in.
	var got []int64
	for len(got) < n {
		select {
		case fr := <-q.ch:
			got = append(got, fr.Recv.UnixNano())
		default:
			q.drainOverflow()
		}
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("frame %d came out as %d; the buffer reordered the stream", i, v)
		}
	}
	if q.depth() != 0 {
		t.Fatalf("depth = %d after draining everything", q.depth())
	}
}

// close hands the overflow over rather than discarding it, so a clean shutdown
// under the buffer policy writes every frame it accepted.
func TestBufferPolicyFlushesOnClose(t *testing.T) {
	q := newQueue(PolicyBuffer, 2)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		q.Send(ctx, dataFrame(i))
	}

	stop := make(chan struct{})
	go q.close(stop)

	var n int
	for range q.ch {
		n++
	}
	if n != 20 {
		t.Fatalf("close handed over %d frames, want 20", n)
	}
	if q.dropCount() != 0 {
		t.Fatalf("a clean close dropped %d frames", q.dropCount())
	}
}

// If the writer has stopped, the frames still buffered are lost — and a lost
// frame is a dropped frame, counted, never silently forgotten.
func TestBufferPolicyCountsWhatItCannotHandOver(t *testing.T) {
	q := newQueue(PolicyBuffer, 2)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		q.Send(ctx, dataFrame(i))
	}

	stop := make(chan struct{})
	close(stop)
	q.close(stop)

	held := 0
	for range q.ch {
		held++
	}
	if int64(held)+q.dropCount() != 20 {
		t.Fatalf("%d handed over plus %d dropped is not the 20 accepted", held, q.dropCount())
	}
	if q.dropCount() == 0 {
		t.Fatal("frames the writer never received were not counted as dropped")
	}
	if q.takePending() != q.dropCount() {
		t.Fatal("abandoned frames were not left pending for a gap record")
	}
}

// A drop becomes a gap record in the stream. This is the whole cost of the
// policy and the only thing that makes it legal under invariant 2.
func TestDropsAreWrittenAsGapRecords(t *testing.T) {
	root := t.TempDir()
	w, err := tapefile.NewWriter(root, "BTC-USD", time.Minute)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	sum := Summary{}
	s := &sink{w: w, seq: newSeqTracker(feed.SeqMonotonic), log: quietLogger(), sum: &sum, met: nopRecorder{}}

	at := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	if err := s.recordDrop(913, at); err != nil {
		t.Fatalf("recordDrop: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sum.Gaps != 1 {
		t.Fatalf("a drop counted %d gaps, want 1", sum.Gaps)
	}

	c := readAll(t, w.Stats().Files)
	if len(c.gapRecords) != 1 {
		t.Fatalf("%d gap records on disk, want 1", len(c.gapRecords))
	}
	g := c.gapRecords[0]
	if g.Dropped != 913 {
		t.Fatalf("gap record says %d dropped, want 913", g.Dropped)
	}
	if !g.At.Equal(at) {
		t.Fatalf("gap record is stamped %s, want %s", g.At, at)
	}
	if s.hist.count != 1 {
		t.Fatalf("the gap write was not timed: %d observations", s.hist.count)
	}
}

type nopRecorder struct{}

func (nopRecorder) Message()          {}
func (nopRecorder) Lag(time.Duration) {}
func (nopRecorder) Gap()              {}
func (nopRecorder) QueueDepth(int)    {}

func TestUnknownPolicyIsRefused(t *testing.T) {
	_, err := Run(context.Background(), &feed.Synthetic{ProductID: "BTC-USD", Count: 1},
		Config{Root: t.TempDir(), Log: quietLogger(), Policy: Policy("shed-a-bit")})
	if err == nil {
		t.Fatal("an unknown policy must be refused, not defaulted")
	}
}

// Every frame a session accepted is either on disk or counted as dropped, under
// every policy. That is the accounting invariant behind all of this: a frame
// that is neither written nor reported is the silent loss invariant 2 forbids.
func TestEveryFrameIsWrittenOrCounted(t *testing.T) {
	for _, pol := range Policies {
		t.Run(string(pol), func(t *testing.T) {
			s := &feed.Synthetic{
				ProductID: "BTC-USD",
				Mode:      feed.SeqMonotonic,
				StartSeq:  1,
				Count:     20000,
				Step:      3,
			}
			sum, c := runCapture(t, s, Config{
				Window: time.Hour,
				Buffer: 1,
				Policy: pol,
				Log:    quietLogger(),
			})

			if sum.Policy != pol {
				t.Fatalf("summary reports policy %q, want %q", sum.Policy, pol)
			}
			if int64(c.messages)+sum.Dropped != int64(s.Count) {
				t.Fatalf("%d messages on disk plus %d dropped is not the %d sent",
					c.messages, sum.Dropped, s.Count)
			}
			if int64(c.messages) != sum.Messages {
				t.Fatalf("summary counted %d messages, disk holds %d", sum.Messages, c.messages)
			}

			// Every dropped frame is inside a gap record, and the counts agree.
			var recorded uint64
			for _, g := range c.gapRecords {
				recorded += g.Dropped
			}
			if int64(recorded) != sum.Dropped {
				t.Fatalf("gap records account for %d dropped frames, session dropped %d",
					recorded, sum.Dropped)
			}
			if pol != PolicyDrop && sum.Dropped != 0 {
				t.Fatalf("%s dropped %d frames; only %s may", pol, sum.Dropped, PolicyDrop)
			}
			if sum.WriteLatency.Count == 0 {
				t.Fatal("no write was timed")
			}
		})
	}
}
