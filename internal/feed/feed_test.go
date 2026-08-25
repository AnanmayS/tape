package feed

import (
	"context"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/event"
)

// drain collects every frame a feed emits.
func drain(t *testing.T, f Feed) []Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make(chan Frame, 256)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, ChanSink(out)); close(out) }()

	var got []Frame
	for fr := range out {
		got = append(got, fr)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	return got
}

func fixedClock(start time.Time, step time.Duration) func() time.Time {
	t := start.Add(-step)
	return func() time.Time {
		t = t.Add(step)
		return t
	}
}

func TestSyntheticEmitsDecodableCoinbaseFrames(t *testing.T) {
	s := &Synthetic{
		ProductID: "BTC-USD",
		Mode:      SeqContiguous,
		StartSeq:  100,
		Count:     5,
		Now:       fixedClock(time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC), time.Second),
	}
	frames := drain(t, s)

	if frames[0].Kind != KindReseed || frames[0].Reason != "subscribed" {
		t.Fatalf("first frame = %+v, want a subscribed reseed", frames[0])
	}
	data := frames[1:]
	if len(data) != 5 {
		t.Fatalf("got %d data frames, want 5", len(data))
	}
	for i, fr := range data {
		e, err := event.Decode(fr.Raw, fr.Recv)
		if err != nil {
			t.Fatalf("frame %d does not decode: %v", i, err)
		}
		if e.Type != "match" || e.Product != "BTC-USD" {
			t.Fatalf("frame %d = %+v", i, e)
		}
		if want := uint64(100 + i); e.Sequence != want {
			t.Fatalf("frame %d sequence = %d, want %d", i, e.Sequence, want)
		}
		if e.Price == 0 || e.Size == 0 {
			t.Fatalf("frame %d has no price/size: %+v", i, e)
		}
	}
}

func TestSyntheticSkipsSequences(t *testing.T) {
	s := &Synthetic{
		ProductID: "BTC-USD",
		Mode:      SeqContiguous,
		StartSeq:  1,
		Count:     4,
		SkipAfter: map[int]uint64{1: 10},
	}
	frames := drain(t, s)
	var seqs []uint64
	for _, fr := range frames {
		if fr.Kind != KindData {
			continue
		}
		e, err := event.Decode(fr.Raw, fr.Recv)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, e.Sequence)
	}
	want := []uint64{1, 2, 13, 14}
	if len(seqs) != len(want) {
		t.Fatalf("got %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("got %v, want %v", seqs, want)
		}
	}
}

func TestSyntheticSeversConnection(t *testing.T) {
	s := &Synthetic{
		ProductID:  "BTC-USD",
		Mode:       SeqContiguous,
		StartSeq:   1,
		Count:      4,
		SeverAfter: map[int]bool{1: true},
		SkipAfter:  map[int]uint64{1: 50},
	}
	frames := drain(t, s)

	var reseeds int
	for _, fr := range frames {
		if fr.Kind == KindReseed {
			reseeds++
		}
	}
	if reseeds != 2 {
		t.Fatalf("reseeds = %d, want 2 (initial subscribe plus one reconnect)", reseeds)
	}
	if frames[3].Kind != KindReseed || frames[3].Reason == "subscribed" {
		t.Fatalf("frame 3 = %+v, want the reconnect reseed", frames[3])
	}
}

func TestSyntheticStopsOnContextCancel(t *testing.T) {
	s := &Synthetic{ProductID: "BTC-USD", Count: 1000, Delay: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Frame)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, ChanSink(out)) }()

	<-out // the initial reseed
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx.Err() after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestCoinbaseFeedShape(t *testing.T) {
	c := NewCoinbase(nil)
	if c.Product() != "BTC-USD" {
		t.Errorf("Product = %q", c.Product())
	}
	if c.SeqMode() != SeqMonotonic {
		t.Errorf("SeqMode = %v, want monotonic", c.SeqMode())
	}
	// level2 requires authentication; this project uses no credentials.
	want := []string{"level2_batch", "matches"}
	if len(c.Channels) != len(want) {
		t.Fatalf("channels = %v, want %v", c.Channels, want)
	}
	for i := range want {
		if c.Channels[i] != want[i] {
			t.Fatalf("channels = %v, want %v", c.Channels, want)
		}
	}
	var _ Feed = c
	var _ Feed = &Synthetic{}
}
