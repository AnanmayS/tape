package capture

import (
	"context"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
)

// A session with a Progress channel must sample itself, and every number in the
// sample must be the same number the summary ends up reporting.
func TestProgressSamplesTheRunningSession(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     400,
		Delay:     time.Millisecond,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}

	ch := make(chan Progress, 1)
	var got []Progress
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range ch {
			got = append(got, p)
		}
	}()

	sum, err := Run(context.Background(), s, Config{
		Root:             t.TempDir(),
		Window:           time.Hour,
		FlushInterval:    time.Hour,
		Log:              quietLogger(),
		Progress:         ch,
		ProgressInterval: 5 * time.Millisecond,
	})
	close(ch)
	<-done
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	if len(got) == 0 {
		t.Fatal("a session with a Progress channel sampled itself no times")
	}
	last := got[len(got)-1]
	if last.Messages <= 0 {
		t.Fatalf("last sample counted %d messages", last.Messages)
	}
	if last.Messages > sum.Messages {
		t.Fatalf("a sample counted %d messages, more than the %d the session received",
			last.Messages, sum.Messages)
	}
	if last.QueueCapacity != 4096 {
		t.Fatalf("queue capacity in the sample is %d, want the configured 4096", last.QueueCapacity)
	}
	if last.QueueDepth > last.QueueCapacity {
		t.Fatalf("queue depth %d exceeds its own ceiling %d", last.QueueDepth, last.QueueCapacity)
	}
	if !last.Started.Equal(sum.Started) {
		t.Fatalf("sample says the session started at %s, summary says %s", last.Started, sum.Started)
	}

	// Counters only ever go up, and the sample's file is one of the files the
	// session reports writing.
	for i := 1; i < len(got); i++ {
		if got[i].Messages < got[i-1].Messages || got[i].Records < got[i-1].Records {
			t.Fatalf("sample %d went backwards: %+v then %+v", i, got[i-1], got[i])
		}
	}
	if last.File == "" {
		t.Fatal("no file named in the last sample of a session that wrote one")
	}
}

// The channel is never blocked on. A consumer that never reads must cost the
// session nothing but the samples it did not collect.
func TestProgressNeverBlocksTheWriter(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     300,
		Delay:     time.Millisecond,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}

	// Unbuffered, and nobody receiving: every send must be abandoned.
	ch := make(chan Progress)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sum, err := Run(ctx, s, Config{
		Root:             t.TempDir(),
		Window:           time.Hour,
		FlushInterval:    time.Hour,
		Log:              quietLogger(),
		Progress:         ch,
		ProgressInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if sum.Messages != 300 {
		t.Fatalf("messages = %d, want 300: a dropped sample cost the session frames", sum.Messages)
	}
}

// Without a Progress channel nothing is sampled and nothing changes.
func TestNoProgressChannelSamplesNothing(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     50,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})
	if sum.Messages != 50 || got.messages != 50 {
		t.Fatalf("summary %d, disk %d, want 50 each", sum.Messages, got.messages)
	}
}

// A finished session renders through the same code as a running one, so the
// summary has to be able to present itself as a final sample.
func TestSummaryAsFinalProgress(t *testing.T) {
	sum := Summary{
		Started:       time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC),
		Ended:         time.Date(2026, 8, 25, 4, 1, 40, 0, time.UTC),
		Messages:      5000,
		Records:       5002,
		Bytes:         123456,
		Gaps:          2,
		Reseeds:       1,
		MaxQueueDepth: 37,
	}
	p := sum.Progress()

	if p.Elapsed() != 100*time.Second {
		t.Fatalf("elapsed %s, want 1m40s", p.Elapsed())
	}
	if p.MessagesPerSecond() != 50 {
		t.Fatalf("rate %v, want 50", p.MessagesPerSecond())
	}
	if p.Messages != sum.Messages || p.Gaps != sum.Gaps || p.MaxQueueDepth != sum.MaxQueueDepth {
		t.Fatalf("the final sample disagrees with the summary it came from: %+v", p)
	}

	var zero Progress
	if zero.Elapsed() != 0 || zero.MessagesPerSecond() != 0 {
		t.Fatal("the zero sample reports a rate over no time at all")
	}
}
