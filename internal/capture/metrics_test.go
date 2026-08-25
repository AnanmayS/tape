package capture

import (
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/metrics"
)

// The metrics a session publishes must be the same numbers its summary
// reports. They are counted in two different places — the summary on the
// Summary struct, the metric on the Collector — and a graph that disagrees with
// the log line is worse than no graph, because someone will believe it.
func TestPublishedCountsMatchTheSummary(t *testing.T) {
	col := metrics.NewCollector()

	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1000,
		Count:     40,
		SkipAfter: map[int]uint64{5: 3, 20: 60},
		Now:       stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, _ := runCapture(t, s, Config{
		Window:        time.Hour,
		FlushInterval: time.Hour,
		Metrics:       col,
	})

	snap := col.Collect()
	if snap.Messages != sum.Messages {
		t.Errorf("metric messages = %d, summary messages = %d", snap.Messages, sum.Messages)
	}
	if snap.Gaps != sum.Gaps {
		t.Errorf("metric gaps = %d, summary gaps = %d", snap.Gaps, sum.Gaps)
	}
	if sum.Gaps != 2 {
		t.Fatalf("summary gaps = %d, want 2", sum.Gaps)
	}
	if !snap.QueueObserved || snap.MaxQueueDepth != sum.MaxQueueDepth {
		t.Errorf("metric queue depth = %d (observed %v), summary = %d",
			snap.MaxQueueDepth, snap.QueueObserved, sum.MaxQueueDepth)
	}

	// Every synthetic frame carries an exchange timestamp, so every message
	// contributes a lag sample.
	if snap.Lag.Count != sum.Messages {
		t.Errorf("lag samples = %d, messages = %d", snap.Lag.Count, sum.Messages)
	}
}

// A capture told nothing about metrics must record nothing and must not reach
// for a Recorder that is not there. This is the local-capture path, and it is
// the one that must never need AWS.
func TestMetricsDefaultToNop(t *testing.T) {
	var cfg Config
	cfg.withDefaults()
	if _, ok := cfg.Metrics.(metrics.Nop); !ok {
		t.Fatalf("default Metrics = %T, want metrics.Nop", cfg.Metrics)
	}

	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     25,
		SkipAfter: map[int]uint64{10: 5},
		Now:       stepClock(time.Date(2026, 8, 25, 5, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Hour, FlushInterval: time.Hour})

	if sum.Messages != 25 || sum.Gaps != 1 {
		t.Fatalf("summary = %d messages, %d gaps; want 25 and 1", sum.Messages, sum.Gaps)
	}
	if got.messages != 25 {
		t.Fatalf("messages on disk = %d, want 25", got.messages)
	}
}
