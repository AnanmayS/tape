package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeSink records what it was asked to publish and can be told to fail.
type fakeSink struct {
	mu    sync.Mutex
	calls [][]Datum
	err   error
}

func (f *fakeSink) Publish(_ context.Context, data []Datum) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	cp := make([]Datum, len(data))
	copy(cp, data)
	f.calls = append(f.calls, cp)
	return nil
}

func (f *fakeSink) String() string { return "fake" }

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeSink) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// The last interval of a session is almost always a partial one, and it holds
// the messages captured since the final tick. Dropping it would mean a
// ninety-second session reporting sixty seconds of work.
func TestCloseFlushesThePartialInterval(t *testing.T) {
	sink := &fakeSink{}
	p := NewPublisher(sink, PublisherConfig{Interval: time.Hour, Log: quietLog()})

	for i := 0; i < 7; i++ {
		p.Message()
	}
	p.Gap()
	p.Lag(250 * time.Millisecond)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := sink.count(); got != 1 {
		t.Fatalf("published %d intervals, want 1", got)
	}

	byName := map[string]Datum{}
	for _, d := range sink.calls[0] {
		byName[d.Name] = d
	}
	if got := byName[MetricMessages].Value; got != 7 {
		t.Errorf("MessagesReceived = %v, want 7", got)
	}
	if got := byName[MetricGaps].Value; got != 1 {
		t.Errorf("GapsDetected = %v, want 1", got)
	}
	if st := byName[MetricIngestLag].Statistics; st == nil || st.Count != 1 {
		t.Errorf("IngestLag statistics = %+v, want one sample", st)
	}

	stats := p.Stats()
	if stats.Published != 1 || stats.Failed != 0 || stats.Datapoints != int64(len(sink.calls[0])) {
		t.Errorf("stats = %+v", stats)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	sink := &fakeSink{}
	p := NewPublisher(sink, PublisherConfig{Interval: time.Hour, Log: quietLog()})
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := sink.count(); got != 1 {
		t.Errorf("published %d intervals across two Closes, want 1", got)
	}
}

func TestPublisherTicks(t *testing.T) {
	sink := &fakeSink{}
	p := NewPublisher(sink, PublisherConfig{Interval: 5 * time.Millisecond, Log: quietLog()})

	deadline := time.Now().Add(2 * time.Second)
	for sink.count() < 3 && time.Now().Before(deadline) {
		p.Message()
		time.Sleep(time.Millisecond)
	}
	p.Close()

	if got := sink.count(); got < 3 {
		t.Errorf("published %d intervals in 2s at a 5ms interval, want at least 3", got)
	}
}

// A publish that fails costs that interval's numbers and nothing else. It must
// not stop the ticker, and it must not be retried: a retry would queue stale
// datapoints behind fresh ones, and the capture — the thing that cannot be
// re-run — is entirely unaffected either way.
func TestPublishFailureIsCountedAndSurvived(t *testing.T) {
	sink := &fakeSink{}
	sink.fail(errors.New("throttled"))

	p := NewPublisher(sink, PublisherConfig{Interval: 5 * time.Millisecond, Log: quietLog()})

	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Failed < 2 && time.Now().Before(deadline) {
		p.Message()
		time.Sleep(time.Millisecond)
	}
	if got := p.Stats().Failed; got < 2 {
		t.Fatalf("failed publishes = %d, want at least 2", got)
	}
	if sink.count() != 0 {
		t.Fatalf("failing sink recorded %d calls", sink.count())
	}

	// Recovery: the ticker is still running, so the next interval lands.
	sink.fail(nil)
	deadline = time.Now().Add(2 * time.Second)
	for sink.count() == 0 && time.Now().Before(deadline) {
		p.Message()
		time.Sleep(time.Millisecond)
	}
	p.Close()

	if sink.count() == 0 {
		t.Error("publisher never recovered after a failing sink was fixed")
	}
	if p.Stats().Published == 0 {
		t.Error("stats never counted a successful publish")
	}
}

// The capture writer records on one goroutine and the publisher collects on
// another. This is the only place the two meet, and -race is the test.
func TestConcurrentRecordAndCollect(t *testing.T) {
	sink := &fakeSink{}
	p := NewPublisher(sink, PublisherConfig{Interval: time.Millisecond, Log: quietLog()})

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				p.Message()
				p.Lag(time.Duration(i) * time.Microsecond)
				p.QueueDepth(i % 64)
				if i%500 == 0 {
					p.Gap()
				}
			}
		}()
	}
	wg.Wait()
	p.Close()

	var messages, gaps float64
	for _, call := range sink.calls {
		for _, d := range call {
			switch d.Name {
			case MetricMessages:
				messages += d.Value
			case MetricGaps:
				gaps += d.Value
			}
		}
	}
	if messages != 8000 {
		t.Errorf("messages across all intervals = %v, want 8000", messages)
	}
	if gaps != 16 {
		t.Errorf("gaps across all intervals = %v, want 16", gaps)
	}
}
