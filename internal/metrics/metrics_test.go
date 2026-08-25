package metrics

import (
	"math"
	"testing"
	"time"
)

// clock is a hand-driven time source. Every duration this package reports is a
// difference of two readings, so driving it by hand turns "about 60 messages a
// second" into exactly 60 and makes the assertion worth making.
type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestCollector() (*Collector, *clock) {
	c := &clock{t: time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)}
	return newCollector(c.now), c
}

func TestCollectorAggregatesOneInterval(t *testing.T) {
	col, clk := newTestCollector()

	for i := 0; i < 120; i++ {
		col.Message()
	}
	col.Gap()
	col.Gap()
	col.Lag(100 * time.Millisecond)
	col.Lag(900 * time.Millisecond)
	col.Lag(500 * time.Millisecond)
	col.QueueDepth(3)
	col.QueueDepth(41)
	col.QueueDepth(7)

	clk.add(60 * time.Second)
	s := col.Collect()

	if s.Messages != 120 {
		t.Errorf("messages = %d, want 120", s.Messages)
	}
	if s.Gaps != 2 {
		t.Errorf("gaps = %d, want 2", s.Gaps)
	}
	if got, want := s.MessagesPerSecond(), 2.0; got != want {
		t.Errorf("messages/sec = %v, want %v", got, want)
	}
	if s.Duration() != time.Minute {
		t.Errorf("duration = %v, want 1m", s.Duration())
	}

	// The maximum is the point of the queue-depth metric: a mean would hide
	// the one moment the writer fell behind, which is the only moment worth
	// looking at.
	if !s.QueueObserved || s.MaxQueueDepth != 41 {
		t.Errorf("queue depth = %d (observed %v), want 41 observed", s.MaxQueueDepth, s.QueueObserved)
	}

	if s.Lag.Count != 3 {
		t.Errorf("lag count = %d, want 3", s.Lag.Count)
	}
	if !closeTo(s.Lag.Min, 0.1) || !closeTo(s.Lag.Max, 0.9) || !closeTo(s.Lag.Sum, 1.5) {
		t.Errorf("lag = %+v, want min 0.1 max 0.9 sum 1.5", s.Lag)
	}
	if !closeTo(s.Lag.Mean(), 0.5) {
		t.Errorf("lag mean = %v, want 0.5", s.Lag.Mean())
	}
}

// Intervals must tile the session: the next one starts where the last one
// ended, not at whatever moment the ticker happened to fire. Otherwise a slow
// publish quietly shortens the following interval and inflates its rate.
func TestCollectorIntervalsTile(t *testing.T) {
	col, clk := newTestCollector()

	col.Message()
	clk.add(30 * time.Second)
	first := col.Collect()

	if first.Messages != 1 {
		t.Fatalf("first messages = %d, want 1", first.Messages)
	}

	// Simulate the publish taking a moment before the next interval's work.
	clk.add(45 * time.Second)
	second := col.Collect()

	if !second.Start.Equal(first.End) {
		t.Errorf("second interval starts at %v, want %v", second.Start, first.End)
	}
	if second.Duration() != 45*time.Second {
		t.Errorf("second duration = %v, want 45s", second.Duration())
	}
	if !second.Empty() {
		t.Errorf("second interval = %+v, want empty after reset", second)
	}
	if second.Messages != 0 || second.Gaps != 0 || second.Lag.Count != 0 || second.QueueObserved {
		t.Errorf("counters not reset: %+v", second)
	}
}

func TestSnapshotDataOmitsWhatWasNeverMeasured(t *testing.T) {
	col, clk := newTestCollector()
	col.Message()
	clk.add(time.Minute)

	data := col.Collect().Data()
	byName := map[string]Datum{}
	for _, d := range data {
		byName[d.Name] = d
	}

	// The three that are always true of an interval.
	for _, name := range []string{MetricMessages, MetricMessageRate, MetricGaps} {
		if _, ok := byName[name]; !ok {
			t.Errorf("%s missing from %d datapoints", name, len(data))
		}
	}

	// An interval where no frame carried an exchange timestamp has no lag, and
	// no observation of the queue is not an observation of an empty queue.
	// Publishing either as zero would be reporting a measurement nobody made.
	if _, ok := byName[MetricIngestLag]; ok {
		t.Error("IngestLag published with no lag observations")
	}
	if _, ok := byName[MetricQueueDepth]; ok {
		t.Error("QueueDepth published with no depth observations")
	}
}

func TestSnapshotDataUnitsAndValues(t *testing.T) {
	col, clk := newTestCollector()
	for i := 0; i < 30; i++ {
		col.Message()
	}
	col.Gap()
	col.Lag(2 * time.Second)
	col.QueueDepth(12)
	clk.add(10 * time.Second)

	want := map[string]struct {
		unit  Unit
		value float64
	}{
		MetricMessages:    {UnitCount, 30},
		MetricMessageRate: {UnitCountPerSecond, 3},
		MetricGaps:        {UnitCount, 1},
		MetricQueueDepth:  {UnitCount, 12},
	}

	snap := col.Collect()
	seen := map[string]bool{}
	for _, d := range snap.Data() {
		seen[d.Name] = true
		if !d.Time.Equal(snap.End) {
			t.Errorf("%s timestamped %v, want interval end %v", d.Name, d.Time, snap.End)
		}
		if d.Name == MetricIngestLag {
			if d.Unit != UnitSeconds {
				t.Errorf("IngestLag unit = %q, want %q", d.Unit, UnitSeconds)
			}
			if d.Statistics == nil || d.Statistics.Count != 1 || !closeTo(d.Statistics.Max, 2) {
				t.Errorf("IngestLag statistics = %+v, want one sample of 2s", d.Statistics)
			}
			continue
		}
		w, ok := want[d.Name]
		if !ok {
			t.Errorf("unexpected metric %q", d.Name)
			continue
		}
		if d.Unit != w.unit {
			t.Errorf("%s unit = %q, want %q", d.Name, d.Unit, w.unit)
		}
		if !closeTo(d.Value, w.value) {
			t.Errorf("%s value = %v, want %v", d.Name, d.Value, w.value)
		}
		if d.Statistics != nil {
			t.Errorf("%s carries both a value and a statistic set", d.Name)
		}
	}
	if !seen[MetricIngestLag] {
		t.Error("IngestLag missing after a lag observation")
	}
}

func TestStatObserve(t *testing.T) {
	var s Stat
	if s.Count != 0 || s.Mean() != 0 {
		t.Fatalf("zero Stat = %+v, mean %v", s, s.Mean())
	}

	// A negative lag is real: Coinbase stamps its own clock, and a clock a
	// little ahead of ours produces a message that arrives "before" it was
	// sent. Clamping it to zero would hide exactly the skew worth knowing about.
	s.Observe(-0.25)
	if !closeTo(s.Min, -0.25) || !closeTo(s.Max, -0.25) || s.Count != 1 {
		t.Errorf("after one sample: %+v", s)
	}

	s.Observe(4)
	s.Observe(1)
	if s.Count != 3 || !closeTo(s.Min, -0.25) || !closeTo(s.Max, 4) || !closeTo(s.Sum, 4.75) {
		t.Errorf("after three samples: %+v", s)
	}
	if !closeTo(s.Mean(), 4.75/3) {
		t.Errorf("mean = %v", s.Mean())
	}
}

// An interval in which nothing happened is still published. A flat line at zero
// and a hole in the graph mean different things, and only one of them means the
// feed went quiet.
func TestEmptyIntervalStillPublishesTheCounts(t *testing.T) {
	col, clk := newTestCollector()
	clk.add(time.Minute)

	snap := col.Collect()
	if !snap.Empty() {
		t.Fatalf("snapshot = %+v, want empty", snap)
	}
	data := snap.Data()
	if len(data) != 3 {
		t.Fatalf("empty interval produced %d datapoints, want 3", len(data))
	}
	for _, d := range data {
		if d.Value != 0 {
			t.Errorf("%s = %v on an empty interval, want 0", d.Name, d.Value)
		}
	}
}

func TestZeroDurationRateIsNotInfinite(t *testing.T) {
	col, _ := newTestCollector()
	col.Message()
	if got := col.Collect().MessagesPerSecond(); got != 0 {
		t.Errorf("rate over a zero-length interval = %v, want 0", got)
	}
}

func TestNopRecordsNothing(t *testing.T) {
	var r Recorder = Nop{}
	r.Message()
	r.Lag(time.Second)
	r.Gap()
	r.QueueDepth(9)
}

func closeTo(got, want float64) bool { return math.Abs(got-want) < 1e-9 }
