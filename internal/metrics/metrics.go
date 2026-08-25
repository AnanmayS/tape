// Package metrics turns what a capture session is doing into a handful of
// numbers per minute.
//
// The shape of this package is decided by one constraint: a capture session
// must not depend on AWS being reachable, or on AWS existing at all. Metrics
// are off unless a namespace is named, the aggregation is pure Go with no SDK
// anywhere near it, and publishing happens on its own goroutine where a failure
// is a log line rather than a lost frame. The CloudWatch half lives in the
// cwmetrics subpackage, the same way the S3 half of storage lives in s3store,
// so that everything here is testable without a credential.
//
// # Why aggregate
//
// PutMetricData is a network call. One per message would cost more than the
// capture does and would be billed per request besides. A Collector folds a
// minute of messages into five numbers — a count, a rate, a gap count, a
// summary of the ingest lag, and the deepest the queue got — and the publisher
// sends them in a single call. The lag goes as a StatisticSet, so CloudWatch
// gets the real minimum, maximum, sum and sample count rather than an average
// of averages.
//
// # Which five, and why not more
//
// Custom metrics are billed per metric per month, so each one has to earn its
// place. These are the numbers README.md commits to reporting: throughput,
// gaps, and the lag that moves first when the reader stops keeping up with the
// socket. Everything else a session counts is already in the summary it logs.
package metrics

import (
	"sync"
	"time"
)

// Metric names. They are constants because three things have to agree on them:
// the emitter, the CloudWatch alarms in terraform/alarms.tf, and anyone reading
// a graph. A typo in any one of those produces an alarm that never fires, which
// is the failure mode that looks exactly like success.
const (
	// MetricMessages is the number of data frames received in the interval.
	MetricMessages = "MessagesReceived"

	// MetricMessageRate is that count over the interval's length. It is
	// redundant with MessagesReceived divided by the period, and it is here
	// anyway because the sustained rate is the number this project quotes and
	// a graph of it should not need arithmetic to read.
	MetricMessageRate = "MessageRate"

	// MetricGaps is the number of sequence gaps recorded in the interval. Any
	// non-zero value means part of this session is untrustworthy.
	MetricGaps = "GapsDetected"

	// MetricIngestLag is the exchange-timestamp-to-write delay, as a statistic
	// set over the interval's messages.
	MetricIngestLag = "IngestLag"

	// MetricQueueDepth is the deepest the reader-to-writer channel got. It is
	// the first real backpressure signal and M7's starting point.
	MetricQueueDepth = "QueueDepth"
)

// DimensionProduct is the one dimension every metric carries. One dimension,
// not three: an alarm has to name every dimension of the metric it watches, so
// each extra one is another way for an alarm to sit silently on a metric that
// nothing writes. v1 is one exchange and one product.
const DimensionProduct = "Product"

// Unit is what a datum's numbers mean. The set is deliberately tiny; the
// CloudWatch mapping lives in the cwmetrics subpackage so that nothing here
// imports an SDK.
type Unit string

const (
	UnitCount          Unit = "Count"
	UnitCountPerSecond Unit = "Count/Second"
	UnitSeconds        Unit = "Seconds"
)

// Recorder is what the capture path calls. It is an interface so that the
// default is a value that does nothing: a session without metrics pays three
// inlined no-op method calls per message and reaches no network at all.
type Recorder interface {
	// Message records one data frame received from the exchange.
	Message()

	// Lag records the delay between the exchange's timestamp on a message and
	// the moment this process finished writing it. Frames that carry no
	// exchange timestamp are not reported here: a missing timestamp is not a
	// lag of zero, and averaging it in as one would drag the number toward a
	// reassuring lie.
	Lag(d time.Duration)

	// Gap records one sequence gap.
	Gap()

	// QueueDepth records an observation of the reader-to-writer channel depth.
	// Only the maximum over an interval is published.
	QueueDepth(n int)
}

// Nop is a Recorder that discards everything. It is the default, and it is what
// every test and every local capture uses.
type Nop struct{}

func (Nop) Message()          {}
func (Nop) Lag(time.Duration) {}
func (Nop) Gap()              {}
func (Nop) QueueDepth(int)    {}

var _ Recorder = Nop{}

// Stat summarises a set of observations. It is what CloudWatch calls a
// StatisticSet, and it is the honest way to send a distribution in one datum:
// the minimum and maximum survive, which is the whole point when the
// interesting event is one message arriving late among thousands that did not.
type Stat struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Observe folds one value in.
func (s *Stat) Observe(v float64) {
	if s.Count == 0 || v < s.Min {
		s.Min = v
	}
	if s.Count == 0 || v > s.Max {
		s.Max = v
	}
	s.Sum += v
	s.Count++
}

// Mean is the arithmetic mean, or 0 for an empty Stat.
func (s Stat) Mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// Datum is one metric over one interval. Either Value carries the number, or
// Statistics summarises many observations of it; Statistics wins when set.
type Datum struct {
	Name       string
	Unit       Unit
	Time       time.Time
	Value      float64
	Statistics *Stat
}

// Snapshot is one interval's worth of counters, already closed off.
type Snapshot struct {
	// Start and End bound the interval. End is the timestamp the datapoints
	// carry.
	Start time.Time
	End   time.Time

	Messages int64
	Gaps     int64

	// Lag is the ingest lag in seconds over the interval's messages.
	Lag Stat

	// MaxQueueDepth is the deepest observation, and QueueObserved says whether
	// there was one. A depth of zero is a real reading; no reading at all is
	// not, and publishing it as zero would claim an idle queue on an interval
	// where the writer never ran.
	MaxQueueDepth int
	QueueObserved bool
}

// Duration is the interval's length.
func (s Snapshot) Duration() time.Duration { return s.End.Sub(s.Start) }

// MessagesPerSecond is the sustained receive rate over the interval.
func (s Snapshot) MessagesPerSecond() float64 {
	d := s.Duration().Seconds()
	if d <= 0 {
		return 0
	}
	return float64(s.Messages) / d
}

// Empty reports whether nothing at all happened in the interval. An empty
// snapshot is still published: a minute with zero messages is a fact worth
// having on a graph, and a gap in the line would be indistinguishable from a
// gap in the publishing.
func (s Snapshot) Empty() bool {
	return s.Messages == 0 && s.Gaps == 0 && s.Lag.Count == 0 && !s.QueueObserved
}

// Data renders the snapshot as datapoints. Metrics with nothing behind them are
// left out rather than sent as zero — an interval in which no message carried
// an exchange timestamp has no lag, and a zero would be a measurement this
// process never made.
func (s Snapshot) Data() []Datum {
	at := s.End
	data := []Datum{
		{Name: MetricMessages, Unit: UnitCount, Time: at, Value: float64(s.Messages)},
		{Name: MetricMessageRate, Unit: UnitCountPerSecond, Time: at, Value: s.MessagesPerSecond()},
		{Name: MetricGaps, Unit: UnitCount, Time: at, Value: float64(s.Gaps)},
	}
	if s.Lag.Count > 0 {
		lag := s.Lag
		data = append(data, Datum{Name: MetricIngestLag, Unit: UnitSeconds, Time: at, Statistics: &lag})
	}
	if s.QueueObserved {
		data = append(data, Datum{Name: MetricQueueDepth, Unit: UnitCount, Time: at, Value: float64(s.MaxQueueDepth)})
	}
	return data
}

// Collector is the aggregation half: a Recorder that accumulates into a
// Snapshot and hands it over on Collect.
//
// It takes a mutex on every observation. That is affordable because every
// caller is the same goroutine — the capture writer records messages, gaps and
// queue depth, and only Collect comes from anywhere else — so the lock is
// uncontended and costs tens of nanoseconds against a sustained rate measured
// in tens of messages a second. If M7 ever finds it on a profile, the fix is
// atomics for the counters; until then a mutex is the version that is obviously
// correct.
type Collector struct {
	now func() time.Time

	mu    sync.Mutex
	start time.Time
	snap  Snapshot
}

var _ Recorder = (*Collector)(nil)

// NewCollector returns a Collector whose first interval starts now.
func NewCollector() *Collector { return newCollector(time.Now) }

// newCollector lets the tests drive the clock. Every duration this package
// reports is a difference of two readings from it, so a test can produce an
// exact rate rather than a nearly-right one.
func newCollector(now func() time.Time) *Collector {
	if now == nil {
		now = time.Now
	}
	return &Collector{now: now, start: now().UTC()}
}

func (c *Collector) Message() {
	c.mu.Lock()
	c.snap.Messages++
	c.mu.Unlock()
}

func (c *Collector) Lag(d time.Duration) {
	c.mu.Lock()
	c.snap.Lag.Observe(d.Seconds())
	c.mu.Unlock()
}

func (c *Collector) Gap() {
	c.mu.Lock()
	c.snap.Gaps++
	c.mu.Unlock()
}

func (c *Collector) QueueDepth(n int) {
	c.mu.Lock()
	if !c.snap.QueueObserved || n > c.snap.MaxQueueDepth {
		c.snap.MaxQueueDepth = n
	}
	c.snap.QueueObserved = true
	c.mu.Unlock()
}

// Collect closes off the current interval and starts the next one. The next
// interval begins where this one ended, not at the moment Collect happened to
// be called, so intervals tile the session without overlap or holes.
func (c *Collector) Collect() Snapshot {
	end := c.now().UTC()

	c.mu.Lock()
	defer c.mu.Unlock()

	s := c.snap
	s.Start, s.End = c.start, end

	c.snap = Snapshot{}
	c.start = end
	return s
}
