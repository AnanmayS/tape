package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Sink is where a published interval goes. One method, because there is one
// thing to do with a snapshot; the CloudWatch implementation is in cwmetrics.
type Sink interface {
	// Publish sends one interval's datapoints. It must be safe to call from a
	// single goroutine and is never called concurrently with itself.
	Publish(ctx context.Context, data []Datum) error

	// String names the sink for logs.
	String() string
}

// PublisherConfig configures a Publisher. The zero value is usable.
type PublisherConfig struct {
	// Interval is how often a snapshot is taken and published.
	Interval time.Duration

	// Timeout bounds one Publish call.
	Timeout time.Duration

	Log *slog.Logger
}

func (c *PublisherConfig) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Minute
	}
	if c.Timeout <= 0 {
		c.Timeout = 20 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// PublishStats counts what a Publisher did. Like every other counter in this
// project it is counted rather than estimated, and it is logged with the
// session summary so that "the graph is empty" and "the graph is flat" are
// distinguishable afterwards.
type PublishStats struct {
	// Intervals is how many snapshots were taken.
	Intervals int64

	// Published is how many Publish calls succeeded, and Failed how many did
	// not. A failure loses that interval's numbers and nothing else.
	Published int64
	Failed    int64

	// Datapoints is how many datapoints were accepted by the sink.
	Datapoints int64
}

// LogAttrs renders the stats for a structured logger, prefixed because they are
// logged beside a capture summary that has counters of its own.
func (s PublishStats) LogAttrs() []any {
	return []any{
		"metrics_intervals", s.Intervals,
		"metrics_published", s.Published,
		"metrics_failed", s.Failed,
		"metrics_datapoints", s.Datapoints,
	}
}

// Publisher is a Collector with a goroutine behind it that ships each interval
// to a Sink.
//
// Nothing it does can fail a capture. Publishing runs on its own goroutine, so
// a slow or unreachable CloudWatch never reaches the capture path; a failed
// call is logged and the interval is dropped, because a metric is a description
// of the capture and the capture is the thing that cannot be re-run. Retrying
// would only queue stale datapoints behind fresh ones.
type Publisher struct {
	*Collector

	sink Sink
	cfg  PublisherConfig

	stop chan struct{}
	done chan struct{}

	// ctx is cancelled on Close so an in-flight publish during shutdown does
	// not hold the process open past its timeout.
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	stats PublishStats

	closeOnce sync.Once
}

var _ Recorder = (*Publisher)(nil)

// NewPublisher starts a publisher. Close it when the session ends; Close takes
// a final snapshot, so the last partial interval is published rather than
// discarded.
func NewPublisher(sink Sink, cfg PublisherConfig) *Publisher {
	cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		Collector: NewCollector(),
		sink:      sink,
		cfg:       cfg,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}
	go p.run()
	return p
}

// Stats returns a snapshot of the publishing counters.
func (p *Publisher) Stats() PublishStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Close stops the ticker, publishes the final partial interval, and returns
// once the goroutine has stopped. It is safe to call more than once.
func (p *Publisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.stop)
		<-p.done
		p.cancel()
	})
	return nil
}

func (p *Publisher) run() {
	defer close(p.done)

	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.flush()
		case <-p.stop:
			// The final interval is short and is published anyway. A session
			// that ran for ninety seconds should show ninety seconds of
			// messages, not sixty and a shrug.
			p.flush()
			return
		}
	}
}

// flush takes a snapshot and publishes it.
func (p *Publisher) flush() {
	snap := p.Collect()
	data := snap.Data()

	p.bump(func(s *PublishStats) { s.Intervals++ })
	if len(data) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.Timeout)
	defer cancel()

	if err := p.sink.Publish(ctx, data); err != nil {
		p.bump(func(s *PublishStats) { s.Failed++ })
		p.cfg.Log.Error("metrics publish failed",
			"err", err, "sink", p.sink.String(), "datapoints", len(data),
			"remedy", "capture is unaffected; this interval's numbers are lost")
		return
	}
	p.bump(func(s *PublishStats) {
		s.Published++
		s.Datapoints += int64(len(data))
	})
	p.cfg.Log.Debug("metrics published",
		"sink", p.sink.String(),
		"datapoints", len(data),
		"messages", snap.Messages,
		"gaps", snap.Gaps,
		"messages_per_sec", snap.MessagesPerSecond())
}

func (p *Publisher) bump(fn func(*PublishStats)) {
	p.mu.Lock()
	fn(&p.stats)
	p.mu.Unlock()
}
