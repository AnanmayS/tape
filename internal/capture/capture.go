// Package capture wires a feed to a tape file: a reader goroutine pulls frames
// off the socket into a queue, and the writer goroutine drains that queue to
// disk.
//
// The split is not decoration. A socket read that waits on a disk write is a
// socket read that stops draining the kernel buffer, and what the queue does
// when it fills is the backpressure policy — block, drop or buffer. It is
// PolicyBlock, by measurement rather than by default; policy.go says what each
// one costs and CLAUDE.md carries the number that decided it.
//
// Two things about that queue are instrumented, because they are the two halves
// of the same question. Its depth says the writer fell behind. The per-record
// write latency in the Summary says by how much and how often, which is what
// decides whether "behind" is a burst the queue absorbs or a rate it cannot.
package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AnanmayS/tape/internal/colfmt"
	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/metrics"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Format selects the on-disk format a session writes. Both are read by replay
// without being told which is which, so this is a storage decision and not a
// decision about what the data is.
type Format string

const (
	// FormatRaw is the v1 record log: every frame written as it arrives.
	FormatRaw Format = "raw"

	// FormatColumnar is the v2 delta-encoded columnar format, about 5.8 times
	// smaller on a real BTC-USD window.
	FormatColumnar Format = "columnar"
)

// Config configures a capture session.
type Config struct {
	// Root is the local data directory. Files land at Root plus the storage
	// key: Root/v1/symbol={symbol}/date={date}/hour={hour}/{start}.tape.
	Root string

	// Store, if set, receives every file as it closes. Local disk stays the
	// durable copy either way — an upload is a copy of a file that is already
	// safely written, never the only place a window lives.
	Store storage.Store

	// Upload configures the uploader used when Store is set.
	Upload storage.UploadConfig

	// Format is the on-disk format. Empty means FormatRaw; see the flag's help
	// in cmd/tape for why that is still the default.
	Format Format

	// Window is the wall-clock rotation window.
	Window time.Duration

	// Buffer is the depth of the channel between reader and writer.
	Buffer int

	// Policy is what happens when that channel is full. Empty means
	// PolicyBlock; policy.go says why.
	Policy Policy

	// FlushInterval bounds how long written records sit in the buffer.
	FlushInterval time.Duration

	// Log receives structured progress. Required in practice; nil falls back
	// to the default logger.
	Log *slog.Logger

	// Metrics receives the same counts the summary reports, one interval at a
	// time. nil means metrics.Nop: a session that was not told where to publish
	// does not publish, and reaches no network to find that out.
	Metrics metrics.Recorder
}

func (c *Config) withDefaults() {
	if c.Format == "" {
		c.Format = FormatRaw
	}
	if c.Window <= 0 {
		c.Window = tapefile.DefaultWindow
	}
	if c.Buffer <= 0 {
		c.Buffer = 4096
	}
	if c.Policy == "" {
		c.Policy = PolicyBlock
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Metrics == nil {
		c.Metrics = metrics.Nop{}
	}
}

// Summary is what a capture session did. Every field is counted, not estimated.
type Summary struct {
	Feed    string
	Product string
	Format  Format
	Policy  Policy
	Started time.Time
	Ended   time.Time

	// Messages counts data frames received from the exchange.
	Messages int64

	// Records counts everything written to disk, including gap and reseed
	// records.
	Records int64

	Bytes     int64
	Rotations int
	Reseeds   int64

	// Gaps counts sequence anomalies written into the stream as gap records.
	// A non-zero count means part of this session is untrustworthy: the public
	// feed offers no backfill, so the gap record is the correction.
	Gaps int64

	// StaleMessages counts messages the exchange re-sent from before a fresh
	// snapshot. Expected after a reseed, and not gaps. They are still stored.
	StaleMessages int64

	// DecodeErrors counts frames that would not parse. They are still written:
	// an unparseable frame is data we cannot afford to discard.
	DecodeErrors int64

	// ExchangeErrors counts error frames the exchange sent back.
	ExchangeErrors int64

	// MaxQueueDepth is the deepest the reader-to-writer queue got, counting
	// anything the buffer policy was holding in front of the channel.
	MaxQueueDepth int

	// Dropped counts frames the backpressure policy discarded. Every one of
	// them is also inside Gaps, because a drop is written into the stream as a
	// gap record; the two are not added together.
	Dropped int64

	// WriteLatency is the distribution of per-record write times. It is the
	// number that says whether the writer can drain the queue, and its tail is
	// where a columnar batch flush shows up.
	WriteLatency Latency

	Files []string

	// Store names where files were uploaded, or "" if nowhere.
	Store string

	// Upload counts what the uploader did. A session that uploaded nothing
	// because the bucket was unreachable is a session that still captured
	// everything, and these are the numbers that say so.
	Upload storage.UploadStats
}

// Duration is the wall-clock length of the session.
func (s Summary) Duration() time.Duration { return s.Ended.Sub(s.Started) }

// MessagesPerSecond is the sustained receive rate over the session.
func (s Summary) MessagesPerSecond() float64 {
	d := s.Duration().Seconds()
	if d <= 0 {
		return 0
	}
	return float64(s.Messages) / d
}

// LogAttrs renders the summary for a structured logger.
func (s Summary) LogAttrs() []any {
	attrs := s.captureAttrs()
	if s.Store != "" {
		attrs = append(attrs, "store", s.Store)
		attrs = append(attrs, s.Upload.LogAttrs()...)
	}
	return attrs
}

func (s Summary) captureAttrs() []any {
	return []any{
		"feed", s.Feed,
		"product", s.Product,
		"duration", s.Duration().Round(time.Millisecond).String(),
		"messages", s.Messages,
		"records", s.Records,
		"bytes", s.Bytes,
		"files", len(s.Files),
		"rotations", s.Rotations,
		"reseeds", s.Reseeds,
		"gaps", s.Gaps,
		"stale_messages", s.StaleMessages,
		"decode_errors", s.DecodeErrors,
		"exchange_errors", s.ExchangeErrors,
		"policy", string(s.Policy),
		"max_queue_depth", s.MaxQueueDepth,
		"dropped", s.Dropped,
		"write_latency", s.WriteLatency.String(),
		"messages_per_sec", fmt.Sprintf("%.1f", s.MessagesPerSecond()),
	}
}

// Run captures f until ctx is cancelled or the feed stops, and returns what
// happened. The returned Summary is valid even when the error is non-nil: a
// session that died halfway still wrote what it wrote.
func Run(ctx context.Context, f feed.Feed, cfg Config) (Summary, error) {
	cfg.withDefaults()
	if err := validatePolicy(cfg.Policy); err != nil {
		return Summary{}, err
	}
	log := cfg.Log.With("feed", f.Name(), "product", f.Product())

	// The uploader is wired in through the writer's closed-file hook, so a
	// window is offered to the store at exactly the moment it stops being
	// written to and not one record earlier. Add never blocks, so the hook
	// firing on the writer goroutine costs the capture path nothing.
	var up *storage.Uploader
	var opts []tapefile.Option
	if cfg.Store != nil {
		ucfg := cfg.Upload
		if ucfg.Log == nil {
			ucfg.Log = log
		}
		up = storage.NewUploader(cfg.Store, ucfg)
		opts = append(opts, tapefile.OnFileClosed(func(fl tapefile.File) {
			up.Add(fl.Path, fl.Key)
		}))
	}

	w, err := newWriter(cfg, f.Product(), opts)
	if err != nil {
		if up != nil {
			up.Close()
		}
		return Summary{}, err
	}

	sum := Summary{
		Feed:    f.Name(),
		Product: f.Product(),
		Format:  cfg.Format,
		Policy:  cfg.Policy,
		Started: time.Now().UTC(),
	}
	if cfg.Store != nil {
		sum.Store = cfg.Store.String()
	}
	sink := &sink{w: w, seq: newSeqTracker(f.SeqMode()), log: log, sum: &sum, met: cfg.Metrics}

	// If the writer stops first, the reader must be told, or it will block
	// forever on a send nobody is receiving.
	ctx, stopReader := context.WithCancel(ctx)
	defer stopReader()

	// The queue carries the backpressure policy; see policy.go. writerDone is
	// how it learns that nobody is receiving any more, which is the one case
	// where a policy that never discards has to.
	q := newQueue(cfg.Policy, cfg.Buffer)
	writerDone := make(chan struct{})

	// Reader goroutine: socket to queue.
	readErr := make(chan error, 1)
	go func() {
		err := f.Run(ctx, q)
		q.close(writerDone)
		readErr <- err
	}()

	// Writer goroutine: queue to disk. This is that goroutine.
	flush := time.NewTicker(cfg.FlushInterval)
	defer flush.Stop()

	var writeErr error
drain:
	for {
		select {
		case fr, ok := <-q.ch:
			if !ok {
				break drain
			}
			d := q.depth() + 1
			if d > sum.MaxQueueDepth {
				sum.MaxQueueDepth = d
			}
			cfg.Metrics.QueueDepth(d)

			// Drops go into the stream ahead of the frame that followed them,
			// the same way a sequence gap does: here is where the stream broke,
			// and here is what resumed.
			if n := q.takePending(); n > 0 {
				if err := sink.recordDrop(n, fr.Recv); err != nil {
					writeErr = err
					break drain
				}
			}
			if err := sink.handle(fr); err != nil {
				writeErr = err
				break drain
			}
		case <-flush.C:
			if err := w.Flush(); err != nil {
				writeErr = err
				break drain
			}
		}
	}
	close(writerDone)

	// A drop that landed after the last frame still has to be recorded, or the
	// session would end with frames missing and nothing saying so.
	if n := q.takePending(); n > 0 && writeErr == nil {
		writeErr = sink.recordDrop(n, time.Now().UTC())
	}
	sum.WriteLatency = sink.hist.summary()

	// Close before the uploader: closing the writer is what hands the last
	// file over, and a drain started before that would miss it.
	if cerr := w.Close(); writeErr == nil {
		writeErr = cerr
	}
	var uploadErr error
	if up != nil {
		uploadErr = up.Close()
		sum.Upload = up.Stats()
	}

	// Let the reader notice it is done, then collect its result rather than
	// leaking the goroutine.
	stopReader()
	var feedErr error
	select {
	case feedErr = <-readErr:
	case <-time.After(readerStopTimeout):
		feedErr = errors.New("capture: feed did not stop within " + readerStopTimeout.String())
	}
	// A cancelled context is how a clean shutdown is spelled, not a failure.
	if errors.Is(feedErr, context.Canceled) || errors.Is(feedErr, context.DeadlineExceeded) {
		feedErr = nil
	}

	sum.Ended = time.Now().UTC()
	sum.Dropped = q.dropCount()
	st := w.Stats()
	sum.Records, sum.Bytes, sum.Rotations, sum.Files = st.Records, st.Bytes, st.Rotations, st.Files

	// An upload that did not finish is reported but does not make a capture a
	// failure: every byte is on local disk, complete, under the key the object
	// would have had. That is a re-run, not data loss.
	return sum, errors.Join(writeErr, feedErr, uploadErr)
}

// readerStopTimeout bounds how long shutdown waits for the reader goroutine.
const readerStopTimeout = 10 * time.Second

// recordWriter is everything capture needs from a tape writer. Both formats
// present exactly this, which is why choosing between them is a line in a
// switch rather than a second capture path.
type recordWriter interface {
	WriteMessage(tapefile.Message) error
	WriteGap(tapefile.Gap) error
	WriteReseed(tapefile.Reseed) error
	Flush() error
	Close() error
	Path() string
	Stats() tapefile.Stats
}

func newWriter(cfg Config, symbol string, opts []tapefile.Option) (recordWriter, error) {
	switch cfg.Format {
	case FormatColumnar:
		return colfmt.NewWriter(cfg.Root, symbol, cfg.Window, opts...)
	case FormatRaw:
		return tapefile.NewWriter(cfg.Root, symbol, cfg.Window, opts...)
	default:
		return nil, fmt.Errorf("capture: unknown format %q (want %s or %s)",
			cfg.Format, FormatRaw, FormatColumnar)
	}
}

// sink turns frames into records. It owns the writer, the sequence tracker and
// the counters, so that every path from a frame to disk goes through one place.
type sink struct {
	w    recordWriter
	seq  *seqTracker
	log  *slog.Logger
	sum  *Summary
	met  metrics.Recorder
	hist hist
}

// write times one record on its way to disk. Two clock reads per record is
// about 50ns against a write that costs microseconds, and it buys the only
// direct measurement of whether the writer can keep up with the socket.
func (s *sink) write(f func() error) error {
	start := time.Now()
	err := f()
	s.hist.observe(time.Since(start))
	return err
}

// recordDrop writes n discarded frames into the stream as a gap record.
//
// This is the whole price of a policy that sheds load, and it is not optional:
// a window that lost frames must say so, and on a monotonic feed the sequence
// numbers cannot. It counts as a gap everywhere a gap counts — the summary, the
// CloudWatch metric, the alarm whose threshold is zero — because it is one.
func (s *sink) recordDrop(n int64, at time.Time) error {
	s.sum.Gaps++
	s.met.Gap()
	s.log.Warn("frames dropped by backpressure policy", "dropped", n, "at", at)
	return s.write(func() error {
		return s.w.WriteGap(tapefile.Gap{At: at, Dropped: uint64(n)})
	})
}

// handle writes one frame and updates counters.
func (s *sink) handle(fr feed.Frame) error {
	before := s.w.Path()
	defer func() {
		if after := s.w.Path(); before != "" && after != before {
			s.log.Info("rotated", "closed", before, "open", after)
		}
	}()

	switch fr.Kind {
	case feed.KindReseed:
		s.sum.Reseeds++
		s.log.Info("reseed", "reason", fr.Reason)
		// Record where the fresh subscription landed before anything from it
		// is written, so a replayer sees the boundary in the right place.
		s.seq.reseed()
		return s.write(func() error {
			return s.w.WriteReseed(tapefile.Reseed{At: fr.Recv, Reason: fr.Reason})
		})

	case feed.KindData:
		s.sum.Messages++
		s.met.Message()

		// Kept past the decode so the lag can be measured against the moment
		// the record is actually written rather than the moment it was parsed.
		var exchangeTime time.Time

		e, err := event.Decode(fr.Raw, fr.Recv)
		if err != nil {
			// Write it anyway. A frame we cannot parse is still evidence, and
			// discarding it would be the silent kind of data loss.
			s.sum.DecodeErrors++
			s.log.Warn("undecodable frame", "err", err, "bytes", len(fr.Raw))
		} else {
			exchangeTime = e.ExchangeTime
			if e.IsError() {
				s.sum.ExchangeErrors++
				s.log.Error("exchange error frame", "detail", event.ErrorText(fr.Raw))
			}
			if e.HasSequence {
				if err := s.checkSequence(e, fr.Recv); err != nil {
					return err
				}
			}
		}

		if err := s.write(func() error {
			return s.w.WriteMessage(tapefile.Message{Recv: fr.Recv, Raw: fr.Raw})
		}); err != nil {
			return err
		}

		// Ingest lag is measured to the write and not to the socket read, so
		// that time spent waiting in the reader-to-writer channel is inside the
		// number rather than hidden behind it. That wait is exactly what grows
		// when the feed outruns the writer, which is the thing this metric is
		// for. Frames with no exchange timestamp — snapshots, control messages —
		// contribute nothing rather than a zero.
		if !exchangeTime.IsZero() {
			s.met.Lag(time.Now().UTC().Sub(exchangeTime))
		}

		if s.sum.Messages%progressEvery == 0 {
			st := s.w.Stats()
			s.log.Info("progress",
				"messages", s.sum.Messages,
				"records", st.Records,
				"bytes", st.Bytes,
				"gaps", s.sum.Gaps,
				"file", s.w.Path())
		}
		return nil

	default:
		return fmt.Errorf("capture: unknown frame kind %d", fr.Kind)
	}
}

// checkSequence records a gap record ahead of the message that revealed it, so
// the tape reads as "here is where continuity broke, and here is what resumed".
// A gap is written into the stream and not merely logged: the public feed
// offers no backfill, so the record is the only thing that can tell a later
// reader this window is untrustworthy.
func (s *sink) checkSequence(e event.Event, at time.Time) error {
	a, stale := s.seq.observe(e.Product, e.Sequence)
	if stale {
		s.sum.StaleMessages++
		s.log.Debug("stale message after reseed", "sequence", e.Sequence)
		return nil
	}
	if a.Kind == AnomalyNone {
		return nil
	}

	s.sum.Gaps++
	s.met.Gap()
	s.log.Warn("sequence gap",
		"kind", a.Kind.String(),
		"expected", a.Expected,
		"got", a.Got,
		"missing", int64(a.Got)-int64(a.Expected))
	return s.write(func() error {
		return s.w.WriteGap(tapefile.Gap{At: at, Expected: a.Expected, Got: a.Got})
	})
}

// progressEvery is how often the running count is logged. A constant rather
// than a knob: there is one caller and no reason for it to differ.
const progressEvery = 5000
