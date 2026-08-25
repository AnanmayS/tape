// Package capture wires a feed to a tape file: a reader goroutine pulls frames
// off the socket into a buffered channel, and the writer goroutine drains that
// channel to disk.
//
// The split is not decoration. A socket read that waits on a disk write is a
// socket read that stops draining the kernel buffer, and the channel between
// the two is the thing M7 has to size and measure. It goes in now so that the
// measurement later is of the real design rather than of a rewrite.
package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Config configures a capture session.
type Config struct {
	// Root is the data directory. Files land at
	// Root/{symbol}/{date}/{window start}.tape.
	Root string

	// Window is the wall-clock rotation window.
	Window time.Duration

	// Buffer is the depth of the channel between reader and writer.
	Buffer int

	// FlushInterval bounds how long written records sit in the buffer.
	FlushInterval time.Duration

	// Log receives structured progress. Required in practice; nil falls back
	// to the default logger.
	Log *slog.Logger
}

func (c *Config) withDefaults() {
	if c.Window <= 0 {
		c.Window = tapefile.DefaultWindow
	}
	if c.Buffer <= 0 {
		c.Buffer = 4096
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Summary is what a capture session did. Every field is counted, not estimated.
type Summary struct {
	Feed    string
	Product string
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

	// MaxQueueDepth is the deepest the reader-to-writer channel got. It is the
	// first real number about backpressure, and M7's starting point.
	MaxQueueDepth int

	Files []string
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
		"max_queue_depth", s.MaxQueueDepth,
		"messages_per_sec", fmt.Sprintf("%.1f", s.MessagesPerSecond()),
	}
}

// Run captures f until ctx is cancelled or the feed stops, and returns what
// happened. The returned Summary is valid even when the error is non-nil: a
// session that died halfway still wrote what it wrote.
func Run(ctx context.Context, f feed.Feed, cfg Config) (Summary, error) {
	cfg.withDefaults()
	log := cfg.Log.With("feed", f.Name(), "product", f.Product())

	w, err := tapefile.NewWriter(cfg.Root, f.Product(), cfg.Window)
	if err != nil {
		return Summary{}, err
	}

	sum := Summary{
		Feed:    f.Name(),
		Product: f.Product(),
		Started: time.Now().UTC(),
	}
	sink := &sink{w: w, seq: newSeqTracker(f.SeqMode()), log: log, sum: &sum}

	// If the writer stops first, the reader must be told, or it will block
	// forever on a send nobody is receiving.
	ctx, stopReader := context.WithCancel(ctx)
	defer stopReader()

	// Reader goroutine: socket to channel.
	frames := make(chan feed.Frame, cfg.Buffer)
	readErr := make(chan error, 1)
	go func() {
		err := f.Run(ctx, frames)
		close(frames)
		readErr <- err
	}()

	// Writer goroutine: channel to disk. This is that goroutine.
	flush := time.NewTicker(cfg.FlushInterval)
	defer flush.Stop()

	var writeErr error
drain:
	for {
		select {
		case fr, ok := <-frames:
			if !ok {
				break drain
			}
			if d := len(frames) + 1; d > sum.MaxQueueDepth {
				sum.MaxQueueDepth = d
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

	if cerr := w.Close(); writeErr == nil {
		writeErr = cerr
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
	st := w.Stats()
	sum.Records, sum.Bytes, sum.Rotations, sum.Files = st.Records, st.Bytes, st.Rotations, st.Files

	return sum, errors.Join(writeErr, feedErr)
}

// readerStopTimeout bounds how long shutdown waits for the reader goroutine.
const readerStopTimeout = 10 * time.Second

// sink turns frames into records. It owns the writer, the sequence tracker and
// the counters, so that every path from a frame to disk goes through one place.
type sink struct {
	w   *tapefile.Writer
	seq *seqTracker
	log *slog.Logger
	sum *Summary
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
		return s.w.WriteReseed(tapefile.Reseed{At: fr.Recv, Reason: fr.Reason})

	case feed.KindData:
		s.sum.Messages++

		e, err := event.Decode(fr.Raw, fr.Recv)
		if err != nil {
			// Write it anyway. A frame we cannot parse is still evidence, and
			// discarding it would be the silent kind of data loss.
			s.sum.DecodeErrors++
			s.log.Warn("undecodable frame", "err", err, "bytes", len(fr.Raw))
		} else {
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

		if err := s.w.WriteMessage(tapefile.Message{Recv: fr.Recv, Raw: fr.Raw}); err != nil {
			return err
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
		s.log.Debug("stale message after reseed",
			"product", e.Product, "sequence", e.Sequence)
		return nil
	}
	if a.Kind == AnomalyNone {
		return nil
	}

	s.sum.Gaps++
	s.log.Warn("sequence gap",
		"kind", a.Kind.String(),
		"product", e.Product,
		"expected", a.Expected,
		"got", a.Got,
		"missing", int64(a.Got)-int64(a.Expected))
	return s.w.WriteGap(tapefile.Gap{At: at, Expected: a.Expected, Got: a.Got})
}

// progressEvery is how often the running count is logged. A constant rather
// than a knob: there is one caller and no reason for it to differ.
const progressEvery = 5000
