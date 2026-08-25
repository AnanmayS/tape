// Package bench drives the capture path at saturation so the backpressure
// policy can be chosen by measurement.
//
// The load generator is the project itself. A captured window is read back with
// the storage readers, held in memory as frames, and pushed into capture.Run
// through the same feed.Sink a live socket pushes into — so the sequence
// tracking, the gap detection, the rotation and the tape or columnar writing
// under load are the production ones, not a model of them. What is substituted
// is the socket, and only the socket.
//
// # What saturation means here
//
// With Speed zero the feed offers frames as fast as the sink will take them,
// which is faster than any exchange will ever send them. That is the point: the
// interesting number is not what the writer does at 40 messages a second, it is
// where it stops keeping up and what each policy does at that edge. The
// difference between the offered rate and the written rate is the headroom this
// project actually has, and it is three to four orders of magnitude.
//
// Speed above zero replays the window at a multiple of the wall-clock time it
// took to record, which is how the approach to that edge is measured rather
// than just its far side.
package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/colfmt"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/metrics"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Load is a captured window held in memory, ready to be pushed through the
// capture path.
//
// Only message records are kept, and they are kept in stored order — arrival
// order, the order the socket produced them — rather than in replay order.
// Replay order is sorted by exchange timestamp, and offering that to a sequence
// tracker manufactures thousands of regressions that never happened. Gap and
// reseed records are left behind too: they describe what happened to the
// connection that recorded this window, and replaying them would put someone
// else's reconnects into this measurement.
type Load struct {
	// Product is the symbol the window holds.
	Product string

	// Frames are the window's message frames in stored order, with their
	// original receive timestamps.
	Frames []frame

	// Bytes is the total size of the raw frames.
	Bytes int64

	// Span is the wall-clock time the window took to record. It is what a
	// speed multiple is a multiple of.
	Span time.Duration

	// seqSpan is how far the window's sequence numbers travelled. Each pass
	// after the first advances every sequence by this much, so that repeating a
	// window is a feed that kept going rather than one that went backwards.
	seqSpan uint64
}

// frame is one loaded message: the stored frame, and where its sequence number
// sits inside it.
type frame struct {
	raw  []byte
	recv time.Time

	// seqAt and seqEnd bound the digits of the sequence value in raw, and seq
	// is what they say. seqEnd is zero for a frame that carries none.
	seqAt, seqEnd int
	seq           uint64
}

// Rate is the rate the window was recorded at, in messages per second.
func (l *Load) Rate() float64 {
	if l.Span <= 0 {
		return 0
	}
	return float64(len(l.Frames)) / l.Span.Seconds()
}

// LoadWindow reads a captured window into memory. root is a directory of tape
// files or a single tape file, in either on-disk format.
func LoadWindow(root, product string) (*Load, error) {
	files, err := tapeFiles(root)
	if err != nil {
		return nil, err
	}

	l := &Load{Product: product}
	var minSeq, maxSeq uint64
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		recs, err := colfmt.OpenRecords(f)
		if err != nil {
			return nil, err
		}
		for {
			typ, payload, err := recs.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				recs.Close()
				return nil, fmt.Errorf("bench: read %s: %w", path, err)
			}
			if typ != tapefile.RecordMessage {
				continue
			}
			m, err := tapefile.DecodeMessage(payload)
			if err != nil {
				recs.Close()
				return nil, fmt.Errorf("bench: decode %s: %w", path, err)
			}
			fr := frame{raw: m.Raw, recv: m.Recv}
			fr.seqAt, fr.seqEnd, fr.seq = findSequence(m.Raw)
			if fr.seqEnd > 0 {
				if minSeq == 0 || fr.seq < minSeq {
					minSeq = fr.seq
				}
				if fr.seq > maxSeq {
					maxSeq = fr.seq
				}
			}
			l.Frames = append(l.Frames, fr)
			l.Bytes += int64(len(m.Raw))
		}
		recs.Close()
	}
	if len(l.Frames) == 0 {
		return nil, fmt.Errorf("bench: %s holds no message records", root)
	}
	l.Span = l.Frames[len(l.Frames)-1].recv.Sub(l.Frames[0].recv)
	if maxSeq > minSeq {
		l.seqSpan = maxSeq - minSeq + 1
	}
	return l, nil
}

// tapeFiles lists the window's files in the order their names sort, which for
// this key layout is time order. A single file is a window of one.
func tapeFiles(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{root}, nil
	}
	var out []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, storage.Ext) {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("bench: no %s files under %s", storage.Ext, root)
	}
	sort.Strings(out)
	return out, nil
}

// seqKey is the JSON member whose value the harness advances between passes.
var seqKey = []byte(`"sequence":`)

// findSequence locates the sequence value in a Coinbase frame. It is a byte
// scan rather than a decode because it runs once per frame at load time and
// because what is wanted is where the digits are, which a decoded value has
// already forgotten.
func findSequence(raw []byte) (start, end int, seq uint64) {
	i := bytes.Index(raw, seqKey)
	if i < 0 {
		return 0, 0, 0
	}
	start = i + len(seqKey)
	end = start
	for end < len(raw) && raw[end] >= '0' && raw[end] <= '9' {
		seq = seq*10 + uint64(raw[end]-'0')
		end++
	}
	if end == start {
		return 0, 0, 0
	}
	return start, end, seq
}

// Config is one run of the harness.
type Config struct {
	// Root is where the run writes its tape files. It is emptied by the caller,
	// not by this package: a harness that deletes directories is a harness one
	// typo away from deleting a capture.
	Root string

	// Policy and Format are what the run is measuring.
	Policy capture.Policy
	Format capture.Format

	// Repeat is how many times the window is pushed through. One pass of a
	// six-minute window is under a second of work at saturation, which is not
	// long enough to measure anything.
	Repeat int

	// Speed is the multiple of the original wall clock to replay at. Zero means
	// as fast as the sink will accept, which is what saturates the writer.
	Speed float64

	// Window, Buffer and FlushInterval are the capture settings under test.
	Window        time.Duration
	Buffer        int
	FlushInterval time.Duration

	// Interval is how often the run samples the queue and the heap. Zero means
	// SampleInterval.
	Interval time.Duration

	Log *slog.Logger
}

// SampleInterval is the default sampling period. It is short enough to see a
// queue fill and long enough that reading the heap statistics — which stops the
// world briefly — is not itself the thing being measured.
const SampleInterval = 100 * time.Millisecond

// minSleep is the shortest wait a paced run will actually take. Below it the
// timer costs more than the delay it is trying to produce, so the pacing is
// done in small bursts whose average is exact rather than in sleeps whose
// overshoot would make every requested rate a slower one.
const minSleep = 200 * time.Microsecond

// Result is one run's measurement. Every field is counted or sampled; nothing
// here is derived from an assumption about the machine.
type Result struct {
	Policy capture.Policy
	Format capture.Format

	// Offered is how many frames the feed handed to the sink, dropped or not.
	Offered int64

	// Elapsed is the wall-clock time the run took.
	Elapsed time.Duration

	// Summary is what the capture session reported.
	Summary capture.Summary

	// Samples is one snapshot per Interval, from the same metrics.Collector the
	// production build publishes to CloudWatch.
	Samples []metrics.Snapshot

	// BaseHeap and PeakHeap bound the live heap across the run. The difference
	// is what the policy cost in memory, which is the whole question for the
	// buffer policy and close to zero for the other two.
	BaseHeap, PeakHeap uint64
}

// OfferedRate is how fast the feed pushed frames, in messages per second.
func (r Result) OfferedRate() float64 { return perSecond(r.Offered, r.Elapsed) }

// WrittenRate is how fast the writer got them onto disk. Under a policy that
// discards nothing it is the same number as OfferedRate, because the feed can
// only go as fast as the sink accepts.
func (r Result) WrittenRate() float64 { return perSecond(r.Summary.Messages, r.Elapsed) }

// ByteRate is the raw frame bytes written per second.
func (r Result) ByteRate() float64 { return perSecond(r.Summary.Bytes, r.Elapsed) }

// LossFraction is the share of offered frames the policy discarded.
func (r Result) LossFraction() float64 {
	if r.Offered == 0 {
		return 0
	}
	return float64(r.Summary.Dropped) / float64(r.Offered)
}

// HeapGrowth is how much live heap the run added over its baseline.
func (r Result) HeapGrowth() int64 { return int64(r.PeakHeap) - int64(r.BaseHeap) }

// PeakDepth is the deepest the queue got in any sampled interval. It comes from
// the sampled snapshots rather than from the summary, so it is the same number
// a CloudWatch graph of the same run would show.
func (r Result) PeakDepth() int {
	peak := 0
	for _, s := range r.Samples {
		if s.QueueObserved && s.MaxQueueDepth > peak {
			peak = s.MaxQueueDepth
		}
	}
	return peak
}

func perSecond(n int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

// Run pushes the load through the capture path once and returns what happened.
func Run(ctx context.Context, l *Load, cfg Config) (Result, error) {
	if cfg.Repeat <= 0 {
		cfg.Repeat = 1
	}
	if cfg.Interval <= 0 {
		cfg.Interval = SampleInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	f := &loadFeed{load: l, repeat: cfg.Repeat, speed: cfg.Speed}
	col := metrics.NewCollector()

	// The heap baseline is taken after a collection, so that what the run is
	// measured against is live memory and not whatever the loader left behind.
	runtime.GC()
	res := Result{Policy: cfg.Policy, Format: cfg.Format, BaseHeap: liveHeap()}
	res.PeakHeap = res.BaseHeap

	// The sampler owns everything it writes and hands it back at the end, so
	// nothing here is shared with the goroutine running the capture.
	type sampled struct {
		snaps []metrics.Snapshot
		peak  uint64
	}
	sampling := make(chan struct{})
	done := make(chan sampled)
	go func() {
		s := sampled{peak: res.BaseHeap}
		t := time.NewTicker(cfg.Interval)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-t.C:
				s.snaps = append(s.snaps, col.Collect())
				// Reading the heap statistics briefly stops the world, so it
				// happens a fifth as often as the queue is sampled.
				if i%5 == 0 {
					if h := liveHeap(); h > s.peak {
						s.peak = h
					}
				}
			case <-sampling:
				s.snaps = append(s.snaps, col.Collect())
				if h := liveHeap(); h > s.peak {
					s.peak = h
				}
				done <- s
				return
			}
		}
	}()

	start := time.Now()
	sum, err := capture.Run(ctx, f, capture.Config{
		Root:          cfg.Root,
		Format:        cfg.Format,
		Policy:        cfg.Policy,
		Window:        cfg.Window,
		Buffer:        cfg.Buffer,
		FlushInterval: cfg.FlushInterval,
		Log:           cfg.Log,
		Metrics:       col,
	})
	res.Elapsed = time.Since(start)

	close(sampling)
	s := <-done
	res.Samples, res.PeakHeap = s.snaps, s.peak

	res.Summary = sum
	res.Offered = f.offered
	return res, err
}

func liveHeap() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// loadFeed replays a Load into a Sink. It is a feed.Feed, so capture.Run cannot
// tell it from a socket.
type loadFeed struct {
	load   *Load
	repeat int
	speed  float64

	// offered counts frames handed to the sink. It is written by Run's own
	// goroutine and read after that goroutine has returned.
	offered int64
}

func (f *loadFeed) Name() string { return "bench" }

func (f *loadFeed) Product() string { return f.load.Product }

// SeqMode is monotonic, which is what the Coinbase feed reports. It is also the
// harder case for a policy that drops: on a monotonic feed a skipped sequence
// number proves nothing, so the sequence numbers cannot reveal a dropped frame
// and the drop record is the only thing that can.
func (f *loadFeed) SeqMode() feed.SeqMode { return feed.SeqMonotonic }

func (f *loadFeed) Run(ctx context.Context, out feed.Sink) error {
	frames := f.load.Frames
	first := frames[0].recv

	// Time is shifted forward per pass rather than replayed as recorded: a
	// writer that rotates on receive time cannot be handed the same hour twice,
	// and reopening a closed file is a thing this project refuses to do. Gaps
	// between frames are preserved exactly, so a paced run paces on the real
	// arrival pattern and a batch closes where it would have closed live.
	base := time.Now().UTC()
	start := base

	if !out.Send(ctx, feed.Frame{Kind: feed.KindReseed, Recv: base, Reason: "subscribed"}) {
		return ctx.Err()
	}

	for pass := 0; pass < f.repeat; pass++ {
		offset := base.Sub(first)
		advance := uint64(pass) * f.load.seqSpan
		for i := range frames {
			recv := frames[i].recv.Add(offset)

			if f.speed > 0 {
				elapsed := time.Duration(float64(recv.Sub(start)) / f.speed)
				// Sleeping for less than minSleep costs more than it waits —
				// the timer's own granularity is tens of microseconds — so a
				// small deficit is spent sending instead. The rate is right
				// over any window longer than minSleep, which is every window
				// anyone reads.
				if wait := time.Until(start.Add(elapsed)); wait > minSleep {
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			if !out.Send(ctx, feed.Frame{
				Kind: feed.KindData,
				Raw:  frames[i].render(advance),
				Recv: recv,
			}) {
				return ctx.Err()
			}
			f.offered++
		}
		// One nanosecond so the next pass never reuses a timestamp.
		base = base.Add(f.load.Span + 1)
	}
	return nil
}

// render builds the frame this pass sends: a fresh copy, with the sequence
// number advanced past every pass before it.
//
// The copy is not waste. A socket read allocates the bytes it hands over, so a
// harness whose frames all alias one loaded buffer would let a policy hold a
// million frames for the price of a million pointers — and the memory a policy
// costs is the entire question for one of the three. Advancing the sequence is
// what makes a repeated window a feed that kept going rather than one that went
// backwards into a wall of regressions that never happened.
func (f frame) render(advance uint64) []byte {
	if f.seqEnd == 0 || advance == 0 {
		return bytes.Clone(f.raw)
	}
	out := make([]byte, 0, len(f.raw)+2)
	out = append(out, f.raw[:f.seqAt]...)
	out = strconv.AppendUint(out, f.seq+advance, 10)
	return append(out, f.raw[f.seqEnd:]...)
}
