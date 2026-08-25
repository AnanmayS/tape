package capture

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/metrics"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// quietLogger keeps test output readable while still exercising the log calls.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// stepClock returns a clock that advances by step on every call, so a test can
// drive file rotation without waiting on wall-clock time.
func stepClock(start time.Time, step time.Duration) func() time.Time {
	t := start.Add(-step)
	return func() time.Time {
		t = t.Add(step)
		return t
	}
}

// counts tallies every record on disk under root.
type counts struct {
	messages, gaps, reseeds int
	files                   []string
	gapRecords              []tapefile.Gap
}

func readAll(t *testing.T, files []string) counts {
	t.Helper()
	var c counts
	for _, p := range files {
		r, err := tapefile.Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		c.files = append(c.files, p)
		for {
			typ, payload, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.Close()
				t.Fatalf("read %s: %v", p, err)
			}
			switch typ {
			case tapefile.RecordMessage:
				if _, err := tapefile.DecodeMessage(payload); err != nil {
					t.Fatalf("decode message in %s: %v", p, err)
				}
				c.messages++
			case tapefile.RecordGap:
				g, err := tapefile.DecodeGap(payload)
				if err != nil {
					t.Fatalf("decode gap in %s: %v", p, err)
				}
				c.gapRecords = append(c.gapRecords, g)
				c.gaps++
			case tapefile.RecordReseed:
				if _, err := tapefile.DecodeReseed(payload); err != nil {
					t.Fatalf("decode reseed in %s: %v", p, err)
				}
				c.reseeds++
			default:
				t.Fatalf("unknown record type %v in %s", typ, p)
			}
		}
		r.Close()
	}
	return c
}

// recordOrder returns the record types across all files in order, so a test can
// assert where a gap record sits relative to the messages around it.
func recordOrder(t *testing.T, files []string) []string {
	t.Helper()
	var order []string
	for _, p := range files {
		r, err := tapefile.Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		for {
			typ, _, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.Close()
				t.Fatalf("read %s: %v", p, err)
			}
			order = append(order, typ.String())
		}
		r.Close()
	}
	return order
}

func runCapture(t *testing.T, s *feed.Synthetic, cfg Config) (Summary, counts) {
	t.Helper()
	if cfg.Root == "" {
		cfg.Root = t.TempDir()
	}
	if cfg.Log == nil {
		cfg.Log = quietLogger()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sum, err := Run(ctx, s, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return sum, readAll(t, sum.Files)
}

// M1's acceptance condition: the logged message count must equal the number of
// message frames actually on disk, across rotations.
func TestCaptureCountsMatchDisk(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     500,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Minute, FlushInterval: time.Hour})

	if sum.Messages != 500 {
		t.Fatalf("summary messages = %d, want 500", sum.Messages)
	}
	if got.messages != int(sum.Messages) {
		t.Fatalf("on disk %d message records, summary says %d", got.messages, sum.Messages)
	}
	if got.reseeds != int(sum.Reseeds) || sum.Reseeds != 1 {
		t.Fatalf("reseeds: disk=%d summary=%d, want 1", got.reseeds, sum.Reseeds)
	}
	total := int64(got.messages + got.gaps + got.reseeds)
	if total != sum.Records {
		t.Fatalf("on disk %d records, summary says %d", total, sum.Records)
	}
	if sum.DecodeErrors != 0 {
		t.Fatalf("decode errors = %d, want 0", sum.DecodeErrors)
	}
}

// A capture that spans several rotation windows must leave one file per window
// and nothing else.
func TestCaptureRotatesFiles(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		// The initial reseed takes the 04:00:00 tick, so the data frames run
		// 04:00:01 through 04:02:59: three one-minute windows exactly.
		Count: 179,
		Now:   stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
	sum, got := runCapture(t, s, Config{Window: time.Minute, FlushInterval: time.Hour})

	if len(sum.Files) != 3 {
		t.Fatalf("files = %v, want 3", sum.Files)
	}
	if sum.Rotations != 2 {
		t.Fatalf("rotations = %d, want 2", sum.Rotations)
	}
	if got.messages != 179 {
		t.Fatalf("messages on disk = %d, want 179", got.messages)
	}
	for _, p := range sum.Files {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if fi.Size() <= tapefile.HeaderSize {
			t.Fatalf("%s has only a header", p)
		}
	}
}

// Every path must be distinct: a file opened twice is a file that could have
// been rewritten.
func TestCaptureNeverReopensAFile(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     300,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
	sum, _ := runCapture(t, s, Config{Window: 30 * time.Second, FlushInterval: time.Hour})

	seen := map[string]bool{}
	for _, p := range sum.Files {
		if seen[p] {
			t.Fatalf("file %s opened more than once", p)
		}
		seen[p] = true
	}
	if len(sum.Files) < 2 {
		t.Fatalf("expected several files, got %v", sum.Files)
	}
}

// Shutdown is by context cancellation, and it must still flush and close.
func TestCaptureStopsCleanlyOnCancel(t *testing.T) {
	s := &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     100000,
		Delay:     time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	sum, err := Run(ctx, s, Config{Root: t.TempDir(), Window: time.Minute, Log: quietLogger()})
	if err != nil {
		t.Fatalf("a cancelled capture should not be an error, got %v", err)
	}
	if sum.Messages == 0 {
		t.Fatal("expected some messages before cancellation")
	}
	got := readAll(t, sum.Files)
	if got.messages != int(sum.Messages) {
		t.Fatalf("on disk %d messages, summary says %d; shutdown lost data",
			got.messages, sum.Messages)
	}
}

// An unparseable frame must still be stored. Losing it would be exactly the
// silent data loss this project exists to avoid.
func TestUndecodableFramesAreStillWritten(t *testing.T) {
	root := t.TempDir()
	w, err := tapefile.NewWriter(root, "BTC-USD", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sum := Summary{}
	s := &sink{w: w, seq: newSeqTracker(feed.SeqContiguous), log: quietLogger(), sum: &sum, met: metrics.Nop{}}
	fr := feed.Frame{Kind: feed.KindData, Raw: []byte(`{"type":`), Recv: time.Now().UTC()}
	if err := s.handle(fr); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sum.DecodeErrors != 1 {
		t.Fatalf("decode errors = %d, want 1", sum.DecodeErrors)
	}
	got := readAll(t, w.Stats().Files)
	if got.messages != 1 {
		t.Fatalf("messages on disk = %d, want 1", got.messages)
	}
}

func TestSummaryRate(t *testing.T) {
	s := Summary{
		Started:  time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC),
		Ended:    time.Date(2026, 8, 25, 4, 0, 10, 0, time.UTC),
		Messages: 500,
	}
	if got := s.MessagesPerSecond(); got != 50 {
		t.Fatalf("MessagesPerSecond = %v, want 50", got)
	}
	if got := (Summary{}).MessagesPerSecond(); got != 0 {
		t.Fatalf("zero-duration rate = %v, want 0", got)
	}
}
