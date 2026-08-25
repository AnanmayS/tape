package replay

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// The golden fixture under testdata/window is a real Coinbase BTC-USD capture.
// Every frame in it came off wss://ws-feed.exchange.coinbase.com verbatim; the
// timestamps are the ones capture recorded, and the gap and reseed records were
// produced by the production capture path reacting to the real sequence
// numbers, not written by hand.
//
// It is stored gzipped, one .tape.gz per file, and materialized into a temp
// directory by fixtureWindow. Real Coinbase frames average around 660 bytes —
// level2_batch updates for BTC-USD run past a kilobyte — so a few thousand of
// them is a couple of megabytes raw and a couple of hundred kilobytes gzipped.
// Storing them compressed is what makes "a few thousand real events" and "small
// enough to commit" both true. Nothing about the replay changes: the test reads
// the same bytes capture wrote.
//
// The fixture keeps the directory names it was captured under, which are no
// longer the ones capture writes: M4 moved local files onto the storage key
// layout. That is deliberate. Replay names files relative to the window root
// and knows nothing about the partitioning above it, so the fixture is a
// perfectly good window either way — and leaving the names alone is what lets
// goldenDigest carry across the storage move unchanged, which is the whole
// evidence that the move changed nothing. Regenerating the fixture would refile
// it under the new layout and move the digest; that is a decision, not a fix.
//
// The level2 snapshot frame is the one thing left out. A single BTC-USD
// snapshot is 1.1 MB, larger than the rest of the fixture put together, and it
// exercises nothing that the subscriptions frame does not: both are records
// with no exchange timestamp and no sequence, which is the ordering case that
// matters.
//
// To rebuild it:
//
//	go run ./cmd/tape capture -dir /tmp/src -duration 200s -window 1m
//	go test ./internal/replay -run TestGenerateFixture -fixture.src /tmp/src
//
// The generator rotates every 30s rather than every minute, so the fixture
// spans three files and exercises multi-file windows without having to carry
// three minutes of BTC-USD.

var (
	fixtureSrc = flag.String("fixture.src", "",
		"rebuild testdata/window from this captured window; empty skips the generator")
	fixtureEvents = flag.Int("fixture.events", 2400,
		"how many real frames to put in the fixture")
	fixtureSeverAt = flag.Int("fixture.sever-at", 1200,
		"sever the connection after this many frames")
	fixtureDrop = flag.Int("fixture.drop", 60,
		"how many frames the severed connection loses")
	fixtureWindowSize = flag.Duration("fixture.window", 30*time.Second,
		"rotation window; short so the fixture spans several files without being large")
)

const fixtureDir = "testdata/window"

// fixtureWindow materializes the golden fixture into a temp directory and
// returns its root.
func fixtureWindow(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	var n int
	err := filepath.WalkDir(fixtureDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".tape.gz") {
			return err
		}
		rel, err := filepath.Rel(fixtureDir, p)
		if err != nil {
			return err
		}
		out := filepath.Join(root, strings.TrimSuffix(rel, ".gz"))
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		if err := gunzipTo(p, out); err != nil {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("materialize fixture: %v", err)
	}
	if n == 0 {
		t.Fatalf("no %s/**/*.tape.gz found; regenerate with -fixture.src", fixtureDir)
	}
	return root
}

func gunzipTo(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, zr); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// TestGenerateFixture rebuilds testdata/window. It is skipped without
// -fixture.src, and it is the provenance of the fixture: the frames are read
// back out of a real capture and pushed through capture.Run, so the tape files
// it writes are written by the same code that writes a live session, and the
// gap record in them is produced by the real sequence tracker reacting to the
// real sequence numbers of a real reconnect.
func TestGenerateFixture(t *testing.T) {
	if *fixtureSrc == "" {
		t.Skip("set -fixture.src to a captured window to rebuild the fixture")
	}

	frames, err := realFrames(*fixtureSrc, *fixtureEvents)
	if err != nil {
		t.Fatalf("read source window: %v", err)
	}
	t.Logf("read %d real frames from %s", len(frames), *fixtureSrc)

	staging := t.TempDir()
	sum, err := capture.Run(context.Background(), &recordedFeed{
		frames:  frames,
		severAt: *fixtureSeverAt,
		drop:    *fixtureDrop,
	}, capture.Config{
		Root:          staging,
		Window:        *fixtureWindowSize,
		Buffer:        1024,
		FlushInterval: time.Hour, // Close flushes; no ticker needed here.
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	t.Logf("captured: messages=%d records=%d bytes=%d files=%d reseeds=%d gaps=%d",
		sum.Messages, sum.Records, sum.Bytes, len(sum.Files), sum.Reseeds, sum.Gaps)

	if sum.Gaps == 0 {
		t.Fatalf("fixture must contain a gap record; the sever produced none")
	}
	if sum.Reseeds < 2 {
		t.Fatalf("fixture must contain an opening reseed and a reconnect reseed, got %d", sum.Reseeds)
	}
	if len(sum.Files) < 3 {
		t.Fatalf("fixture must span several files to exercise multi-file windows, got %d", len(sum.Files))
	}

	if err := os.RemoveAll(fixtureDir); err != nil {
		t.Fatalf("clear fixture dir: %v", err)
	}
	var total int64
	for _, p := range sum.Files {
		rel, err := filepath.Rel(staging, p)
		if err != nil {
			t.Fatalf("rel: %v", err)
		}
		out := filepath.Join(fixtureDir, rel+".gz")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		n, err := gzipTo(p, out)
		if err != nil {
			t.Fatalf("gzip %s: %v", p, err)
		}
		total += n
		t.Logf("wrote %s (%d bytes gzipped)", out, n)
	}
	t.Logf("fixture total %d bytes gzipped from %d bytes stored", total, sum.Bytes)
}

func gzipTo(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	zw, err := gzip.NewWriterLevel(out, gzip.BestCompression)
	if err != nil {
		out.Close()
		return 0, err
	}
	if _, err := io.Copy(zw, in); err != nil {
		out.Close()
		return 0, err
	}
	if err := zw.Close(); err != nil {
		out.Close()
		return 0, err
	}
	if err := out.Close(); err != nil {
		return 0, err
	}
	st, err := os.Stat(dst)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// realFrames reads up to n message records back out of a captured window,
// keeping the raw bytes and receive times exactly as stored. Snapshot frames
// are skipped; see the note at the top of this file.
func realFrames(root string, n int) ([]feed.Frame, error) {
	store := storage.NewLocal(root)
	files, err := windowFiles(context.Background(), store, "")
	if err != nil {
		return nil, err
	}
	var out []feed.Frame
	for _, rel := range files {
		rd, err := tapefile.Open(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return nil, err
		}
		for len(out) < n {
			typ, payload, err := rd.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				rd.Close()
				return nil, err
			}
			if typ != tapefile.RecordMessage {
				continue
			}
			m, err := tapefile.DecodeMessage(payload)
			if err != nil {
				rd.Close()
				return nil, err
			}
			if isSnapshot(m.Raw) {
				continue
			}
			out = append(out, feed.Frame{Kind: feed.KindData, Raw: m.Raw, Recv: m.Recv})
		}
		if err := rd.Close(); err != nil {
			return nil, err
		}
		if len(out) >= n {
			break
		}
	}
	return out, nil
}

func isSnapshot(raw []byte) bool {
	var w struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return false
	}
	return w.Type == "snapshot"
}

// recordedFeed pushes previously captured frames back through the capture path,
// severing the connection once so that the reconnect, the lost messages and the
// gap the sequence numbers reveal are all produced by production code.
type recordedFeed struct {
	frames  []feed.Frame
	severAt int
	drop    int
}

func (f *recordedFeed) Name() string          { return "recorded" }
func (f *recordedFeed) Product() string       { return feed.CoinbaseProduct }
func (f *recordedFeed) SeqMode() feed.SeqMode { return feed.SeqMonotonic }

func (f *recordedFeed) Run(ctx context.Context, out chan<- feed.Frame) error {
	send := func(fr feed.Frame) bool {
		select {
		case out <- fr:
			return true
		case <-ctx.Done():
			return false
		}
	}

	if len(f.frames) == 0 {
		return nil
	}
	if !send(feed.Frame{Kind: feed.KindReseed, Recv: f.frames[0].Recv, Reason: "subscribed"}) {
		return ctx.Err()
	}
	for i := 0; i < len(f.frames); i++ {
		if i == f.severAt && f.drop > 0 {
			// The connection dies here: the next f.drop frames are the ones the
			// exchange sent while nobody was listening, and the resubscribe
			// lands after them.
			lost := min(f.drop, len(f.frames)-i)
			i += lost
			if i >= len(f.frames) {
				break
			}
			if !send(feed.Frame{
				Kind:   feed.KindReseed,
				Recv:   f.frames[i].Recv,
				Reason: "reconnect: connection reset by peer",
			}) {
				return ctx.Err()
			}
		}
		if !send(f.frames[i]) {
			return ctx.Err()
		}
	}
	return nil
}
