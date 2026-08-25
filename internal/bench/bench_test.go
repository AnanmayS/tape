package bench

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/feed"
)

// window captures a short synthetic session and returns it, so the harness is
// exercised against a real window written by the real writer.
func window(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	_, err := capture.Run(context.Background(), &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqMonotonic,
		StartSeq:  1000,
		Count:     300,
		Step:      3,
		Now:       stepClock(time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC), time.Millisecond),
	}, capture.Config{Root: root, Window: time.Hour, Log: quiet()})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	return root
}

func stepClock(start time.Time, step time.Duration) func() time.Time {
	t := start.Add(-step)
	return func() time.Time {
		t = t.Add(step)
		return t
	}
}

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestLoadWindowKeepsStoredOrder(t *testing.T) {
	l, err := LoadWindow(window(t), "BTC-USD")
	if err != nil {
		t.Fatalf("LoadWindow: %v", err)
	}
	if len(l.Frames) != 300 {
		t.Fatalf("loaded %d frames, want 300", len(l.Frames))
	}
	// Stored order is arrival order, so the sequence numbers only go up. That
	// is the whole reason the loader does not go through the replay reader,
	// whose order is sorted by exchange time and would regress here.
	for i := 1; i < len(l.Frames); i++ {
		if l.Frames[i].seq <= l.Frames[i-1].seq {
			t.Fatalf("frame %d has sequence %d after %d; the load is not in arrival order",
				i, l.Frames[i].seq, l.Frames[i-1].seq)
		}
		if !l.Frames[i].recv.After(l.Frames[i-1].recv) {
			t.Fatalf("frame %d is stamped before its predecessor", i)
		}
	}
	if l.seqSpan == 0 {
		t.Fatal("no sequence span was measured, so repeated passes would regress")
	}
}

// A repeated window must look like a feed that kept going. If the sequence
// numbers restarted, every pass after the first would be a wall of regression
// gap records and the measurement would be of those, not of the writer.
func TestRepeatedPassesDoNotRegress(t *testing.T) {
	root := window(t)
	l, err := LoadWindow(root, "BTC-USD")
	if err != nil {
		t.Fatalf("LoadWindow: %v", err)
	}

	out := filepath.Join(t.TempDir(), "run")
	res, err := Run(context.Background(), l, Config{
		Root: out, Policy: capture.PolicyBlock, Format: capture.FormatRaw,
		Repeat: 4, Window: time.Hour, Log: quiet(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := int64(len(l.Frames)) * 4; res.Offered != want {
		t.Fatalf("offered %d frames, want %d", res.Offered, want)
	}
	if res.Summary.Messages != res.Offered {
		t.Fatalf("wrote %d of %d offered under the block policy",
			res.Summary.Messages, res.Offered)
	}
	if res.Summary.Gaps != 0 {
		t.Fatalf("%d gap records; repeating the window manufactured discontinuities",
			res.Summary.Gaps)
	}
	if res.Summary.Dropped != 0 {
		t.Fatalf("the block policy dropped %d frames", res.Summary.Dropped)
	}
	if res.Summary.WriteLatency.Count == 0 || res.WrittenRate() <= 0 {
		t.Fatalf("no throughput measured: %+v", res.Summary.WriteLatency)
	}
	if len(res.Samples) == 0 {
		t.Fatal("no samples collected")
	}
	if err := os.RemoveAll(out); err != nil {
		t.Fatal(err)
	}
}

// Every policy has to come out of the harness with its accounting intact:
// offered equals written plus dropped, and every dropped frame is inside a gap
// record. This is the same invariant capture's own tests assert, checked here
// against the load the decision was actually made on.
func TestHarnessAccountsForEveryFrame(t *testing.T) {
	root := window(t)
	l, err := LoadWindow(root, "BTC-USD")
	if err != nil {
		t.Fatalf("LoadWindow: %v", err)
	}
	for _, pol := range capture.Policies {
		t.Run(string(pol), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "run")
			res, err := Run(context.Background(), l, Config{
				Root: out, Policy: pol, Format: capture.FormatColumnar,
				Repeat: 3, Buffer: 4, Window: time.Hour, Log: quiet(),
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Summary.Messages+res.Summary.Dropped != res.Offered {
				t.Fatalf("%d written plus %d dropped is not the %d offered",
					res.Summary.Messages, res.Summary.Dropped, res.Offered)
			}
			if pol != capture.PolicyDrop && res.Summary.Dropped != 0 {
				t.Fatalf("%s dropped %d frames", pol, res.Summary.Dropped)
			}
			if res.Summary.Dropped > 0 && res.Summary.Gaps == 0 {
				t.Fatal("frames were dropped and no gap record says so")
			}
		})
	}
}

func TestFindSequence(t *testing.T) {
	raw := []byte(`{"type":"match","product_id":"BTC-USD","sequence":134914397266,"size":"0.1"}`)
	at, end, seq := findSequence(raw)
	if seq != 134914397266 {
		t.Fatalf("seq = %d", seq)
	}
	if string(raw[at:end]) != "134914397266" {
		t.Fatalf("digits are %q", raw[at:end])
	}

	f := frame{raw: raw, seqAt: at, seqEnd: end, seq: seq}
	got := f.render(1000)
	if string(got) != `{"type":"match","product_id":"BTC-USD","sequence":134914398266,"size":"0.1"}` {
		t.Fatalf("render gave %s", got)
	}
	if &got[0] == &raw[0] {
		t.Fatal("render aliased the loaded frame; a queued frame must own its bytes")
	}
	if unchanged := f.render(0); string(unchanged) != string(raw) {
		t.Fatalf("pass zero changed the frame: %s", unchanged)
	}

	if _, end, _ := findSequence([]byte(`{"type":"l2update"}`)); end != 0 {
		t.Fatal("a frame with no sequence reported one")
	}
}
