package replay

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// base is the arbitrary but fixed instant the hand-built windows below hang
// off. Nothing in this package reads a clock, so a fixed base is enough to make
// every one of these tests independent of when it runs.
var base = time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC)

// fileWindow is the rotation window the hand-built tests use. Records are given
// receive times that place them in a chosen file, so a test can say exactly
// which file each record lands in.
const fileWindow = 10 * time.Second

// buildWindow writes a window with the given rotation and returns its root.
func buildWindow(t *testing.T, write func(w *tapefile.Writer)) string {
	t.Helper()
	root := t.TempDir()
	w, err := tapefile.NewWriter(root, "TEST", fileWindow)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	write(w)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return root
}

// matchFrame is a Coinbase match: a channel, an exchange timestamp and a
// sequence number.
func matchFrame(seq uint64, exch time.Time) []byte {
	return fmt.Appendf(nil,
		`{"type":"match","trade_id":1,"side":"buy","size":"0.001","price":"80000.00",`+
			`"product_id":"BTC-USD","sequence":%d,"time":%q}`,
		seq, exch.UTC().Format("2006-01-02T15:04:05.000000Z"))
}

// l2updateFrame is a Coinbase level2_batch update: an exchange timestamp and no
// sequence number at all. This is the shape the "no sequence" rule exists for.
func l2updateFrame(exch time.Time) []byte {
	return fmt.Appendf(nil,
		`{"type":"l2update","product_id":"BTC-USD","changes":[["buy","80000.00","0.5"]],"time":%q}`,
		exch.UTC().Format("2006-01-02T15:04:05.000000Z"))
}

// subscriptionsFrame is a control frame: no exchange timestamp and no sequence.
// It is the shape the pinning rule exists for.
func subscriptionsFrame() []byte {
	return []byte(`{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}`)
}

// replayAll drains a window and returns everything it delivered.
func replayAll(t *testing.T, root string, opts ...Option) []Record {
	t.Helper()
	r, err := Open(root, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	var out []Record
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("Next after %d records: %v", len(out), err)
		}
		out = append(out, rec)
	}
}

// summarize renders a replay compactly for a failure message.
func summarize(recs []Record) string {
	var b strings.Builder
	for _, r := range recs {
		switch r.Kind {
		case KindMessage:
			fmt.Fprintf(&b, "%s(%s)", r.Event.Type, timeText(r.Event.ExchangeTime))
		default:
			fmt.Fprintf(&b, "%s", r.Kind)
		}
		b.WriteByte(' ')
	}
	return strings.TrimSpace(b.String())
}

func TestSortsByExchangeTimeNotArrivalOrder(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		// Stored in the order they came off the socket, which is not the order
		// the exchange stamped them in.
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(3, base.Add(3*time.Second))}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base.Add(1*time.Second))}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(2, base.Add(2*time.Second))}))
	})

	recs := replayAll(t, root)
	var got []uint64
	for _, r := range recs {
		got = append(got, r.Event.Sequence)
	}
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("replayed %v, want [1 2 3] — got %s", got, summarize(recs))
	}
}

func TestUnsequencedSortsBeforeSequencedAtTheSameInstant(t *testing.T) {
	at := base.Add(time.Second)
	root := buildWindow(t, func(w *tapefile.Writer) {
		// The sequenced record is stored first; the rule, not arrival, decides.
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(9, at)}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: l2updateFrame(at)}))
	})

	recs := replayAll(t, root)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Event.Type != "l2update" || recs[1].Event.Type != "match" {
		t.Errorf("got %s, want the unsequenced l2update first", summarize(recs))
	}
	if recs[0].Event.HasSequence {
		t.Error("an l2update reported a sequence number")
	}
}

func TestRecordsWithNoExchangeTimeArePinnedToTheirPredecessor(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteReseed(tapefile.Reseed{At: base, Reason: "subscribed"}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: subscriptionsFrame()}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base.Add(time.Second))}))
		mustWrite(t, w.WriteReseed(tapefile.Reseed{At: base, Reason: "reconnect: read timeout"}))
		mustWrite(t, w.WriteGap(tapefile.Gap{At: base, Expected: 2, Got: 40}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(40, base.Add(2*time.Second))}))
	})

	recs := replayAll(t, root, WithContinueOnGap())
	want := []Kind{KindReseed, KindMessage, KindMessage, KindReseed, KindGap, KindMessage}
	if len(recs) != len(want) {
		t.Fatalf("got %d records, want %d: %s", len(recs), len(want), summarize(recs))
	}
	for i, k := range want {
		if recs[i].Kind != k {
			t.Fatalf("record %d is %s, want %s — the reconnect must land where it happened, "+
				"not at the front of the window: %s", i, recs[i].Kind, k, summarize(recs))
		}
	}
	if !recs[0].Opening {
		t.Error("the first reseed is the subscription opening the window and should be marked so")
	}
	if recs[3].Opening {
		t.Error("a reseed after messages have been delivered is a discontinuity, not an opening")
	}
}

func TestPinningCrossesFileBoundaries(t *testing.T) {
	// File one ends with a timestamped match. File two opens with a reconnect,
	// which carries no exchange timestamp of its own. If the pinning anchor
	// were reset per file, that reseed would sort to the head of the window and
	// claim the book was rebuilt before anything happened.
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base.Add(time.Second))}))
		second := base.Add(fileWindow)
		mustWrite(t, w.WriteReseed(tapefile.Reseed{At: second, Reason: "reconnect: connection reset by peer"}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: second, Raw: matchFrame(9, base.Add(2*time.Second))}))
	})

	r, err := Open(root, WithContinueOnGap())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if got := len(r.Files()); got != 2 {
		t.Fatalf("window has %d files, want 2", got)
	}
	r.Close()

	recs := replayAll(t, root, WithContinueOnGap())
	want := []Kind{KindMessage, KindReseed, KindMessage}
	for i, k := range want {
		if i >= len(recs) || recs[i].Kind != k {
			t.Fatalf("got %s, want message reseed message", summarize(recs))
		}
	}
	if recs[1].Opening {
		t.Error("the reconnect reseed was mistaken for the window's opening subscription")
	}
}

func TestMultiFileWindowsInterleaveByExchangeTime(t *testing.T) {
	// Rotation splits on receive time; the exchange stamps are what ordering
	// follows, and the two do not have to agree.
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base.Add(1*time.Second))}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(3, base.Add(3*time.Second))}))
		second := base.Add(fileWindow)
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: second, Raw: matchFrame(2, base.Add(2*time.Second))}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: second, Raw: matchFrame(4, base.Add(4*time.Second))}))
	})

	recs := replayAll(t, root)
	var got []uint64
	for _, r := range recs {
		got = append(got, r.Event.Sequence)
	}
	if fmt.Sprint(got) != "[1 2 3 4]" {
		t.Errorf("replayed %v, want [1 2 3 4]", got)
	}
	// Files are still identified individually in the output.
	if recs[0].Position.File == recs[1].Position.File {
		t.Errorf("records 0 and 1 came from the same file %q; the interleave did not cross files",
			recs[0].Position.File)
	}
}

func TestFilesAreReadInSortedOrder(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		for i := range 3 {
			at := base.Add(time.Duration(i) * fileWindow)
			mustWrite(t, w.WriteMessage(tapefile.Message{Recv: at, Raw: matchFrame(uint64(i+1), at)}))
		}
	})

	r, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	files := r.Files()
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3", len(files))
	}
	for i := 1; i < len(files); i++ {
		if files[i-1] >= files[i] {
			t.Errorf("files are not in sorted order: %v", files)
		}
	}
	// Capture's fixed-width UTC names are what makes sorted order chronological.
	if !strings.HasSuffix(files[0], "20260825T040000Z.tape") {
		t.Errorf("unexpected first file %q", files[0])
	}
}

func TestSingleFileWindow(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base)}))
	})
	path := filepath.Join(root, filepath.FromSlash(storage.Key("TEST", base)))

	recs := replayAll(t, path)
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if recs[0].Position.File != "20260825T040000Z.tape" {
		t.Errorf("file is %q, want it relative to the window root", recs[0].Position.File)
	}
}

func TestReorderWindowTooSmallFailsLoudly(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(2, base.Add(2*time.Second))}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base.Add(1*time.Second))}))
	})

	r, err := Open(root, WithReorderWindow(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if _, err := r.Next(); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	_, err = r.Next()
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("second Next returned %v, want ErrOutOfOrder — a reorder buffer too small "+
			"to sort a window must fail rather than emit it misordered", err)
	}

	var oo *OutOfOrderError
	if !errors.As(err, &oo) || oo.Window != 1 {
		t.Errorf("error %v does not report the reorder window it exceeded", err)
	}
	// A window large enough sorts the same data without complaint.
	if got := len(replayAll(t, root, WithReorderWindow(8))); got != 2 {
		t.Errorf("with a big enough window, got %d records, want 2", got)
	}
}

func TestUnparseableFrameIsDeliveredNotDropped(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: matchFrame(1, base)}))
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: []byte("not json at all")}))
	})

	recs := replayAll(t, root)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2 — an unparseable frame must not vanish", len(recs))
	}
	bad := recs[1]
	if bad.DecodeError == "" {
		t.Error("the unparseable frame was delivered with no complaint attached")
	}

	line := encodeOne(t, bad)
	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		t.Fatalf("canonical output for an unparseable frame is not valid JSON: %v", err)
	}
	if _, ok := obj["raw_b64"]; !ok {
		t.Errorf("unparseable frame has no raw_b64 field: %s", line)
	}
	if _, ok := obj["raw"]; ok {
		t.Errorf("unparseable frame emitted a raw field as well: %s", line)
	}
}

func TestCanonicalSequenceIsNullNotZero(t *testing.T) {
	root := buildWindow(t, func(w *tapefile.Writer) {
		mustWrite(t, w.WriteMessage(tapefile.Message{Recv: base, Raw: l2updateFrame(base)}))
	})
	recs := replayAll(t, root)
	line := string(encodeOne(t, recs[0]))
	if !strings.Contains(line, `"sequence":null`) {
		t.Errorf("a record with no sequence serialized as %s; absence must not become 0", line)
	}
}

func TestEmptyWindowIsAnError(t *testing.T) {
	if _, err := Open(t.TempDir()); err == nil {
		t.Error("opening a directory with no tape files should fail rather than replay nothing")
	}
}

func encodeOne(t *testing.T, rec Record) []byte {
	t.Helper()
	var b strings.Builder
	if err := NewCanonicalEncoder(&b).Encode(rec); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return []byte(strings.TrimSpace(b.String()))
}

func mustWrite(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestCompareKeysFieldPrecedence pins the comparison order itself. Each case
// differs from the reference key in exactly one field, and the expected result
// is the one the documented precedence gives.
func TestCompareKeysFieldPrecedence(t *testing.T) {
	ref := orderKey{exchange: 100, seqRank: 1, sequence: 50, channel: "matches", file: 2, record: 7}

	cases := []struct {
		name string
		k    orderKey
		want int
	}{
		{"earlier exchange time wins over everything",
			orderKey{exchange: 99, seqRank: 1, sequence: 9999, channel: "zzz", file: 9, record: 99}, -1},
		{"no sequence sorts before a sequence",
			orderKey{exchange: 100, seqRank: 0, sequence: 9999, channel: "zzz", file: 9, record: 99}, -1},
		{"lower sequence sorts first",
			orderKey{exchange: 100, seqRank: 1, sequence: 49, channel: "zzz", file: 9, record: 99}, -1},
		{"channel breaks a sequence tie byte-wise",
			orderKey{exchange: 100, seqRank: 1, sequence: 50, channel: "level2_batch", file: 9, record: 99}, -1},
		{"earlier file breaks a channel tie",
			orderKey{exchange: 100, seqRank: 1, sequence: 50, channel: "matches", file: 1, record: 99}, -1},
		{"record ordinal is the last word",
			orderKey{exchange: 100, seqRank: 1, sequence: 50, channel: "matches", file: 2, record: 6}, -1},
		{"identical keys compare equal", ref, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compareKeys(c.k, ref); got != c.want {
				t.Errorf("compareKeys(k, ref) = %d, want %d", got, c.want)
			}
			if got := compareKeys(ref, c.k); got != -c.want {
				t.Errorf("comparison is not antisymmetric: reversed gives %d, want %d", got, -c.want)
			}
		})
	}
}

// TestCompareKeysHandlesExtremeTimestamps guards the subtraction in the
// timestamp comparison: two timestamps far enough apart overflow an int64 if
// they are subtracted, and the sign of the overflowed result is wrong.
func TestCompareKeysHandlesExtremeTimestamps(t *testing.T) {
	lo := orderKey{exchange: -(1 << 62)}
	hi := orderKey{exchange: 1 << 62}
	if compareKeys(lo, hi) != -1 {
		t.Errorf("compareKeys(lo, hi) = %d, want -1", compareKeys(lo, hi))
	}
	if compareKeys(hi, lo) != 1 {
		t.Errorf("compareKeys(hi, lo) = %d, want 1", compareKeys(hi, lo))
	}
}
