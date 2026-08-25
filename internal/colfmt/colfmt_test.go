package colfmt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Frames captured from a live wss://ws-feed.exchange.coinbase.com session,
// product BTC-USD, channels level2_batch + matches.
const (
	matchFrame = `{"type":"match","trade_id":1081862675,"maker_order_id":"f07e510f-4edf-469d-b844-4f670f1e93f1","taker_order_id":"e6f50944-3e2a-439e-ae49-308300ba8296","side":"sell","size":"0.00000001","price":"80691.53","product_id":"BTC-USD","sequence":134905860111,"time":"2026-08-25T03:35:57.198335Z"}`

	l2updateFrame = `{"type":"l2update","product_id":"BTC-USD","changes":[["buy","80688.08","0.59153273"],["sell","80745.16","0.20558981"]],"time":"2026-08-25T03:35:57.300000Z"}`

	subscriptionsFrame = `{"type":"subscriptions","channels":[{"name":"level2_batch","product_ids":["BTC-USD"],"account_ids":null}]}`
)

// record is a stored record as either format hands it back.
type record struct {
	typ     tapefile.RecordType
	payload []byte
}

// drain reads a whole record stream.
func drain(t *testing.T, r tapefile.Records) []record {
	t.Helper()
	var out []record
	for {
		typ, p, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, record{typ: typ, payload: p})
	}
}

// writeBoth writes the same records through both formats and returns the two
// windows' roots. The point of every test that uses it is that what comes back
// out cannot tell them apart.
func writeBoth(t *testing.T, window time.Duration, write func(w recordWriter)) (v1root, v2root string) {
	t.Helper()
	v1root, v2root = t.TempDir(), t.TempDir()

	raw, err := tapefile.NewWriter(v1root, "BTC-USD", window)
	if err != nil {
		t.Fatalf("tapefile.NewWriter: %v", err)
	}
	write(raw)
	if err := raw.Close(); err != nil {
		t.Fatalf("v1 Close: %v", err)
	}

	col, err := NewWriter(v2root, "BTC-USD", window)
	if err != nil {
		t.Fatalf("colfmt.NewWriter: %v", err)
	}
	write(col)
	if err := col.Close(); err != nil {
		t.Fatalf("v2 Close: %v", err)
	}
	return v1root, v2root
}

// recordWriter is the API both writers present. Capture depends on exactly this
// much, which is why the two are interchangeable there.
type recordWriter interface {
	WriteMessage(tapefile.Message) error
	WriteGap(tapefile.Gap) error
	WriteReseed(tapefile.Reseed) error
	Flush() error
	Close() error
	Path() string
	Stats() tapefile.Stats
}

// tapeFiles lists a window's files, sorted, relative to root.
func tapeFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".tape" {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(out) == 0 {
		t.Fatalf("no .tape files under %s", root)
	}
	return out
}

// readWindow reads every file of a window in name order through the dispatcher,
// so the test exercises the same path replay does.
func readWindow(t *testing.T, root string) ([]record, uint16) {
	t.Helper()
	var out []record
	var version uint16
	for _, rel := range tapeFiles(t, root) {
		f, err := os.Open(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		rd, err := OpenRecords(f)
		if err != nil {
			t.Fatalf("OpenRecords %s: %v", rel, err)
		}
		version = rd.Version()
		out = append(out, drain(t, rd)...)
		if err := rd.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	return out, version
}

var base = time.Date(2026, 8, 25, 3, 35, 57, 198335000, time.UTC)

// TestRecordsAreIdenticalToV1 is the claim the whole milestone rests on: a
// columnar file hands back the same records, byte for byte, as the v1 file
// holding the same window. Replay sits on top of exactly this stream, so a
// difference here would be a difference in every replayed byte — and there is
// none to find, because the payloads are compared literally.
func TestRecordsAreIdenticalToV1(t *testing.T) {
	v1root, v2root := writeBoth(t, time.Minute, func(w recordWriter) {
		if err := w.WriteReseed(tapefile.Reseed{At: base, Reason: "subscribed"}); err != nil {
			t.Fatalf("reseed: %v", err)
		}
		for i := 0; i < 500; i++ {
			at := base.Add(time.Duration(i) * 100 * time.Millisecond)
			frame := matchFrame
			if i%3 == 0 {
				frame = l2updateFrame
			}
			if i%97 == 0 {
				frame = subscriptionsFrame
			}
			if err := w.WriteMessage(tapefile.Message{Recv: at, Raw: []byte(frame)}); err != nil {
				t.Fatalf("message %d: %v", i, err)
			}
			if i == 250 {
				if err := w.WriteGap(tapefile.Gap{
					At: at, Expected: 134905860112, Got: 134905861576,
				}); err != nil {
					t.Fatalf("gap: %v", err)
				}
			}
		}
	})

	v1, v1ver := readWindow(t, v1root)
	v2, v2ver := readWindow(t, v2root)
	if v1ver != tapefile.Version || v2ver != Version {
		t.Fatalf("versions: v1 file read as v%d, v2 file read as v%d", v1ver, v2ver)
	}
	if len(v1) != len(v2) {
		t.Fatalf("v1 has %d records, v2 has %d", len(v1), len(v2))
	}
	for i := range v1 {
		if v1[i].typ != v2[i].typ {
			t.Fatalf("record %d: v1 type %s, v2 type %s", i, v1[i].typ, v2[i].typ)
		}
		if !bytes.Equal(v1[i].payload, v2[i].payload) {
			t.Fatalf("record %d (%s): payloads differ\n v1 %q\n v2 %q",
				i, v1[i].typ, v1[i].payload, v2[i].payload)
		}
	}
	if len(v1) != 502 {
		t.Fatalf("wrote 502 records, read %d", len(v1))
	}

	// The two windows have the same file names and different sizes; that is the
	// whole point.
	v1names, v2names := tapeFiles(t, v1root), tapeFiles(t, v2root)
	if strings.Join(v1names, ",") != strings.Join(v2names, ",") {
		t.Errorf("file names differ:\n v1 %v\n v2 %v", v1names, v2names)
	}
	a, b := windowBytes(t, v1root), windowBytes(t, v2root)
	if b >= a {
		t.Errorf("columnar window is %d bytes against v1's %d; it is meant to be smaller", b, a)
	}
	t.Logf("%d records: v1 %d bytes, v2 %d bytes, %.2fx", len(v1), a, b, float64(a)/float64(b))
}

func windowBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	for _, rel := range tapeFiles(t, root) {
		fi, err := os.Stat(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		total += fi.Size()
	}
	return total
}

// TestRotationMatchesV1 checks the batching does not move a record into another
// window. A record belongs to the file its own timestamp names, and a format
// that buffered records across a rotation boundary would quietly refile them.
func TestRotationMatchesV1(t *testing.T) {
	write := func(w recordWriter) {
		for i := 0; i < 6; i++ {
			at := base.Truncate(time.Minute).Add(time.Duration(i) * 30 * time.Second)
			if err := w.WriteMessage(tapefile.Message{Recv: at, Raw: []byte(matchFrame)}); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
	}
	v1root, v2root := writeBoth(t, time.Minute, write)

	v1names, v2names := tapeFiles(t, v1root), tapeFiles(t, v2root)
	if strings.Join(v1names, ",") != strings.Join(v2names, ",") {
		t.Fatalf("rotation differs:\n v1 %v\n v2 %v", v1names, v2names)
	}
	if len(v2names) != 3 {
		t.Fatalf("expected 3 files, got %v", v2names)
	}
	// Every file must hold the records whose timestamps name it.
	for _, rel := range v2names {
		rd, err := Open(filepath.Join(v2root, rel))
		if err != nil {
			t.Fatalf("Open %s: %v", rel, err)
		}
		recs := drain(t, rd)
		rd.Close()
		if len(recs) != 2 {
			t.Errorf("%s holds %d records, want 2", rel, len(recs))
		}
	}
}

// TestIndexColumnsMatchTheFrames is the guard against the failure this format
// is most able to produce: a delta chain that decodes into plausible, wrong
// values. Every index column is checked against the frame stored beside it, so
// a price that drifted by one tick fails here rather than looking like a price.
func TestIndexColumnsMatchTheFrames(t *testing.T) {
	rows := realisticRows(2000, 7)
	batch, err := encodeBatch(rows)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	body, f, err := readBatch(bytes.NewReader(batch))
	if err != nil {
		t.Fatalf("readBatch: %v", err)
	}
	got, err := decodeBatch(body, f)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("decoded %d rows, encoded %d", len(got), len(rows))
	}

	for i, r := range got {
		if r.kind != tapefile.RecordMessage {
			continue
		}
		e, derr := event.Decode(r.raw, r.at)
		if derr != nil {
			continue
		}
		if e.PriceText != r.price {
			t.Fatalf("row %d: column price %q, frame price %q", i, r.price, e.PriceText)
		}
		if e.SizeText != r.size {
			t.Fatalf("row %d: column size %q, frame size %q", i, r.size, e.SizeText)
		}
		if e.Side != r.side {
			t.Fatalf("row %d: column side %q, frame side %q", i, r.side, e.Side)
		}
		if e.Type != r.msgType {
			t.Fatalf("row %d: column type %q, frame type %q", i, r.msgType, e.Type)
		}
		if e.HasSequence != r.hasSequence || e.Sequence != r.sequence {
			t.Fatalf("row %d: column sequence %d/%v, frame %d/%v",
				i, r.sequence, r.hasSequence, e.Sequence, e.HasSequence)
		}
		if got, want := r.exchange, e.ExchangeTime; r.hasExchange && !got.Equal(want) {
			t.Fatalf("row %d: column exchange time %s, frame %s", i, got, want)
		}
	}

	// And the rows themselves survived, field for field.
	for i := range rows {
		if !sameRow(rows[i], got[i]) {
			t.Fatalf("row %d did not round-trip:\n in  %+v\n out %+v", i, rows[i], got[i])
		}
	}
	t.Logf("%d rows, %d bytes encoded, footer rows=%d flags=%d", len(rows), len(batch), f.Rows, f.Flags)
}

func sameRow(a, b row) bool {
	return a.kind == b.kind &&
		a.at.Equal(b.at) &&
		bytes.Equal(a.raw, b.raw) &&
		a.msgType == b.msgType &&
		a.side == b.side &&
		a.price == b.price &&
		a.size == b.size &&
		a.hasSequence == b.hasSequence && a.sequence == b.sequence &&
		a.hasExchange == b.hasExchange && a.exchange.Equal(b.exchange) &&
		a.expected == b.expected && a.got == b.got && a.dropped == b.dropped &&
		a.reason == b.reason
}

// realisticRows builds a batch that looks like a real window: mostly trades and
// book updates, prices that walk, sizes that do not, occasional control frames,
// a reseed and a gap.
func realisticRows(n int, seed uint64) []row {
	rng := rand.New(rand.NewPCG(seed, seed*7+1))
	at := base
	price := 8069153
	seq := uint64(134905860111)
	rows := []row{reseedRow(tapefile.Reseed{At: at, Reason: "subscribed"})}

	for i := 0; len(rows) < n; i++ {
		at = at.Add(time.Duration(rng.IntN(50_000_000)) * time.Nanosecond)
		switch {
		case i%211 == 0:
			rows = append(rows, gapRow(tapefile.Gap{
				At: at, Expected: seq, Got: seq + uint64(rng.IntN(2000)),
			}))
		case i%97 == 0:
			rows = append(rows, messageRow(tapefile.Message{Recv: at, Raw: []byte(subscriptionsFrame)}))
		case i%3 == 0:
			rows = append(rows, messageRow(tapefile.Message{Recv: at, Raw: []byte(l2updateFrame)}))
		default:
			price += rng.IntN(21) - 10
			seq += uint64(rng.IntN(400))
			side := "buy"
			if rng.IntN(2) == 0 {
				side = "sell"
			}
			frame := fmt.Sprintf(
				`{"type":"match","trade_id":%d,"side":%q,"size":"%d.%08d","price":"%d.%02d","product_id":"BTC-USD","sequence":%d,"time":"%s"}`,
				1081862675+i, side, rng.IntN(3), rng.IntN(100000000),
				price/100, price%100, seq, at.Add(-40*time.Millisecond).Format(time.RFC3339Nano))
			rows = append(rows, messageRow(tapefile.Message{Recv: at, Raw: []byte(frame)}))
		}
	}
	return rows
}

// TestUndecodableFrameSurvives: a frame this build cannot parse is still a
// frame, and the format has to hand it back exactly rather than losing it with
// the fields it could not read.
func TestUndecodableFrameSurvives(t *testing.T) {
	garbage := []byte("\x00\x01not json at all\xff")
	rows := []row{
		messageRow(tapefile.Message{Recv: base, Raw: garbage}),
		messageRow(tapefile.Message{Recv: base.Add(time.Second), Raw: []byte(matchFrame)}),
	}
	got := roundTrip(t, rows)
	if !bytes.Equal(got[0].raw, garbage) {
		t.Fatalf("undecodable frame came back as %q", got[0].raw)
	}
	if got[0].price != "" || got[0].msgType != "" {
		t.Errorf("undecodable frame gained index fields: %+v", got[0])
	}
	if got[1].price != "80691.53" {
		t.Errorf("the frame after it decoded as %q", got[1].price)
	}
}

// TestOddDecimalsAreStoredExactly covers the values the scaled-integer path
// refuses. Each of these has to come back character for character, because a
// price the exchange sent is not this format's to normalise.
func TestOddDecimalsAreStoredExactly(t *testing.T) {
	odd := []string{
		"80691.53", "0.00000001", "1", "0", "-1.5", "80691.50", "1.000",
		"0000.5", "1e-8", "+3", "-0.0", ".5", "1.",
		"12345678901234567890.1", "0.1234567890123456789",
	}
	var rows []row
	for i, s := range odd {
		frame := fmt.Sprintf(`{"type":"match","side":"buy","price":%q,"size":%q}`, s, s)
		rows = append(rows, row{
			kind:  tapefile.RecordMessage,
			at:    base.Add(time.Duration(i) * time.Millisecond),
			raw:   []byte(frame),
			price: s,
			size:  s,
		})
	}
	got := roundTrip(t, rows)
	for i, s := range odd {
		if got[i].price != s {
			t.Errorf("price %q came back as %q", s, got[i].price)
		}
		if got[i].size != s {
			t.Errorf("size %q came back as %q", s, got[i].size)
		}
	}
}

// TestUnexpectedSideIsStored: side is a bitset, and a bitset can only say buy
// or sell. Anything else has to take the exception path rather than being
// rounded to the nearer of the two.
func TestUnexpectedSideIsStored(t *testing.T) {
	sides := []string{"buy", "sell", "", "BUY", "unknown", "sell "}
	var rows []row
	for i, s := range sides {
		rows = append(rows, row{
			kind: tapefile.RecordMessage,
			at:   base.Add(time.Duration(i) * time.Millisecond),
			raw:  []byte(matchFrame),
			side: s,
		})
	}
	got := roundTrip(t, rows)
	for i, s := range sides {
		if got[i].side != s {
			t.Errorf("side %q came back as %q", s, got[i].side)
		}
	}
}

func roundTrip(t *testing.T, rows []row) []row {
	t.Helper()
	batch, err := encodeBatch(rows)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	body, f, err := readBatch(bytes.NewReader(batch))
	if err != nil {
		t.Fatalf("readBatch: %v", err)
	}
	got, err := decodeBatch(body, f)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("decoded %d rows, encoded %d", len(got), len(rows))
	}
	return got
}

// TestChecksumCatchesCorruption is why the footer carries a CRC. A delta column
// with one byte changed does not fail to decode; it decodes into different
// numbers. Every byte of the body is flipped in turn and the batch must be
// rejected — by the checksum, or by a decoder that noticed the columns no
// longer line up. What must never happen is a clean decode of different rows.
func TestChecksumCatchesCorruption(t *testing.T) {
	rows := realisticRows(200, 11)
	batch, err := encodeBatch(rows)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	bodyLen := int(binary.LittleEndian.Uint32(batch[:4]))

	var checksumCaught, decoderCaught int
	for i := batchLenSize; i < batchLenSize+bodyLen; i++ {
		corrupt := append([]byte(nil), batch...)
		corrupt[i] ^= 0x40

		body, f, err := readBatch(bytes.NewReader(corrupt))
		if errors.Is(err, ErrChecksum) {
			checksumCaught++
			continue
		}
		if err != nil {
			decoderCaught++
			continue
		}
		got, err := decodeBatch(body, f)
		if err != nil {
			decoderCaught++
			continue
		}
		for j := range rows {
			if !sameRow(rows[j], got[j]) {
				t.Fatalf("byte %d: batch decoded cleanly into different rows at %d:\n in  %+v\n out %+v",
					i, j, rows[j], got[j])
			}
		}
	}
	if checksumCaught == 0 {
		t.Fatal("no corruption was caught by the checksum")
	}
	t.Logf("%d body bytes: %d caught by checksum, %d by the decoder",
		bodyLen, checksumCaught, decoderCaught)
}

// TestCorruptFooterIsRefused covers the half of a batch the body's checksum
// cannot reach. The footer is what Scan reads and the gap flag is what it is
// read for, so a footer that can be quietly wrong is the fast path quietly
// losing a gap. Every byte of one is flipped in turn.
func TestCorruptFooterIsRefused(t *testing.T) {
	rows := realisticRows(200, 13)
	batch, err := encodeBatch(rows)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	for i := len(batch) - FooterSize; i < len(batch); i++ {
		corrupt := append([]byte(nil), batch...)
		corrupt[i] ^= 0x40
		if _, _, err := readBatch(bytes.NewReader(corrupt)); err == nil {
			t.Fatalf("byte %d of the footer was flipped and the batch was accepted", i)
		}
	}
}

// TestFootersAreScannable is the columnar layout's other payoff: what a window
// holds can be read without decompressing it.
func TestFootersAreScannable(t *testing.T) {
	root := t.TempDir()
	w, err := NewWriter(root, "BTC-USD", time.Hour)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteReseed(tapefile.Reseed{At: base, Reason: "subscribed"}); err != nil {
		t.Fatalf("reseed: %v", err)
	}
	for i := 0; i < DefaultMaxRows+50; i++ {
		at := base.Add(time.Duration(i) * time.Millisecond)
		if err := w.WriteMessage(tapefile.Message{Recv: at, Raw: []byte(matchFrame)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	last := base.Add(time.Duration(DefaultMaxRows+50) * time.Millisecond)
	if err := w.WriteGap(tapefile.Gap{At: last, Expected: 1, Got: 9}); err != nil {
		t.Fatalf("gap: %v", err)
	}
	path := w.Path()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	footers := scanFooters(t, path)
	if len(footers) != 2 {
		t.Fatalf("scanned %d batches, want 2 (%d rows at %d per batch)",
			len(footers), DefaultMaxRows+52, DefaultMaxRows)
	}
	var rows uint32
	var gap, reseed bool
	for _, ft := range footers {
		rows += ft.Rows
		gap = gap || ft.HasGap()
		reseed = reseed || ft.HasReseed()
	}
	if rows != uint32(DefaultMaxRows+52) {
		t.Errorf("footers count %d rows, want %d", rows, DefaultMaxRows+52)
	}
	if !gap || !reseed {
		t.Errorf("footer flags: gap=%v reseed=%v, want both", gap, reseed)
	}
	if got := time.Unix(0, footers[0].MinRecv).UTC(); !got.Equal(base) {
		t.Errorf("first batch starts at %s, want %s", got, base)
	}
	if got := time.Unix(0, footers[len(footers)-1].MaxRecv).UTC(); !got.Equal(last) {
		t.Errorf("last batch ends at %s, want %s", got, last)
	}
}

// TestBatchesAreBoundedByRecordsNotTheClock is the policy the live capture
// argued for. A batch closes on the records in it — how many, how large, how
// far apart their timestamps are — and on nothing else, so the same frames
// always produce the same file, and a durability flush cannot cut a batch down
// to a size not worth compressing.
func TestBatchesAreBoundedByRecordsNotTheClock(t *testing.T) {
	// Flushing after every record must not split a single batch.
	flushed := batchesFor(t, 20, time.Second, true)
	unflushed := batchesFor(t, 20, time.Second, false)
	if len(flushed) != len(unflushed) {
		t.Fatalf("flushing changed the batching: %d batches with, %d without",
			len(flushed), len(unflushed))
	}
	if len(flushed) != 1 {
		t.Fatalf("20 records one second apart made %d batches, want 1", len(flushed))
	}

	// Records far enough apart close a batch on age, so a slow feed still
	// reaches disk.
	spread := batchesFor(t, 40, DefaultMaxAge/4, false)
	if len(spread) < 8 {
		t.Fatalf("40 records %s apart made %d batches, want one per %s of feed",
			DefaultMaxAge/4, len(spread), DefaultMaxAge)
	}
	for _, f := range spread {
		if span := time.Duration(f.MaxRecv - f.MinRecv); span > DefaultMaxAge {
			t.Errorf("a batch spans %s, longer than the %s bound", span, DefaultMaxAge)
		}
	}
}

// batchesFor captures n records at the given spacing and returns the footers of
// the batches they landed in.
func batchesFor(t *testing.T, n int, gap time.Duration, flushEvery bool) []Footer {
	t.Helper()
	root := t.TempDir()
	w, err := NewWriter(root, "BTC-USD", time.Hour)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(i) * gap)
		if err := w.WriteMessage(tapefile.Message{Recv: at, Raw: []byte(matchFrame)}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if flushEvery {
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	files := w.Stats().Files
	if len(files) != 1 {
		t.Fatalf("wrote %d files, want 1", len(files))
	}
	footers := scanFooters(t, files[0])
	var rows uint32
	for _, ft := range footers {
		rows += ft.Rows
	}
	if rows != uint32(n) {
		t.Fatalf("wrote %d records, the footers count %d", n, rows)
	}
	return footers
}

// scanFooters reads a file's batch footers, skipping every body.
func scanFooters(t *testing.T, path string) []Footer {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if _, err := f.Seek(tapefile.HeaderSize, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	footers, err := Scan(f)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return footers
}

// TestVersionByteKeepsTheFormatsApart: neither reader may ever interpret the
// other's file. This is the guarantee the version byte exists for, and it is
// checked in both directions and through the dispatcher.
func TestVersionByteKeepsTheFormatsApart(t *testing.T) {
	v1root, v2root := writeBoth(t, time.Hour, func(w recordWriter) {
		if err := w.WriteMessage(tapefile.Message{Recv: base, Raw: []byte(matchFrame)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	})
	v1path := filepath.Join(v1root, tapeFiles(t, v1root)[0])
	v2path := filepath.Join(v2root, tapeFiles(t, v2root)[0])

	if _, err := Open(v1path); !errors.Is(err, tapefile.ErrBadVersion) {
		t.Errorf("colfmt.Open on a v1 file: %v, want ErrBadVersion", err)
	}
	if _, err := tapefile.Open(v2path); !errors.Is(err, tapefile.ErrBadVersion) {
		t.Errorf("tapefile.Open on a v2 file: %v, want ErrBadVersion", err)
	}

	for path, want := range map[string]uint16{v1path: tapefile.Version, v2path: Version} {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		rd, err := OpenRecords(f)
		if err != nil {
			t.Fatalf("OpenRecords %s: %v", path, err)
		}
		if rd.Version() != want {
			t.Errorf("%s dispatched to a v%d reader, want v%d", path, rd.Version(), want)
		}
		rd.Close()
	}

	// A version neither format claims is refused rather than guessed at.
	future := append(tapefile.EncodeHeader(99), 0, 0, 0, 0)
	if _, err := OpenRecords(io.NopCloser(bytes.NewReader(future))); !errors.Is(err, tapefile.ErrBadVersion) {
		t.Errorf("OpenRecords on a v99 file: %v, want ErrBadVersion", err)
	}
	if _, err := OpenRecords(io.NopCloser(bytes.NewReader([]byte("NOPE\x02\x00\x00\x00")))); !errors.Is(err, tapefile.ErrBadMagic) {
		t.Errorf("OpenRecords on foreign bytes: %v, want ErrBadMagic", err)
	}
}

// TestTruncatedFileFails: a capture killed mid-batch leaves a partial batch,
// and a partial batch is an error rather than a short window.
func TestTruncatedFileFails(t *testing.T) {
	var buf bytes.Buffer
	bw := NewBatchWriter(&buf)
	for i := 0; i < 100; i++ {
		if err := bw.WriteRecord(tapefile.RecordMessage, tapefile.EncodeMessage(
			tapefile.Message{Recv: base.Add(time.Duration(i) * time.Millisecond), Raw: []byte(matchFrame)},
		)); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	whole := buf.Bytes()

	for _, cut := range []int{len(whole) - 1, len(whole) - FooterSize, len(whole) / 2} {
		rd, err := NewReader(bytes.NewReader(whole[:cut]))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		var readErr error
		for {
			if _, _, err := rd.Next(); err != nil {
				readErr = err
				break
			}
		}
		if errors.Is(readErr, io.EOF) {
			t.Errorf("truncating to %d of %d bytes read as a clean end of file", cut, len(whole))
		}
	}
}

// TestEmptyWindowIsAValidFile: a file that was opened and never got a record is
// a header and nothing else, and it reads back as an empty window rather than
// as an error.
func TestEmptyWindowIsAValidFile(t *testing.T) {
	var buf bytes.Buffer
	bw := NewBatchWriter(&buf)
	if err := bw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rd, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := rd.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next on an empty file: %v, want io.EOF", err)
	}
}

// The drop count is a sparse column, so a batch with no drops in it must not
// carry the column at all — that is what keeps every v2 file written before the
// column existed decodable by this build, and this one decodable by the last.
func TestDropCountIsSparse(t *testing.T) {
	plain := []row{
		{kind: tapefile.RecordGap, at: base, expected: 10, got: 20},
		{kind: tapefile.RecordMessage, at: base.Add(time.Millisecond), raw: []byte(`{"type":"heartbeat"}`)},
	}
	batch, err := encodeBatch(plain)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	body, _, err := readBatch(bytes.NewReader(batch))
	if err != nil {
		t.Fatalf("readBatch: %v", err)
	}
	blocks, err := walkBlocks(body)
	if err != nil {
		t.Fatalf("walkBlocks: %v", err)
	}
	for _, b := range blocks {
		if b.id == colDropped {
			t.Fatalf("a batch with no drops wrote a %s column of %d bytes",
				columnName(colDropped), b.rawLen)
		}
	}

	withDrops := []row{
		{kind: tapefile.RecordGap, at: base, dropped: 1},
		{kind: tapefile.RecordMessage, at: base.Add(time.Millisecond), raw: []byte(`{"type":"heartbeat"}`)},
		{kind: tapefile.RecordGap, at: base.Add(2 * time.Millisecond), expected: 7, got: 9, dropped: 65_000},
		{kind: tapefile.RecordReseed, at: base.Add(3 * time.Millisecond), reason: "reconnect"},
	}
	got := roundTrip(t, withDrops)
	for i := range withDrops {
		if !sameRow(withDrops[i], got[i]) {
			t.Fatalf("row %d round-tripped to %+v, want %+v", i, got[i], withDrops[i])
		}
	}
}
