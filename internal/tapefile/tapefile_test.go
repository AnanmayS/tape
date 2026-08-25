package tapefile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/storage"
)

func mustWriter(t *testing.T, window time.Duration) (*Writer, string) {
	t.Helper()
	root := t.TempDir()
	w, err := NewWriter(root, "BTC-USD", window)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, root
}

func TestRoundTrip(t *testing.T) {
	w, _ := mustWriter(t, DefaultWindow)
	base := time.Date(2026, 8, 24, 23, 17, 3, 0, time.UTC)

	msg := Message{Recv: base, Raw: []byte(`{"type":"match","sequence":7}`)}
	gap := Gap{At: base.Add(time.Second), Expected: 8, Got: 12}
	res := Reseed{At: base.Add(2 * time.Second), Reason: "reconnect: read timeout"}

	for _, err := range []error{w.WriteMessage(msg), w.WriteGap(gap), w.WriteReseed(res)} {
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	path := w.Path()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if r.Version() != Version {
		t.Fatalf("version = %d, want %d", r.Version(), Version)
	}

	typ, p, err := r.Next()
	if err != nil || typ != RecordMessage {
		t.Fatalf("record 1: type=%v err=%v", typ, err)
	}
	got, err := DecodeMessage(p)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !got.Recv.Equal(msg.Recv) || !bytes.Equal(got.Raw, msg.Raw) {
		t.Fatalf("message round-trip: got %+v want %+v", got, msg)
	}

	typ, p, err = r.Next()
	if err != nil || typ != RecordGap {
		t.Fatalf("record 2: type=%v err=%v", typ, err)
	}
	gotGap, err := DecodeGap(p)
	if err != nil {
		t.Fatalf("DecodeGap: %v", err)
	}
	if gotGap.Expected != gap.Expected || gotGap.Got != gap.Got || !gotGap.At.Equal(gap.At) {
		t.Fatalf("gap round-trip: got %+v want %+v", gotGap, gap)
	}

	typ, p, err = r.Next()
	if err != nil || typ != RecordReseed {
		t.Fatalf("record 3: type=%v err=%v", typ, err)
	}
	gotRes, err := DecodeReseed(p)
	if err != nil {
		t.Fatalf("DecodeReseed: %v", err)
	}
	if gotRes.Reason != res.Reason || !gotRes.At.Equal(res.At) {
		t.Fatalf("reseed round-trip: got %+v want %+v", gotRes, res)
	}

	if _, _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF, got %v", err)
	}
}

func TestReaderRefusesUnknownVersion(t *testing.T) {
	h := encodeHeader()
	binary.LittleEndian.PutUint16(h[4:6], Version+1)
	_, err := NewReader(bytes.NewReader(h))
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
}

func TestReaderRefusesBadMagic(t *testing.T) {
	for name, in := range map[string][]byte{
		"wrong magic": []byte("NOPE\x01\x00\x00\x00"),
		"too short":   []byte("TAP"),
		"empty":       {},
	} {
		if _, err := NewReader(bytes.NewReader(in)); !errors.Is(err, ErrBadMagic) {
			t.Fatalf("%s: expected ErrBadMagic, got %v", name, err)
		}
	}
}

func TestRotationOnWindowBoundary(t *testing.T) {
	window := time.Minute
	w, root := mustWriter(t, window)

	base := time.Date(2026, 8, 24, 23, 0, 30, 0, time.UTC)
	times := []time.Time{
		base,                        // window 23:00
		base.Add(10 * time.Second),  // window 23:00
		base.Add(40 * time.Second),  // window 23:01
		base.Add(100 * time.Second), // window 23:02
	}
	for i, ts := range times {
		if err := w.WriteMessage(Message{Recv: ts, Raw: []byte{byte(i)}}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st := w.Stats()
	if st.Rotations != 2 {
		t.Fatalf("rotations = %d, want 2", st.Rotations)
	}
	if len(st.Files) != 3 {
		t.Fatalf("files = %v, want 3", st.Files)
	}

	// The layout is the storage key layout, spelled as a path. Local disk and
	// the object store name the same window the same way.
	part := filepath.Join(root, "v1", "symbol=BTC-USD", "date=2026-08-24", "hour=23")
	want := []string{
		filepath.Join(part, "20260824T230000Z.tape"),
		filepath.Join(part, "20260824T230100Z.tape"),
		filepath.Join(part, "20260824T230200Z.tape"),
	}
	for i, p := range want {
		if st.Files[i] != p {
			t.Fatalf("file %d = %s, want %s", i, st.Files[i], p)
		}
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("file %d missing: %v", i, err)
		}
	}

	// Every file must carry its own header and its own records.
	counts := []int{2, 1, 1}
	for i, p := range want {
		r, err := Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		n := 0
		for {
			_, _, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("read %s: %v", p, err)
			}
			n++
		}
		r.Close()
		if n != counts[i] {
			t.Fatalf("%s has %d records, want %d", p, n, counts[i])
		}
	}
}

// A file that has been rotated away must never be opened for writing again;
// that is what makes the capture append-only in practice and not just in
// intention.
func TestRefusesToReopenClosedFile(t *testing.T) {
	window := time.Minute
	w, _ := mustWriter(t, window)
	base := time.Date(2026, 8, 24, 23, 0, 30, 0, time.UTC)

	if err := w.WriteMessage(Message{Recv: base, Raw: []byte("a")}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.WriteMessage(Message{Recv: base.Add(time.Minute), Raw: []byte("b")}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A late-arriving record for the first window would require reopening it.
	err := w.WriteMessage(Message{Recv: base, Raw: []byte("c")})
	if err == nil || !strings.Contains(err.Error(), "refusing to reopen") {
		t.Fatalf("expected reopen refusal, got %v", err)
	}
}

func TestAppendOnlyNeverRewritesPrefix(t *testing.T) {
	w, _ := mustWriter(t, DefaultWindow)
	base := time.Date(2026, 8, 24, 23, 17, 0, 0, time.UTC)

	if err := w.WriteMessage(Message{Recv: base, Raw: []byte("first")}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	path := w.Path()
	prefix, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for i := 0; i < 50; i++ {
		if err := w.WriteMessage(Message{Recv: base.Add(time.Duration(i) * time.Second), Raw: []byte("more")}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	final, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(final) <= len(prefix) {
		t.Fatalf("file did not grow: %d -> %d", len(prefix), len(final))
	}
	if !bytes.Equal(final[:len(prefix)], prefix) {
		t.Fatal("earlier bytes changed; capture is not append-only")
	}
}

func TestStatsBytesMatchFileSize(t *testing.T) {
	w, _ := mustWriter(t, time.Minute)
	base := time.Date(2026, 8, 24, 23, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		ts := base.Add(time.Duration(i*10) * time.Second)
		if err := w.WriteMessage(Message{Recv: ts, Raw: bytes.Repeat([]byte("x"), i)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st := w.Stats()
	var total int64
	for _, p := range st.Files {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		total += fi.Size()
	}
	if total != st.Bytes {
		t.Fatalf("stats bytes = %d, on-disk = %d", st.Bytes, total)
	}
	if st.Records != 20 {
		t.Fatalf("records = %d, want 20", st.Records)
	}
}

func TestPayloadTooBig(t *testing.T) {
	w, _ := mustWriter(t, DefaultWindow)
	defer w.Close()
	err := w.Write(RecordMessage, make([]byte, MaxPayload+1), time.Now())
	if !errors.Is(err, ErrPayloadTooBig) {
		t.Fatalf("expected ErrPayloadTooBig, got %v", err)
	}
}

func TestDecodeShortPayloads(t *testing.T) {
	if _, err := DecodeMessage([]byte{1, 2}); !errors.Is(err, ErrShortPayload) {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if _, err := DecodeGap(make([]byte, 23)); !errors.Is(err, ErrShortPayload) {
		t.Fatalf("DecodeGap: %v", err)
	}
	if _, err := DecodeReseed([]byte{1}); !errors.Is(err, ErrShortPayload) {
		t.Fatalf("DecodeReseed: %v", err)
	}
}

func TestNewWriterValidation(t *testing.T) {
	if _, err := NewWriter(t.TempDir(), "BTC-USD", 0); err == nil {
		t.Fatal("expected error for zero window")
	}
	if _, err := NewWriter(t.TempDir(), "", time.Minute); err == nil {
		t.Fatal("expected error for empty symbol")
	}
	// A symbol with a slash in it would invent a partition level.
	if _, err := NewWriter(t.TempDir(), "BTC/USD", time.Minute); err == nil {
		t.Fatal("expected error for a symbol that would forge a key component")
	}
}

// TestPathIsRootPlusKey is the claim the whole upload path rests on: a file's
// local path is its object key under the root, so uploading it is copying it to
// the key its own path already spells.
func TestPathIsRootPlusKey(t *testing.T) {
	w, root := mustWriter(t, time.Minute)
	at := time.Date(2026, 8, 25, 14, 35, 20, 0, time.UTC)

	key := w.KeyFor(at)
	if want := storage.Key("BTC-USD", at.Truncate(time.Minute)); key != want {
		t.Errorf("KeyFor = %q, want %q", key, want)
	}
	if got, want := w.PathFor(at), filepath.Join(root, filepath.FromSlash(key)); got != want {
		t.Errorf("PathFor = %q, want %q", got, want)
	}
}

// TestOnFileClosed checks the hook uploads hang off: it fires once per file,
// after the file is closed, with the key that file belongs under — including
// for the last file, which is closed by Close rather than by a rotation.
func TestOnFileClosed(t *testing.T) {
	root := t.TempDir()
	var closed []File
	w, err := NewWriter(root, "BTC-USD", time.Minute, OnFileClosed(func(f File) {
		closed = append(closed, f)
	}))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	base := time.Date(2026, 8, 25, 13, 59, 30, 0, time.UTC)
	for i, ts := range []time.Time{base, base.Add(time.Minute), base.Add(2 * time.Minute)} {
		if err := w.WriteMessage(Message{Recv: ts, Raw: []byte{byte(i)}}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if n := len(closed); n != i {
			t.Fatalf("after write %d, %d files had closed; a file must not be handed over while it is still open", i, n)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(closed) != 3 {
		t.Fatalf("hook fired for %d files, want 3 (two rotations and the final close)", len(closed))
	}
	// The window crosses an hour boundary, so the files land in two partitions.
	want := []string{
		"v1/symbol=BTC-USD/date=2026-08-25/hour=13/20260825T135900Z.tape",
		"v1/symbol=BTC-USD/date=2026-08-25/hour=14/20260825T140000Z.tape",
		"v1/symbol=BTC-USD/date=2026-08-25/hour=14/20260825T140100Z.tape",
	}
	for i, f := range closed {
		if f.Key != want[i] {
			t.Errorf("closed file %d key = %q, want %q", i, f.Key, want[i])
		}
		if f.Path != filepath.Join(root, filepath.FromSlash(want[i])) {
			t.Errorf("closed file %d path = %q", i, f.Path)
		}
		// Closed means closed: the bytes are all there to be read.
		st, err := os.Stat(f.Path)
		if err != nil {
			t.Fatalf("closed file %d: %v", i, err)
		}
		if st.Size() < HeaderSize {
			t.Errorf("closed file %d is %d bytes; it was handed over unflushed", i, st.Size())
		}
	}
}
