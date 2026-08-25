package tapefile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/AnanmayS/tape/internal/storage"
)

// DefaultWindow is the wall-clock rotation window.
const DefaultWindow = 5 * time.Minute

// bufSize is the write buffer. Coinbase level2_batch for one product runs a few
// hundred KiB/s; 256 KiB keeps syscalls down without hiding much data on crash,
// and Flush is called on a ticker by the capture loop anyway.
const bufSize = 256 << 10

// Stats describes what a Writer has done. Bytes counts bytes handed to the
// file, headers included.
type Stats struct {
	Records   int64
	Bytes     int64
	Rotations int
	Files     []string
}

// File is a tape file the Writer has finished with: closed, flushed, and never
// to be written again. It is what an OnFileClosed hook receives.
type File struct {
	// Path is the file on local disk.
	Path string

	// Key is the same file's object key, relative to the writer's root. Local
	// disk and the object store use one layout, so this is Path with the root
	// taken off — see the storage package.
	Key string

	// Start is the window the file covers.
	Start time.Time
}

// Writer appends records to rotating, append-only tape files.
//
// Files are laid out under root by the canonical storage key —
// v1/symbol=BTC-USD/date=2026-08-25/hour=14/{window start}.tape — and rotate on
// fixed wall-clock boundaries, so a file's name tells you exactly which window
// it covers regardless of when the process started. Local disk and the object
// store use the same layout deliberately: uploading a file is then copying it
// to the key its own path already spells, and a window replayed from a bucket
// and the same window replayed from disk are named identically.
//
// A file is opened for writing exactly once in a Writer's lifetime; attempting
// to open one twice is an error, not an assertion, because reopening is how
// append-only quietly stops being true.
type Writer struct {
	root   string
	symbol string
	window time.Duration

	f        *os.File
	bw       *bufio.Writer
	curStart time.Time
	curPath  string
	curKey   string

	onClosed func(File)

	opened map[string]bool
	stats  Stats

	hdr [recordHeaderSize]byte
}

// Option configures a Writer.
type Option func(*Writer)

// OnFileClosed registers a hook called once for every file the Writer finishes
// with, after its bytes are flushed and its descriptor closed — including the
// last file, at Close. It is how a completed window reaches the object store.
//
// The hook runs on the writer's goroutine, so it must not block: an upload is
// queued from here, never performed here.
func OnFileClosed(fn func(File)) Option {
	return func(w *Writer) { w.onClosed = fn }
}

// NewWriter creates a Writer rooted at root, partitioning by symbol and
// rotating every window. No file is created until the first record.
func NewWriter(root, symbol string, window time.Duration, opts ...Option) (*Writer, error) {
	if window <= 0 {
		return nil, fmt.Errorf("tapefile: rotation window must be positive, got %v", window)
	}
	if err := storage.ValidateSymbol(symbol); err != nil {
		return nil, err
	}
	w := &Writer{
		root:   root,
		symbol: symbol,
		window: window,
		opened: make(map[string]bool),
	}
	for _, o := range opts {
		o(w)
	}
	return w, nil
}

// KeyFor returns the object key of the window covering instant t, relative to
// the writer's root.
func (w *Writer) KeyFor(t time.Time) string {
	return storage.Key(w.symbol, t.UTC().Truncate(w.window))
}

// PathFor returns the file path covering instant t.
func (w *Writer) PathFor(t time.Time) string {
	return filepath.Join(w.root, filepath.FromSlash(w.KeyFor(t)))
}

// Write appends one record. at selects the rotation window, so records are
// always filed under the window they belong to rather than the window the
// process happens to be in.
func (w *Writer) Write(t RecordType, payload []byte, at time.Time) error {
	if len(payload) > MaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooBig, len(payload))
	}
	if err := w.ensure(at); err != nil {
		return err
	}
	w.hdr[0] = byte(t)
	binary.LittleEndian.PutUint32(w.hdr[1:5], uint32(len(payload)))
	if _, err := w.bw.Write(w.hdr[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	w.stats.Records++
	w.stats.Bytes += int64(recordHeaderSize + len(payload))
	return nil
}

// WriteMessage appends a raw feed frame with its receive timestamp.
func (w *Writer) WriteMessage(m Message) error {
	return w.Write(RecordMessage, EncodeMessage(m), m.Recv)
}

// WriteGap appends a gap record.
func (w *Writer) WriteGap(g Gap) error {
	return w.Write(RecordGap, EncodeGap(g), g.At)
}

// WriteReseed appends a reseed record.
func (w *Writer) WriteReseed(r Reseed) error {
	return w.Write(RecordReseed, EncodeReseed(r), r.At)
}

// ensure makes sure the file covering at is the open one, rotating if not.
func (w *Writer) ensure(at time.Time) error {
	start := at.UTC().Truncate(w.window)
	if w.f != nil && start.Equal(w.curStart) {
		return nil
	}
	if w.f != nil {
		if err := w.closeCurrent(); err != nil {
			return err
		}
		w.stats.Rotations++
	}
	return w.open(start)
}

func (w *Writer) open(start time.Time) error {
	path := w.PathFor(start)
	if w.opened[path] {
		// Reopening a closed file for writing is exactly the bug the
		// append-only invariant exists to prevent. Refuse loudly.
		return fmt.Errorf("tapefile: refusing to reopen %s for writing", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.opened[path] = true
	w.f = f
	w.bw = bufio.NewWriterSize(f, bufSize)
	w.curStart = start
	w.curPath = path
	w.curKey = w.KeyFor(start)
	w.stats.Files = append(w.stats.Files, path)

	// Only a brand-new file gets a header. If a previous process died holding
	// this window, we append to what it wrote rather than corrupting it.
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		h := encodeHeader()
		if _, err := w.bw.Write(h); err != nil {
			return err
		}
		w.stats.Bytes += int64(len(h))
	}
	return nil
}

func (w *Writer) closeCurrent() error {
	if w.f == nil {
		return nil
	}
	err := w.bw.Flush()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	closed := File{Path: w.curPath, Key: w.curKey, Start: w.curStart}
	w.f, w.bw = nil, nil

	// The hook fires only for a file that closed cleanly. A file whose bytes
	// would not flush is not a window; copying it to an immutable key that can
	// never be corrected afterwards would be the worst possible response.
	if err == nil && w.onClosed != nil {
		w.onClosed(closed)
	}
	return err
}

// Flush pushes buffered bytes to the file. It does not fsync.
func (w *Writer) Flush() error {
	if w.bw == nil {
		return nil
	}
	return w.bw.Flush()
}

// Path returns the file currently open for writing, or "" if none.
func (w *Writer) Path() string {
	if w.f == nil {
		return ""
	}
	return w.curPath
}

// Key returns the object key of the file currently open for writing, or "" if
// none.
func (w *Writer) Key() string {
	if w.f == nil {
		return ""
	}
	return w.curKey
}

// Stats returns a snapshot of write counters.
func (w *Writer) Stats() Stats {
	s := w.stats
	s.Files = append([]string(nil), w.stats.Files...)
	return s
}

// Close flushes and closes the open file. The Writer is not reusable.
func (w *Writer) Close() error {
	return w.closeCurrent()
}
