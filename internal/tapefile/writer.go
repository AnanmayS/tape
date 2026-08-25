package tapefile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
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

// Writer appends records to rotating, append-only tape files.
//
// Files are named data/{symbol}/{date}/{window start}.tape and rotate on fixed
// wall-clock boundaries so that a file's name tells you exactly which window it
// covers regardless of when the process started. A file is opened for writing
// exactly once in a Writer's lifetime; attempting to open one twice is an
// error, not an assertion, because reopening is how append-only quietly stops
// being true.
type Writer struct {
	root   string
	symbol string
	window time.Duration

	f        *os.File
	bw       *bufio.Writer
	curStart time.Time
	curPath  string

	opened map[string]bool
	stats  Stats

	hdr [recordHeaderSize]byte
}

// NewWriter creates a Writer rooted at root, partitioning by symbol and
// rotating every window. No file is created until the first record.
func NewWriter(root, symbol string, window time.Duration) (*Writer, error) {
	if window <= 0 {
		return nil, fmt.Errorf("tapefile: rotation window must be positive, got %v", window)
	}
	if symbol == "" {
		return nil, fmt.Errorf("tapefile: symbol must not be empty")
	}
	return &Writer{
		root:   root,
		symbol: symbol,
		window: window,
		opened: make(map[string]bool),
	}, nil
}

// PathFor returns the file path covering instant t.
func (w *Writer) PathFor(t time.Time) string {
	start := t.UTC().Truncate(w.window)
	return filepath.Join(
		w.root,
		w.symbol,
		start.Format("2006-01-02"),
		start.Format("20060102T150405Z")+".tape",
	)
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
	w.f, w.bw = nil, nil
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
