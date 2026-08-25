package tapefile

import (
	"bufio"
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

// Stats describes what a writer has done. Bytes counts bytes handed to the
// file, headers included.
type Stats struct {
	Records   int64
	Bytes     int64
	Rotations int
	Files     []string
}

// File is a tape file a writer has finished with: closed, flushed, and never to
// be written again. It is what an OnFileClosed hook receives.
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

// Option configures a Rotator, and through it either writer built on one.
type Option func(*Rotator)

// OnFileClosed registers a hook called once for every file the writer finishes
// with, after its bytes are flushed and its descriptor closed — including the
// last file, at Close. It is how a completed window reaches the object store.
//
// The hook runs on the writer's goroutine, so it must not block: an upload is
// queued from here, never performed here.
func OnFileClosed(fn func(File)) Option {
	return func(r *Rotator) { r.onClosed = fn }
}

// Rotator appends bytes to rotating, append-only files under the storage key
// layout. It is the half of a tape writer that has nothing to do with the
// format of what is written: both on-disk formats — v1 records and v2 columnar
// batches — rotate on the same boundaries, land under the same keys and hand
// closed files to the same hook, and only the bytes in between differ.
//
// Files rotate on fixed wall-clock boundaries, so a file's name tells you
// exactly which window it covers regardless of when the process started. Local
// disk and the object store use the same layout deliberately: uploading a file
// is then copying it to the key its own path already spells, and a window
// replayed from a bucket and the same window replayed from disk are named
// identically.
//
// A file is opened for writing exactly once in a Rotator's lifetime; attempting
// to open one twice is an error, not an assertion, because reopening is how
// append-only quietly stops being true.
type Rotator struct {
	root   string
	symbol string
	window time.Duration
	header []byte

	f        *os.File
	bw       *bufio.Writer
	curStart time.Time
	curPath  string
	curKey   string

	onClosed func(File)

	opened map[string]bool
	stats  Stats
}

// NewRotator creates a Rotator rooted at root, partitioning by symbol and
// rotating every window. header is written to each brand-new file and is what
// tells a reader which format the file is in; no file is created until the
// first append.
func NewRotator(root, symbol string, window time.Duration, header []byte, opts ...Option) (*Rotator, error) {
	if window <= 0 {
		return nil, fmt.Errorf("tapefile: rotation window must be positive, got %v", window)
	}
	if err := storage.ValidateSymbol(symbol); err != nil {
		return nil, err
	}
	r := &Rotator{
		root:   root,
		symbol: symbol,
		window: window,
		header: header,
		opened: make(map[string]bool),
	}
	for _, o := range opts {
		o(r)
	}
	return r, nil
}

// KeyFor returns the object key of the window covering instant t, relative to
// the root.
func (r *Rotator) KeyFor(t time.Time) string {
	return storage.Key(r.symbol, t.UTC().Truncate(r.window))
}

// PathFor returns the file path covering instant t.
func (r *Rotator) PathFor(t time.Time) string {
	return filepath.Join(r.root, filepath.FromSlash(r.KeyFor(t)))
}

// Rotates reports whether appending at instant t would close the open file
// first. A format that buffers records — the columnar one buffers a batch — has
// to hand its buffer over before that happens, and this is how it knows.
func (r *Rotator) Rotates(at time.Time) bool {
	return r.f != nil && !at.UTC().Truncate(r.window).Equal(r.curStart)
}

// Append writes chunks to the file covering at, rotating first if that is not
// the open one. Called with no chunks it only opens that file, which is how a
// buffering format forces a rotation to happen at the moment the window turns
// over rather than whenever its buffer next fills.
func (r *Rotator) Append(at time.Time, chunks ...[]byte) error {
	if err := r.ensure(at); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := r.bw.Write(c); err != nil {
			return err
		}
		r.stats.Bytes += int64(len(c))
	}
	return nil
}

// ensure makes sure the file covering at is the open one, rotating if not.
func (r *Rotator) ensure(at time.Time) error {
	start := at.UTC().Truncate(r.window)
	if r.f != nil && start.Equal(r.curStart) {
		return nil
	}
	if r.f != nil {
		if err := r.closeCurrent(); err != nil {
			return err
		}
		r.stats.Rotations++
	}
	return r.open(start)
}

func (r *Rotator) open(start time.Time) error {
	path := r.PathFor(start)
	if r.opened[path] {
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
	r.opened[path] = true
	r.f = f
	r.bw = bufio.NewWriterSize(f, bufSize)
	r.curStart = start
	r.curPath = path
	r.curKey = r.KeyFor(start)
	r.stats.Files = append(r.stats.Files, path)

	// Only a brand-new file gets a header. If a previous process died holding
	// this window, we append to what it wrote rather than corrupting it.
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		if _, err := r.bw.Write(r.header); err != nil {
			return err
		}
		r.stats.Bytes += int64(len(r.header))
	}
	return nil
}

func (r *Rotator) closeCurrent() error {
	if r.f == nil {
		return nil
	}
	err := r.bw.Flush()
	if cerr := r.f.Close(); err == nil {
		err = cerr
	}
	closed := File{Path: r.curPath, Key: r.curKey, Start: r.curStart}
	r.f, r.bw = nil, nil

	// The hook fires only for a file that closed cleanly. A file whose bytes
	// would not flush is not a window; copying it to an immutable key that can
	// never be corrected afterwards would be the worst possible response.
	if err == nil && r.onClosed != nil {
		r.onClosed(closed)
	}
	return err
}

// Flush pushes buffered bytes to the file. It does not fsync.
func (r *Rotator) Flush() error {
	if r.bw == nil {
		return nil
	}
	return r.bw.Flush()
}

// Path returns the file currently open for writing, or "" if none.
func (r *Rotator) Path() string {
	if r.f == nil {
		return ""
	}
	return r.curPath
}

// Key returns the object key of the file currently open for writing, or "" if
// none.
func (r *Rotator) Key() string {
	if r.f == nil {
		return ""
	}
	return r.curKey
}

// Stats returns a snapshot of the byte and file counters. Records is left to
// the format writer on top, which is the only thing that knows what a record is.
func (r *Rotator) Stats() Stats {
	s := r.stats
	s.Files = append([]string(nil), r.stats.Files...)
	return s
}

// Close flushes and closes the open file. The Rotator is not reusable.
func (r *Rotator) Close() error { return r.closeCurrent() }
