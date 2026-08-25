package colfmt

import (
	"fmt"
	"io"
	"time"

	"github.com/AnanmayS/tape/internal/tapefile"
)

// Writer appends columnar batches to rotating, append-only tape files.
//
// It is a drop-in for tapefile.Writer: same constructor shape, same three write
// methods, same options, same stats, and files under the same keys. The
// rotation, the key layout and the closed-file hook are literally the same code
// — a tapefile.Rotator — and only the bytes between the header and the end of
// the file differ.
//
// The one behavioural difference worth knowing is buffering. A v1 writer hands
// each record to the file as it arrives; this one holds a batch in memory until
// it is full, because a column written one value at a time is not a column.
//
// What closes a batch is DefaultMaxRows, DefaultMaxBytes and DefaultMaxAge —
// all three read off the records themselves, never off the clock. So the bytes
// a session writes are a function of the frames that went into it, the same way
// a replay's bytes are, and a batch is big enough to be worth compressing. Flush
// deliberately does not force a batch; see its comment for what that costs and
// what it buys. A rotation always does, into the file the records belong to,
// before that file is closed and handed to the uploader.
type Writer struct {
	rot    *tapefile.Rotator
	window time.Duration

	// pending is the batch being built. It is also what decides when a file
	// rotates, rather than the rotator: nothing reaches the rotator until a
	// batch is flushed, so by the time it could notice a window boundary the
	// records that crossed it would already be in the wrong file.
	pending batch

	// batchWin is the rotation window the pending batch belongs to.
	batchWin time.Time

	records int64
}

// NewWriter creates a Writer rooted at root, partitioning by symbol and
// rotating every window. No file is created until the first record.
func NewWriter(root, symbol string, window time.Duration, opts ...tapefile.Option) (*Writer, error) {
	rot, err := tapefile.NewRotator(root, symbol, window, tapefile.EncodeHeader(Version), opts...)
	if err != nil {
		return nil, err
	}
	return &Writer{rot: rot, window: window, pending: newBatch()}, nil
}

// KeyFor returns the object key of the window covering instant t, relative to
// the writer's root.
func (w *Writer) KeyFor(t time.Time) string { return w.rot.KeyFor(t) }

// PathFor returns the file path covering instant t.
func (w *Writer) PathFor(t time.Time) string { return w.rot.PathFor(t) }

// WriteMessage appends a raw feed frame with its receive timestamp.
//
// The frame is copied. The caller's slice is not retained past this call, which
// is the same contract the v1 writer offers and matters more here: a batch
// lives for thousands of records, and a format that quietly held a reference to
// somebody else's buffer for that long would be a trap.
func (w *Writer) WriteMessage(m tapefile.Message) error {
	m.Raw = append([]byte(nil), m.Raw...)
	return w.add(messageRow(m))
}

// WriteGap appends a gap record.
func (w *Writer) WriteGap(g tapefile.Gap) error { return w.add(gapRow(g)) }

// WriteReseed appends a reseed record.
func (w *Writer) WriteReseed(r tapefile.Reseed) error { return w.add(reseedRow(r)) }

func (w *Writer) add(r row) error {
	win := r.at.UTC().Truncate(w.window)

	// A batch never spans two windows: a record is filed under the window its
	// own timestamp names, exactly as in v1. The pending batch belongs to the
	// file that is about to close, so it goes in first.
	if w.pending.stale(r) || (!w.pending.empty() && !win.Equal(w.batchWin)) {
		if err := w.flushBatch(); err != nil {
			return err
		}
	}
	if w.rot.Rotates(r.at) {
		// Force the rotation now rather than at the next flush, so a closed
		// window reaches the object store the moment the window turns over.
		if err := w.rot.Append(r.at); err != nil {
			return err
		}
	}
	if w.pending.empty() {
		w.batchWin = win
	}
	w.pending.add(r)
	w.records++
	if w.pending.full() {
		return w.flushBatch()
	}
	return nil
}

// flushBatch encodes the pending batch into the file covering its first row.
func (w *Writer) flushBatch() error {
	if w.pending.empty() {
		return nil
	}
	at := w.pending.rows[0].at
	b, err := encodeBatch(w.pending.rows)
	if err != nil {
		return err
	}
	if err := w.rot.Append(at, b); err != nil {
		return err
	}
	w.pending.reset()
	return nil
}

// Flush pushes buffered bytes to the file. It does not fsync, and it does not
// force the pending batch.
//
// That is a deliberate difference from the v1 writer, and it is the one place
// the two formats do not behave alike. Capture calls Flush on a one-second
// ticker to bound how long a record sits in a buffer; encoding a batch every
// time it fires would mean a compression window of whatever arrived in a
// second, which on this feed is twenty-three records and 4.29x instead of
// 4.57x. It would also make the stored bytes depend on when the ticker fired.
//
// The pending batch is instead bounded by the records in it — see the batch
// sizing constants — and flushed by a rotation and by Close. What a hard kill
// can lose is therefore that batch: at most DefaultMaxRows records,
// DefaultMaxBytes of frames, or DefaultMaxAge of feed. A clean stop, which is
// how SIGINT and an ECS task stop both arrive, loses nothing.
func (w *Writer) Flush() error { return w.rot.Flush() }

// Path returns the file currently open for writing, or "" if none.
func (w *Writer) Path() string { return w.rot.Path() }

// Key returns the object key of the file currently open for writing, or "" if
// none.
func (w *Writer) Key() string { return w.rot.Key() }

// Stats returns a snapshot of write counters. Bytes is what reached the file,
// so it lags Records by whatever is still in the pending batch until a flush.
func (w *Writer) Stats() tapefile.Stats {
	s := w.rot.Stats()
	s.Records = w.records
	return s
}

// Close flushes the pending batch and closes the open file. The Writer is not
// reusable.
func (w *Writer) Close() error {
	err := w.flushBatch()
	if cerr := w.rot.Close(); err == nil {
		err = cerr
	}
	return err
}

// BatchWriter writes a columnar stream to a plain io.Writer, with no rotation
// and no files.
//
// It exists for the two jobs that are about the format rather than about
// capture: transcoding an existing window from v1, and measuring one. Capture
// uses Writer.
type BatchWriter struct {
	w       io.Writer
	pending batch
	started bool
}

// NewBatchWriter returns a BatchWriter that writes a whole v2 file to w,
// starting with the header. It batches under the same three bounds Writer does,
// so what it produces for a given stream of records is what capture would have
// produced for the same records.
func NewBatchWriter(w io.Writer) *BatchWriter {
	return &BatchWriter{w: w, pending: newBatch()}
}

// WriteRecord appends one stored record, in the vocabulary tapefile stores it
// in. It is the transcoding entry point: a v1 reader hands out exactly this.
func (b *BatchWriter) WriteRecord(t tapefile.RecordType, payload []byte) error {
	r, err := rowFromPayload(t, payload)
	if err != nil {
		return err
	}
	if b.pending.stale(r) {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	b.pending.add(r)
	if b.pending.full() {
		return b.Flush()
	}
	return nil
}

// Flush writes the pending batch, and the file header if nothing has been
// written yet.
func (b *BatchWriter) Flush() error {
	if !b.started {
		if _, err := b.w.Write(tapefile.EncodeHeader(Version)); err != nil {
			return err
		}
		b.started = true
	}
	if b.pending.empty() {
		return nil
	}
	enc, err := encodeBatch(b.pending.rows)
	if err != nil {
		return err
	}
	if _, err := b.w.Write(enc); err != nil {
		return err
	}
	b.pending.reset()
	return nil
}

// batch is the pending rows and the three bounds that close them. One policy,
// used by both writers: a file's batching is a property of the records in it
// and not of which writer produced them.
type batch struct {
	rows  []row
	bytes int

	maxRows  int
	maxBytes int
	maxAge   time.Duration
}

func newBatch() batch {
	return batch{maxRows: DefaultMaxRows, maxBytes: DefaultMaxBytes, maxAge: DefaultMaxAge}
}

func (b *batch) empty() bool { return len(b.rows) == 0 }

// stale reports whether r is too far past the start of the pending batch to
// join it. The span is measured between the records' own timestamps, so it is
// the feed that closes the batch and never the wall clock.
func (b *batch) stale(r row) bool {
	return len(b.rows) > 0 && r.at.Sub(b.rows[0].at) >= b.maxAge
}

func (b *batch) add(r row) {
	b.rows = append(b.rows, r)
	b.bytes += len(r.raw)
}

func (b *batch) full() bool {
	return len(b.rows) >= b.maxRows || b.bytes >= b.maxBytes
}

func (b *batch) reset() {
	b.rows, b.bytes = b.rows[:0], 0
}

// Close flushes the pending batch. It does not close the underlying writer,
// which it does not own.
func (b *BatchWriter) Close() error { return b.Flush() }

// rowFromPayload rebuilds a row from a stored v1 payload.
func rowFromPayload(t tapefile.RecordType, payload []byte) (row, error) {
	switch t {
	case tapefile.RecordMessage:
		m, err := tapefile.DecodeMessage(payload)
		if err != nil {
			return row{}, err
		}
		m.Raw = append([]byte(nil), m.Raw...)
		return messageRow(m), nil
	case tapefile.RecordGap:
		g, err := tapefile.DecodeGap(payload)
		if err != nil {
			return row{}, err
		}
		return gapRow(g), nil
	case tapefile.RecordReseed:
		r, err := tapefile.DecodeReseed(payload)
		if err != nil {
			return row{}, err
		}
		return reseedRow(r), nil
	default:
		return row{}, fmt.Errorf("colfmt: unknown record type %s", t)
	}
}
