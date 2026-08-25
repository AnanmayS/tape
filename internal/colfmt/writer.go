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
// it is full, because a column that is written one value at a time is not a
// column. Flush encodes the pending batch, so the capture loop's flush ticker
// bounds how long a record can sit unwritten exactly as it does for v1, and a
// rotation flushes the pending batch into the file it belongs to before that
// file is closed and handed over.
type Writer struct {
	rot    *tapefile.Rotator
	window time.Duration

	rows     []row
	rowBytes int

	// batchAt is the timestamp the pending batch is filed under, and batchWin
	// is the rotation window that timestamp falls in. The batch is what decides
	// when a file rotates, not the rotator: nothing reaches the rotator until a
	// batch is flushed, so by the time it could notice a boundary the records
	// that crossed it would already be in the wrong file.
	batchAt  time.Time
	batchWin time.Time

	maxRows  int
	maxBytes int
	records  int64
}

// NewWriter creates a Writer rooted at root, partitioning by symbol and
// rotating every window. No file is created until the first record.
func NewWriter(root, symbol string, window time.Duration, opts ...tapefile.Option) (*Writer, error) {
	rot, err := tapefile.NewRotator(root, symbol, window, tapefile.EncodeHeader(Version), opts...)
	if err != nil {
		return nil, err
	}
	return &Writer{
		rot:      rot,
		window:   window,
		maxRows:  DefaultMaxRows,
		maxBytes: DefaultMaxBytes,
	}, nil
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
	if len(w.rows) > 0 && !win.Equal(w.batchWin) {
		// The pending batch belongs to the file that is about to close, so it
		// goes in first. A batch never spans two windows: a record is filed
		// under the window its own timestamp names, exactly as in v1.
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
	if len(w.rows) == 0 {
		w.batchAt, w.batchWin = r.at, win
	}
	w.rows = append(w.rows, r)
	w.rowBytes += len(r.raw)
	w.records++
	if len(w.rows) >= w.maxRows || w.rowBytes >= w.maxBytes {
		return w.flushBatch()
	}
	return nil
}

// flushBatch encodes the pending batch into the file covering its first row.
func (w *Writer) flushBatch() error {
	if len(w.rows) == 0 {
		return nil
	}
	b, err := encodeBatch(w.rows)
	if err != nil {
		return err
	}
	if err := w.rot.Append(w.batchAt, b); err != nil {
		return err
	}
	w.rows, w.rowBytes = w.rows[:0], 0
	return nil
}

// Flush encodes the pending batch and pushes buffered bytes to the file. It
// does not fsync.
func (w *Writer) Flush() error {
	if err := w.flushBatch(); err != nil {
		return err
	}
	return w.rot.Flush()
}

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
	w        io.Writer
	rows     []row
	rowBytes int
	maxRows  int
	maxBytes int
	started  bool
}

// NewBatchWriter returns a BatchWriter that writes a whole v2 file to w,
// starting with the header.
func NewBatchWriter(w io.Writer) *BatchWriter {
	return &BatchWriter{w: w, maxRows: DefaultMaxRows, maxBytes: DefaultMaxBytes}
}

// WriteRecord appends one stored record, in the vocabulary tapefile stores it
// in. It is the transcoding entry point: a v1 reader hands out exactly this.
func (b *BatchWriter) WriteRecord(t tapefile.RecordType, payload []byte) error {
	r, err := rowFromPayload(t, payload)
	if err != nil {
		return err
	}
	b.rows = append(b.rows, r)
	b.rowBytes += len(r.raw)
	if len(b.rows) >= b.maxRows || b.rowBytes >= b.maxBytes {
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
	if len(b.rows) == 0 {
		return nil
	}
	enc, err := encodeBatch(b.rows)
	if err != nil {
		return err
	}
	if _, err := b.w.Write(enc); err != nil {
		return err
	}
	b.rows, b.rowBytes = b.rows[:0], 0
	return nil
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
