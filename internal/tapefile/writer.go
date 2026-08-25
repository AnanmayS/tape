package tapefile

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Writer appends v1 records to rotating, append-only tape files.
//
// It is the record framing and nothing else: the rotation, the key layout, the
// closed-file hook and the byte counters all live in the Rotator underneath,
// which the columnar writer shares. See Rotator for what a file is named and
// why it is never reopened.
type Writer struct {
	rot     *Rotator
	records int64
	hdr     [recordHeaderSize]byte
}

// NewWriter creates a Writer rooted at root, partitioning by symbol and
// rotating every window. No file is created until the first record.
func NewWriter(root, symbol string, window time.Duration, opts ...Option) (*Writer, error) {
	rot, err := NewRotator(root, symbol, window, encodeHeader(), opts...)
	if err != nil {
		return nil, err
	}
	return &Writer{rot: rot}, nil
}

// KeyFor returns the object key of the window covering instant t, relative to
// the writer's root.
func (w *Writer) KeyFor(t time.Time) string { return w.rot.KeyFor(t) }

// PathFor returns the file path covering instant t.
func (w *Writer) PathFor(t time.Time) string { return w.rot.PathFor(t) }

// Write appends one record. at selects the rotation window, so records are
// always filed under the window they belong to rather than the window the
// process happens to be in.
func (w *Writer) Write(t RecordType, payload []byte, at time.Time) error {
	if len(payload) > MaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooBig, len(payload))
	}
	w.hdr[0] = byte(t)
	binary.LittleEndian.PutUint32(w.hdr[1:5], uint32(len(payload)))
	if err := w.rot.Append(at, w.hdr[:], payload); err != nil {
		return err
	}
	w.records++
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

// Flush pushes buffered bytes to the file. It does not fsync.
func (w *Writer) Flush() error { return w.rot.Flush() }

// Path returns the file currently open for writing, or "" if none.
func (w *Writer) Path() string { return w.rot.Path() }

// Key returns the object key of the file currently open for writing, or "" if
// none.
func (w *Writer) Key() string { return w.rot.Key() }

// Stats returns a snapshot of write counters.
func (w *Writer) Stats() Stats {
	s := w.rot.Stats()
	s.Records = w.records
	return s
}

// Close flushes and closes the open file. The Writer is not reusable.
func (w *Writer) Close() error { return w.rot.Close() }
