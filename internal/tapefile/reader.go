package tapefile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Reader reads records from a tape file. It refuses files whose magic or
// version it does not recognise rather than misinterpreting their bytes.
type Reader struct {
	r       *bufio.Reader
	c       io.Closer
	version uint16
	hdr     [recordHeaderSize]byte
}

// NewReader validates the header on r and returns a Reader positioned at the
// first record.
func NewReader(r io.Reader) (*Reader, error) {
	br := bufio.NewReaderSize(r, bufSize)
	var h [HeaderSize]byte
	if _, err := io.ReadFull(br, h[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrBadMagic
		}
		return nil, err
	}
	v, err := decodeHeader(h[:])
	if err != nil {
		return nil, err
	}
	return &Reader{r: br, version: v}, nil
}

// OpenReader validates the header on rc and returns a Reader that takes
// ownership of it: closing the Reader closes rc. It is the streaming entry
// point, used to read a tape object straight off a store without staging it on
// local disk first.
//
// If the header is bad, rc is closed and the error returned; the caller is
// never left holding something it did not get a Reader for.
func OpenReader(rc io.ReadCloser) (*Reader, error) {
	rd, err := NewReader(rc)
	if err != nil {
		rc.Close()
		return nil, err
	}
	rd.c = rc
	return rd, nil
}

// Open opens a tape file for reading. Close the Reader when done.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rd, err := OpenReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return rd, nil
}

// Version returns the format version of the file being read.
func (r *Reader) Version() uint16 { return r.version }

// Next returns the next record. It returns io.EOF at a clean end of file. The
// returned payload is freshly allocated and owned by the caller.
func (r *Reader) Next() (RecordType, []byte, error) {
	if _, err := io.ReadFull(r.r, r.hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, nil, fmt.Errorf("tapefile: truncated record header: %w", err)
		}
		return 0, nil, err // io.EOF at a record boundary is a clean end
	}
	t := RecordType(r.hdr[0])
	n := binary.LittleEndian.Uint32(r.hdr[1:5])
	if n > MaxPayload {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrPayloadTooBig, n)
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r.r, p); err != nil {
		return 0, nil, fmt.Errorf("tapefile: truncated record payload: %w", err)
	}
	return t, p, nil
}

// Close closes the underlying file, if the Reader owns one.
func (r *Reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}
