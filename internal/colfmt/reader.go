package colfmt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/AnanmayS/tape/internal/tapefile"
)

// bufSize matches the writer's. A batch is read whole in any case; this is only
// how many syscalls it takes.
const bufSize = 256 << 10

// Reader reads records from a v2 columnar file, one batch at a time.
//
// It presents the same stream a v1 reader presents: the (type, payload) pairs
// of package tapefile, in stored order, byte for byte. Everything columnar
// about the file stops here.
type Reader struct {
	r   *bufio.Reader
	c   io.Closer
	ver uint16

	rows []row
	i    int
}

var _ tapefile.Records = (*Reader)(nil)

// NewReader validates the header on r and returns a Reader positioned at the
// first batch. It refuses anything that is not v2 — including a v1 file, which
// is a different layout wearing the same magic.
func NewReader(r io.Reader) (*Reader, error) {
	br := bufio.NewReaderSize(r, bufSize)
	var h [tapefile.HeaderSize]byte
	if _, err := io.ReadFull(br, h[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, tapefile.ErrBadMagic
		}
		return nil, err
	}
	v, err := tapefile.HeaderVersion(h[:])
	if err != nil {
		return nil, err
	}
	if v != Version {
		return nil, fmt.Errorf("%w: file is v%d, this reader reads v%d",
			tapefile.ErrBadVersion, v, Version)
	}
	return &Reader{r: br, ver: v}, nil
}

// OpenReader validates the header on rc and returns a Reader that takes
// ownership of it: closing the Reader closes rc. If the header is bad, rc is
// closed and the error returned.
func OpenReader(rc io.ReadCloser) (*Reader, error) {
	rd, err := NewReader(rc)
	if err != nil {
		rc.Close()
		return nil, err
	}
	rd.c = rc
	return rd, nil
}

// Open opens a columnar file for reading. Close the Reader when done.
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
func (r *Reader) Version() uint16 { return r.ver }

// Next returns the next record. It returns io.EOF at a clean end of file. The
// returned payload is freshly allocated and owned by the caller.
func (r *Reader) Next() (tapefile.RecordType, []byte, error) {
	for r.i >= len(r.rows) {
		rows, _, err := r.nextBatch()
		if err != nil {
			return 0, nil, err
		}
		r.rows, r.i = rows, 0
	}
	row := r.rows[r.i]
	r.i++
	return row.kind, row.payload(), nil
}

// nextBatch reads and decodes one batch.
func (r *Reader) nextBatch() ([]row, Footer, error) {
	body, f, err := readBatch(r.r)
	if err != nil {
		return nil, Footer{}, err
	}
	rows, err := decodeBatch(body, f)
	if err != nil {
		return nil, Footer{}, err
	}
	if len(rows) != int(f.Rows) {
		return nil, Footer{}, fmt.Errorf("%w: batch decoded %d rows, footer says %d",
			ErrCorrupt, len(rows), f.Rows)
	}
	return rows, f, nil
}

// Close closes the underlying stream, if the Reader owns one.
func (r *Reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

// readBatch reads one batch's body and footer, checking the footer against the
// length prefix and the body against its checksum before returning either.
//
// The checksum is not ceremony. A delta-encoded column that loses a byte does
// not fail to decode; it decodes into prices that are wrong by whatever the
// shift happened to be and look exactly like prices. This is the check that
// makes that an error instead of a result.
func readBatch(r io.Reader) ([]byte, Footer, error) {
	var lb [batchLenSize]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, Footer{}, fmt.Errorf("colfmt: truncated batch length: %w", err)
		}
		return nil, Footer{}, err // io.EOF at a batch boundary is a clean end
	}

	n := binary.LittleEndian.Uint32(lb[:])
	if n > MaxBatchBody {
		return nil, Footer{}, fmt.Errorf("%w: %d bytes", ErrBatchTooBig, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, Footer{}, fmt.Errorf("colfmt: truncated batch body: %w", unexpectedEOF(err))
	}
	var fb [FooterSize]byte
	if _, err := io.ReadFull(r, fb[:]); err != nil {
		return nil, Footer{}, fmt.Errorf("colfmt: truncated batch footer: %w", unexpectedEOF(err))
	}
	f, err := decodeFooter(fb[:])
	if err != nil {
		return nil, Footer{}, err
	}
	if f.BodyLen != n {
		return nil, Footer{}, fmt.Errorf("%w: footer says %d body bytes, prefix says %d",
			ErrCorrupt, f.BodyLen, n)
	}
	if got := checksum(body); got != f.CRC {
		return nil, Footer{}, fmt.Errorf("%w: body checksum %08x, footer says %08x",
			ErrChecksum, got, f.CRC)
	}
	return body, f, nil
}

// Scan reads only the batch footers of a columnar file, skipping every body.
//
// This is what the columnar layout buys that a record log cannot: the row
// count, the time span and whether a file contains a gap are four bytes of
// length plus thirty-six bytes of footer per batch, with no decompression and
// no decoding of anything.
func Scan(r io.Reader) ([]Footer, error) {
	var out []Footer
	var lb [batchLenSize]byte
	for {
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, err
		}
		n := binary.LittleEndian.Uint32(lb[:])
		if n > MaxBatchBody {
			return out, fmt.Errorf("%w: %d bytes", ErrBatchTooBig, n)
		}
		if _, err := io.CopyN(io.Discard, r, int64(n)); err != nil {
			return out, fmt.Errorf("colfmt: truncated batch body: %w", unexpectedEOF(err))
		}
		var fb [FooterSize]byte
		if _, err := io.ReadFull(r, fb[:]); err != nil {
			return out, fmt.Errorf("colfmt: truncated batch footer: %w", unexpectedEOF(err))
		}
		f, err := decodeFooter(fb[:])
		if err != nil {
			return out, err
		}
		if f.BodyLen != n {
			return out, fmt.Errorf("%w: footer says %d body bytes, prefix says %d",
				ErrCorrupt, f.BodyLen, n)
		}
		out = append(out, f)
	}
}

// unexpectedEOF turns a stream that stopped mid-batch into an error that is not
// io.EOF. Only a clean batch boundary ends a file; a caller that treated a
// truncated batch as the end of the window would silently drop records a crash
// left half-written.
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// ColumnSize is one column's contribution to a file: what it holds before
// compression and what it costs on disk.
type ColumnSize struct {
	Name    string
	Raw     int64
	Encoded int64
}

// Profile is what a columnar file is made of, column by column. It is the
// measurement the compression ratio is reported from, and it comes from
// parsing a real file rather than from anything the encoder claimed.
type Profile struct {
	Batches []Footer
	Rows    int64

	// Columns is every column present, in column-id order.
	Columns []ColumnSize

	// Framing is the bytes that belong to no column: the file header, the
	// per-batch length prefixes and footers, and the per-block headers.
	Framing int64

	// Bytes is the whole file.
	Bytes int64

	// FrameBytes is the total size of the raw feed frames inside, which is what
	// a naive NDJSON store of the same window would have to write.
	FrameBytes int64
}

// ProfileFile measures one columnar file.
func ProfileFile(r io.Reader) (Profile, error) {
	br := bufio.NewReaderSize(r, bufSize)
	var h [tapefile.HeaderSize]byte
	if _, err := io.ReadFull(br, h[:]); err != nil {
		return Profile{}, tapefile.ErrBadMagic
	}
	v, err := tapefile.HeaderVersion(h[:])
	if err != nil {
		return Profile{}, err
	}
	if v != Version {
		return Profile{}, fmt.Errorf("%w: file is v%d, want v%d", tapefile.ErrBadVersion, v, Version)
	}

	p := Profile{Bytes: tapefile.HeaderSize, Framing: tapefile.HeaderSize}
	byID := map[uint8]*ColumnSize{}
	var ids []uint8
	for {
		body, f, err := readBatch(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return Profile{}, err
		}
		p.Batches = append(p.Batches, f)
		p.Rows += int64(f.Rows)
		p.Bytes += batchLenSize + int64(len(body)) + FooterSize
		p.Framing += batchLenSize + FooterSize

		infos, err := walkBlocks(body)
		if err != nil {
			return Profile{}, err
		}
		var payloads int64
		for _, bi := range infos {
			cs, ok := byID[bi.id]
			if !ok {
				cs = &ColumnSize{Name: columnName(bi.id)}
				byID[bi.id] = cs
				ids = append(ids, bi.id)
			}
			cs.Raw += int64(bi.rawLen)
			cs.Encoded += int64(bi.encLen)
			payloads += int64(bi.encLen)

			if bi.id == colFrames {
				n, err := sumFrames(bi)
				if err != nil {
					return Profile{}, err
				}
				p.FrameBytes += n
			}
		}
		// Whatever the body holds beyond the blocks' payloads is block framing.
		p.Framing += int64(len(body)) - payloads
	}
	sortIDs(ids)
	for _, id := range ids {
		p.Columns = append(p.Columns, *byID[id])
	}
	return p, nil
}

// sumFrames adds up the frames in one frames column, without their length
// prefixes: that total is exactly what a naive NDJSON store of the same window
// would have had to write, which is the denominator of the compression ratio.
func sumFrames(bi blockInfo) (int64, error) {
	raw := bi.payload
	if bi.codec == codecDeflate {
		var err error
		raw, err = decompressBlock(bi.payload, bi.rawLen)
		if err != nil {
			return 0, err
		}
	}
	var total int64
	c := &cursor{b: raw}
	for c.i < len(raw) {
		total += int64(len(c.bytes()))
		if c.err != nil {
			return 0, fmt.Errorf("%w: frames column: %v", ErrCorrupt, c.err)
		}
	}
	return total, nil
}

func sortIDs(ids []uint8) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

// OpenRecords opens a stored tape object of either format, choosing the reader
// by the version byte in its header and nothing else.
//
// It is the only place in the project that dispatches between the two formats.
// Each format's own reader still refuses the other outright, so a file that
// gets past this function has been identified twice.
func OpenRecords(rc io.ReadCloser) (tapefile.Records, error) {
	var h [tapefile.HeaderSize]byte
	if _, err := io.ReadFull(rc, h[:]); err != nil {
		rc.Close()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, tapefile.ErrBadMagic
		}
		return nil, err
	}
	v, err := tapefile.HeaderVersion(h[:])
	if err != nil {
		rc.Close()
		return nil, err
	}
	// The header goes back in front of the stream so that the reader that gets
	// it validates it itself rather than trusting this function's reading.
	whole := &rewound{r: io.MultiReader(bytes.NewReader(h[:]), rc), c: rc}
	switch v {
	case tapefile.Version:
		return tapefile.OpenReader(whole)
	case Version:
		return OpenReader(whole)
	default:
		rc.Close()
		return nil, fmt.Errorf("%w: file is v%d, this build reads v%d and v%d",
			tapefile.ErrBadVersion, v, tapefile.Version, Version)
	}
}

// rewound is a stream with its header put back on the front, closing over the
// original.
type rewound struct {
	r io.Reader
	c io.Closer
}

func (w *rewound) Read(p []byte) (int, error) { return w.r.Read(p) }
func (w *rewound) Close() error               { return w.c.Close() }
