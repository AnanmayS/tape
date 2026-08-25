package colfmt

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

// Block codecs. Stored is not a fallback, it is the answer for most columns: a
// bitset of two hundred bytes is not worth a deflate header, and a block is
// compressed only if compressing it actually made it smaller.
const (
	codecStored  uint8 = 0
	codecDeflate uint8 = 1
)

// compressLevel is deflate level 8, chosen by measurement rather than by
// default. On a real BTC-USD window, per level, ratio against the raw frames
// and single-core throughput:
//
//	level 6   5.78x   143 MB/s   (the "default" level)
//	level 7   5.99x    99 MB/s
//	level 8   6.22x    55 MB/s
//	level 9   6.47x    10 MB/s
//
// Everything up to 8 trades CPU for ratio smoothly; 9 is the deflate cliff,
// 5.5x the CPU for 4% more ratio. 55 MB/s is about 180 times the rate this
// feed produces, so 8 is the last level that is still free in practice.
//
// zstd was measured on the same bytes and did not earn a dependency: at
// comparable speed it compressed this data slightly worse than deflate does
// (6.00x at 141 MB/s for its "better" level against 5.99x at 99 MB/s for
// level 7), and JSON frames are exactly the input deflate was designed for.
// The numbers are in docs/columnar.md.
const compressLevel = 8

var flateWriters = sync.Pool{
	New: func() any {
		w, err := flate.NewWriter(io.Discard, compressLevel)
		if err != nil {
			panic("colfmt: " + err.Error()) // only a bad level can do this
		}
		return w
	},
}

var flateReaders = sync.Pool{
	New: func() any { return flate.NewReader(bytes.NewReader(nil)) },
}

func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// appendBlock encodes one column onto the body. An empty column is not written
// at all: absence is the cheapest possible encoding of nothing, and the decoder
// reads a missing block as an empty one.
func appendBlock(dst []byte, id uint8, raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return dst, nil
	}
	enc, codec, err := compressBlock(raw)
	if err != nil {
		return nil, err
	}
	dst = append(dst, id, codec)
	dst = binary.AppendUvarint(dst, uint64(len(raw)))
	dst = binary.AppendUvarint(dst, uint64(len(enc)))
	return append(dst, enc...), nil
}

func compressBlock(raw []byte) ([]byte, uint8, error) {
	var buf bytes.Buffer
	buf.Grow(len(raw) / 2)
	w := flateWriters.Get().(*flate.Writer)
	defer flateWriters.Put(w)
	w.Reset(&buf)
	if _, err := w.Write(raw); err != nil {
		return nil, 0, err
	}
	if err := w.Close(); err != nil {
		return nil, 0, err
	}
	if buf.Len() >= len(raw) {
		return raw, codecStored, nil
	}
	return buf.Bytes(), codecDeflate, nil
}

func decompressBlock(enc []byte, rawLen int) ([]byte, error) {
	r := flateReaders.Get().(io.ReadCloser)
	defer flateReaders.Put(r)
	if err := r.(flate.Resetter).Reset(bytes.NewReader(enc), nil); err != nil {
		return nil, err
	}
	out := make([]byte, rawLen)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, fmt.Errorf("colfmt: block decompresses short: %w", err)
	}
	// A block that decompresses to more than it claims is as wrong as one that
	// decompresses to less; both mean the length and the bytes disagree.
	var tail [1]byte
	if n, _ := r.Read(tail[:]); n != 0 {
		return nil, fmt.Errorf("colfmt: block decompresses longer than its %d bytes", rawLen)
	}
	return out, nil
}

// blockInfo is one column's framing, without its contents. It is what the
// measurement harness reads and what a reader walks.
type blockInfo struct {
	id      uint8
	codec   uint8
	rawLen  int
	encLen  int
	payload []byte // still encoded
}

// walkBlocks parses a body's framing without decompressing anything.
func walkBlocks(body []byte) ([]blockInfo, error) {
	var out []blockInfo
	c := &cursor{b: body}
	for c.i < len(body) {
		if c.i+2 > len(body) {
			return nil, fmt.Errorf("%w: truncated block header", ErrCorrupt)
		}
		id, codec := body[c.i], body[c.i+1]
		c.i += 2
		rawLen := c.uvarint()
		encLen := c.uvarint()
		if c.err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, c.err)
		}
		if rawLen > MaxBatchBody || encLen > uint64(len(body)-c.i) {
			return nil, fmt.Errorf("%w: block %d claims %d bytes encoded, %d raw",
				ErrCorrupt, id, encLen, rawLen)
		}
		if codec != codecStored && codec != codecDeflate {
			return nil, fmt.Errorf("%w: block %d has unknown codec %d", ErrCorrupt, id, codec)
		}
		if codec == codecStored && rawLen != encLen {
			return nil, fmt.Errorf("%w: stored block %d claims %d bytes for %d",
				ErrCorrupt, id, encLen, rawLen)
		}
		out = append(out, blockInfo{
			id: id, codec: codec,
			rawLen: int(rawLen), encLen: int(encLen),
			payload: body[c.i : c.i+int(encLen)],
		})
		c.i += int(encLen)
	}
	return out, nil
}

// splitBlocks decodes a body into its columns, decompressing each.
//
// A column id this build does not know is kept rather than rejected: a file
// written by a later build with an extra column still decodes, because every
// column this build does know is still exactly where it was.
func splitBlocks(body []byte) (map[uint8][]byte, error) {
	infos, err := walkBlocks(body)
	if err != nil {
		return nil, err
	}
	out := make(map[uint8][]byte, len(infos))
	for _, bi := range infos {
		if _, dup := out[bi.id]; dup {
			return nil, fmt.Errorf("%w: column %s appears twice", ErrCorrupt, columnName(bi.id))
		}
		if bi.codec == codecStored {
			out[bi.id] = bi.payload
			continue
		}
		raw, err := decompressBlock(bi.payload, bi.rawLen)
		if err != nil {
			return nil, err
		}
		out[bi.id] = raw
	}
	return out, nil
}

// cursor reads a decoded column. Every read is checked and a failed read is
// sticky, so a truncated column cannot hand back a plausible zero and let
// decoding carry on into the next column's bytes.
type cursor struct {
	b   []byte
	i   int
	err error
}

func (c *cursor) fail(what string) {
	if c.err == nil {
		c.err = fmt.Errorf("%s at offset %d of %d", what, c.i, len(c.b))
	}
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.i:])
	if n <= 0 {
		c.fail("truncated varint")
		return 0
	}
	c.i += n
	return v
}

func (c *cursor) varint() int64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Varint(c.b[c.i:])
	if n <= 0 {
		c.fail("truncated varint")
		return 0
	}
	c.i += n
	return v
}

// bytes reads a length-prefixed run. The result aliases the column, which is
// why every caller either copies it or hands it straight into a payload that
// does.
func (c *cursor) bytes() []byte {
	n := c.uvarint()
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.b)-c.i) {
		c.fail(fmt.Sprintf("run of %d bytes", n))
		return nil
	}
	s := c.b[c.i : c.i+int(n)]
	c.i += int(n)
	return s
}

// done reports whether the column was consumed exactly.
func (c *cursor) done() error {
	if c.err != nil {
		return c.err
	}
	if c.i != len(c.b) {
		return fmt.Errorf("%d of %d bytes left unread", len(c.b)-c.i, len(c.b))
	}
	return nil
}
