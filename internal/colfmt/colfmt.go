// Package colfmt implements the v2 on-disk capture format: delta-encoded
// columnar batches.
//
// It stores exactly the records package tapefile stores — messages, gaps and
// reseeds — and hands them back as byte-identical payloads. A v2 file is a v1
// file's bytes, rearranged so that repetition lands next to repetition and
// compressed.
//
// # What is stored, and what is truth
//
// The raw feed frame is kept, verbatim, for every message. That is the point of
// the whole capture: the frame is the only thing that can settle an argument
// about what the exchange actually sent, and a format that threw it away to win
// a compression ratio would be trading the project's purpose for a number. So
// the frames are stored in a column of their own and compressed as a block, and
// the structured fields — timestamps, sequence, side, price, size, message type
// — are delta-encoded into columns beside them.
//
// The columns are an index, not the record. Replay decodes the frames, exactly
// as it does for a v1 file, so a v2 window cannot replay differently from the
// v1 window holding the same frames: the reader hands the replay layer the same
// bytes either way. The columns exist so that a scan — which windows hold
// trades above X, where does this sequence range live — can read a few tens of
// kilobytes instead of decompressing megabytes of JSON. They cost about 6% of
// the file; the measurement is in docs/columnar.md.
//
// Reconstructing the frames from the columns instead of storing them was the
// alternative, and it would roughly double the ratio. It is not done because
// byte-exact reconstruction of Coinbase's JSON is not provable: field order,
// key set and number formatting are the exchange's to change without notice,
// and a format that re-renders a frame is a format that can hand back something
// the exchange never sent while looking entirely healthy.
//
// # Layout
//
//	file    = header | batch*
//	header  = magic[4] "TAPE" | version uint16 LE (=2) | reserved uint16 LE
//	batch   = bodyLen uint32 LE | body[bodyLen] | footer[FooterSize]
//	body    = block*
//	block   = id uint8 | codec uint8 | rawLen uvarint | encLen uvarint | data[encLen]
//	footer  = magic[4] "TAPB" | version uint16 LE | flags uint16 LE | rows uint32 LE
//	        | minRecv int64 LE | maxRecv int64 LE | bodyLen uint32 LE | crc32c uint32 LE
//
// The footer is fixed-size and last, and repeats the body length, so a reader
// that wants only the summary reads four bytes, skips the body and reads
// thirty-six more — no decompression, no decoding. Scan does exactly that.
//
// The CRC covers the body. It is here because the failure mode of a
// delta-encoded format is not a crash: a single corrupted varint shifts every
// subsequent value in its column and produces prices that are wrong and
// entirely plausible. A checksum turns that into an error.
//
// A batch is self-contained: every delta chain starts from zero at the top of a
// batch, so a batch can be decoded without reading the ones before it, and a
// file can be appended to after a crash without the new bytes depending on the
// old ones.
//
// # Version
//
// The version byte is checked before anything else. This package's reader
// refuses v1 and package tapefile's reader refuses v2, so neither format can
// ever be read as the other; OpenRecords is the one place that dispatches, and
// it dispatches on that byte alone.
package colfmt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Version is the format version this package reads and writes.
const Version uint16 = 2

// FooterSize is the size in bytes of a batch footer.
const FooterSize = 36

// batchLenSize is the uint32 length prefix in front of a batch body.
const batchLenSize = 4

// MaxBatchBody bounds a single batch body. A batch is at most DefaultMaxRows
// records of a few kilobytes each; 64 MiB leaves room for a level2 snapshot in
// the middle of one without letting a corrupt length field allocate the machine
// to death.
const MaxBatchBody = 64 << 20

// MaxRows bounds the row count a footer may claim, for the same reason.
const MaxRows = 1 << 24

// Batch sizing. Bigger batches compress better and cost more memory to decode,
// since a batch is decoded whole. 4096 rows of BTC-USD is about 2.7 MB of
// frames, which is the same order as the replay reorder buffer already holds.
const (
	// DefaultMaxRows is how many records a batch holds before it is flushed.
	DefaultMaxRows = 4096

	// DefaultMaxBytes caps a batch by the frame bytes in it, so that a burst of
	// large frames — a level2 snapshot is over a megabyte — does not build a
	// batch nobody can afford to decode.
	DefaultMaxBytes = 4 << 20
)

// Footer flags.
const (
	// FlagGap means the batch contains at least one gap record: somewhere in
	// these rows, sequence numbers prove messages were lost.
	FlagGap uint16 = 1 << 0

	// FlagReseed means the batch contains at least one reseed record.
	FlagReseed uint16 = 1 << 1
)

var footerMagic = [4]byte{'T', 'A', 'P', 'B'}

// crcTable is Castagnoli, which has hardware support on every CPU this runs on.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Errors returned when reading a malformed file.
var (
	ErrBadFooter   = errors.New("colfmt: batch footer is not a batch footer")
	ErrChecksum    = errors.New("colfmt: batch body fails its checksum")
	ErrBatchTooBig = errors.New("colfmt: batch exceeds the maximum size")
	ErrCorrupt     = errors.New("colfmt: batch is internally inconsistent")
)

// Footer summarises one batch. Every field is written by the encoder from the
// rows it encoded, so a reader can trust it only as far as the checksum: the
// counts describe the body the CRC covers.
type Footer struct {
	// Version is the format version of the batch.
	Version uint16

	// Flags is the OR of FlagGap and FlagReseed. A window scan that wants to
	// know whether a file is trustworthy reads these and stops.
	Flags uint16

	// Rows is the number of records in the batch.
	Rows uint32

	// MinRecv and MaxRecv bound the batch on the local receive clock — the one
	// timestamp every record kind carries.
	MinRecv, MaxRecv int64

	// BodyLen is the encoded body size in bytes, repeated from the length
	// prefix so that a footer locates its own body.
	BodyLen uint32

	// CRC is Castagnoli over the body.
	CRC uint32
}

// HasGap reports whether the batch contains a gap record.
func (f Footer) HasGap() bool { return f.Flags&FlagGap != 0 }

// HasReseed reports whether the batch contains a reseed record.
func (f Footer) HasReseed() bool { return f.Flags&FlagReseed != 0 }

// encode writes the footer.
func (f Footer) encode() []byte {
	b := make([]byte, FooterSize)
	copy(b[0:4], footerMagic[:])
	binary.LittleEndian.PutUint16(b[4:6], f.Version)
	binary.LittleEndian.PutUint16(b[6:8], f.Flags)
	binary.LittleEndian.PutUint32(b[8:12], f.Rows)
	binary.LittleEndian.PutUint64(b[12:20], uint64(f.MinRecv))
	binary.LittleEndian.PutUint64(b[20:28], uint64(f.MaxRecv))
	binary.LittleEndian.PutUint32(b[28:32], f.BodyLen)
	binary.LittleEndian.PutUint32(b[32:36], f.CRC)
	return b
}

// decodeFooter reads a footer, refusing one that does not identify itself.
func decodeFooter(b []byte) (Footer, error) {
	if len(b) < FooterSize {
		return Footer{}, ErrBadFooter
	}
	if string(b[0:4]) != string(footerMagic[:]) {
		return Footer{}, ErrBadFooter
	}
	f := Footer{
		Version: binary.LittleEndian.Uint16(b[4:6]),
		Flags:   binary.LittleEndian.Uint16(b[6:8]),
		Rows:    binary.LittleEndian.Uint32(b[8:12]),
		MinRecv: int64(binary.LittleEndian.Uint64(b[12:20])),
		MaxRecv: int64(binary.LittleEndian.Uint64(b[20:28])),
		BodyLen: binary.LittleEndian.Uint32(b[28:32]),
		CRC:     binary.LittleEndian.Uint32(b[32:36]),
	}
	if f.Version != Version {
		return Footer{}, fmt.Errorf("%w: batch is v%d, this build reads v%d",
			ErrBadFooter, f.Version, Version)
	}
	if f.Rows > MaxRows {
		return Footer{}, fmt.Errorf("%w: footer claims %d rows", ErrCorrupt, f.Rows)
	}
	return f, nil
}

// bitset is one bit per row, LSB first within each byte. Presence — does this
// record have a sequence number, a price, a side — is a column of its own, so
// that absence costs a bit rather than a sentinel value that could be mistaken
// for data.
type bitset []byte

func newBitset(rows int) bitset { return make(bitset, (rows+7)/8) }

func (b bitset) set(i int) { b[i/8] |= 1 << uint(i%8) }

func (b bitset) get(i int) bool {
	if i/8 >= len(b) {
		return false
	}
	return b[i/8]&(1<<uint(i%8)) != 0
}

// any reports whether any bit is set, which is how an all-absent column is
// dropped from the file entirely.
func (b bitset) any() bool {
	for _, v := range b {
		if v != 0 {
			return true
		}
	}
	return false
}

// zigzag maps signed deltas onto unsigned values so that a small negative delta
// costs one varint byte rather than ten. Timestamps and prices go both ways.
func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func unzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }
