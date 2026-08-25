// Package tapefile implements the on-disk capture format.
//
// A tape file is a small fixed header followed by length-prefixed records
// appended in arrival order. It is append-only by construction: the writer
// opens each file exactly once with O_APPEND and never seeks, and nothing in
// this package can rewrite bytes that were already flushed.
//
// Layout:
//
//	header  = magic[4] "TAPE" | version uint16 LE | reserved uint16 LE
//	record  = type uint8 | length uint32 LE | payload[length]
//
// Every record payload uses the same shape: a fixed-size prefix followed by an
// optional variable-length tail that runs to the end of the payload. That keeps
// decoding trivial and lets record types grow a tail without a format bump.
//
//	RecordMessage = recvUnixNano int64 LE | raw feed frame bytes
//	RecordGap     = atUnixNano   int64 LE | expected uint64 LE | got uint64 LE
//	RecordReseed  = atUnixNano   int64 LE | reason UTF-8 bytes
//
// Gap and reseed records are first-class members of the stream, not log lines.
// A replayer that reads a tape file cannot skip past a gap without seeing it.
//
// Files carry no product identifier: the path (data/{symbol}/{date}/{start}.tape)
// is the partition key, and a writer serves exactly one symbol.
package tapefile

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// HeaderSize is the size in bytes of the file header.
	HeaderSize = 8

	// Version is the format version written by this build. Readers refuse
	// anything they do not recognise rather than guessing at the layout.
	Version uint16 = 1

	// recordHeaderSize is type(1) + length(4).
	recordHeaderSize = 5

	// MaxPayload bounds a single record. A level2 snapshot for a busy product
	// is a few MiB; 32 MiB leaves room without letting a corrupt length field
	// allocate the machine to death.
	MaxPayload = 32 << 20
)

var magic = [4]byte{'T', 'A', 'P', 'E'}

// RecordType identifies what a record's payload means.
type RecordType uint8

const (
	// RecordMessage is a raw frame as received from the feed, with the local
	// receive timestamp. The raw bytes are stored verbatim: they are the only
	// thing that can settle an argument about what the exchange actually sent.
	RecordMessage RecordType = 1

	// RecordGap marks a detected sequence discontinuity. Its presence makes
	// the surrounding window untrustworthy; the public feed offers no backfill.
	RecordGap RecordType = 2

	// RecordReseed marks the point where a fresh subscription landed, so a
	// replayer knows the book was rebuilt rather than continuously updated.
	RecordReseed RecordType = 3
)

func (t RecordType) String() string {
	switch t {
	case RecordMessage:
		return "message"
	case RecordGap:
		return "gap"
	case RecordReseed:
		return "reseed"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// Errors returned when reading a malformed or unsupported file.
var (
	ErrBadMagic      = errors.New("tapefile: bad magic, not a tape file")
	ErrBadVersion    = errors.New("tapefile: unsupported format version")
	ErrPayloadTooBig = errors.New("tapefile: record payload exceeds maximum")
	ErrShortPayload  = errors.New("tapefile: record payload shorter than its fixed prefix")
)

// encodeHeader returns the file header bytes.
func encodeHeader() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:6], Version)
	binary.LittleEndian.PutUint16(b[6:8], 0) // reserved
	return b
}

// decodeHeader validates a file header and returns its version.
func decodeHeader(b []byte) (uint16, error) {
	if len(b) < HeaderSize {
		return 0, ErrBadMagic
	}
	if string(b[0:4]) != string(magic[:]) {
		return 0, ErrBadMagic
	}
	v := binary.LittleEndian.Uint16(b[4:6])
	if v != Version {
		return 0, fmt.Errorf("%w: file is v%d, this build reads v%d", ErrBadVersion, v, Version)
	}
	return v, nil
}
