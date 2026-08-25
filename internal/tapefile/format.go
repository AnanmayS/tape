// Package tapefile implements the v1 on-disk capture format.
//
// A v1 tape file is a small fixed header followed by length-prefixed records
// appended in arrival order. It is append-only by construction: the writer
// opens each file exactly once with O_APPEND and never seeks, and nothing in
// this package can rewrite bytes that were already flushed.
//
// Layout:
//
//	header  = magic[4] "TAPE" | version uint16 LE | reserved uint16 LE
//	record  = type uint8 | length uint32 LE | payload[length]
//
// # Two formats, one version byte
//
// The delta-encoded columnar format is version 2 and lives in package colfmt.
// It shares this header, this record vocabulary and this package's Rotator, and
// differs only in what sits between the header and the end of the file.
//
// The reader in this package reads v1 and refuses everything else, by version
// byte, before it interprets a single byte of the body — which is the whole
// reason a version byte is written at all. A v2 file cannot be misread as a v1
// one, and TestReaderRefusesUnknownVersion is that guarantee written down.
// Callers that want either format ask colfmt.OpenRecords, which dispatches on
// the version byte and hands back the right reader.
//
// Every record payload uses the same shape: a fixed-size prefix followed by an
// optional variable-length tail that runs to the end of the payload. That keeps
// decoding trivial and lets record types grow a tail without a format bump.
//
//	RecordMessage = recvUnixNano int64 LE | raw feed frame bytes
//	RecordGap     = atUnixNano   int64 LE | expected uint64 LE | got uint64 LE
//	                [| dropped uint64 LE]
//	RecordReseed  = atUnixNano   int64 LE | reason UTF-8 bytes
//
// The gap record's drop count is that tail in practice: it appears only on a
// gap caused by capture shedding load, so a sequence gap is the same 24 bytes
// it has always been and no file needs rewriting.
//
// Gap and reseed records are first-class members of the stream, not log lines.
// A replayer that reads a tape file cannot skip past a gap without seeing it.
//
// Files carry no product identifier: the key
// (v1/symbol={symbol}/date={date}/hour={hour}/{start}.tape) is the partition,
// and a writer serves exactly one symbol. That key is the file's path under the
// local root and its object key in the store, which is one layout rather than
// two — see the storage package.
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

// Records is a stored record stream: the records of one tape file, in stored
// order, as the (type, payload) pairs this package's vocabulary is written in.
//
// Both on-disk formats present one, and they present the same one — a v2 file
// hands back byte-identical payloads to the v1 file holding the same records.
// That is what lets everything above this line be written once.
type Records interface {
	// Next returns the next record, or io.EOF at a clean end of file. The
	// payload is owned by the caller.
	Next() (RecordType, []byte, error)

	// Version is the format version of the file being read.
	Version() uint16

	// Close releases the underlying stream.
	Close() error
}

// EncodeHeader returns the file header bytes for format version v. The magic is
// shared by every format so that "is this a tape file" and "which format is it"
// stay two separate questions with two separate answers.
func EncodeHeader(v uint16) []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:6], v)
	binary.LittleEndian.PutUint16(b[6:8], 0) // reserved
	return b
}

// HeaderVersion validates the magic on a header and returns the version it
// declares, without judging whether that version is one this package can read.
// It is what a dispatcher calls before choosing a reader.
func HeaderVersion(b []byte) (uint16, error) {
	if len(b) < HeaderSize || string(b[0:4]) != string(magic[:]) {
		return 0, ErrBadMagic
	}
	return binary.LittleEndian.Uint16(b[4:6]), nil
}

// encodeHeader returns the v1 file header bytes.
func encodeHeader() []byte { return EncodeHeader(Version) }

// decodeHeader validates a file header and returns its version. It accepts v1
// and nothing else: a reader that guessed at a body it does not know the layout
// of is the failure this byte exists to prevent.
func decodeHeader(b []byte) (uint16, error) {
	v, err := HeaderVersion(b)
	if err != nil {
		return 0, err
	}
	if v != Version {
		return 0, fmt.Errorf("%w: file is v%d, this build reads v%d", ErrBadVersion, v, Version)
	}
	return v, nil
}
