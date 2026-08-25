package tapefile

import (
	"encoding/binary"
	"time"
)

// Message is a raw feed frame plus the local time it was received.
type Message struct {
	Recv time.Time
	Raw  []byte
}

// Gap records a sequence discontinuity: the sequence number that should have
// come next, the one that actually arrived, and when we noticed.
type Gap struct {
	At       time.Time
	Expected uint64
	Got      uint64
}

// Reseed records that a fresh subscription (and, for level2, a fresh snapshot)
// landed here. Everything after it is a new book, not a continuation.
type Reseed struct {
	At     time.Time
	Reason string
}

// EncodeMessage encodes a message payload. The raw frame is copied verbatim.
func EncodeMessage(m Message) []byte {
	b := make([]byte, 8+len(m.Raw))
	binary.LittleEndian.PutUint64(b[0:8], uint64(m.Recv.UnixNano()))
	copy(b[8:], m.Raw)
	return b
}

// DecodeMessage decodes a message payload. The returned Raw aliases p.
func DecodeMessage(p []byte) (Message, error) {
	if len(p) < 8 {
		return Message{}, ErrShortPayload
	}
	return Message{
		Recv: time.Unix(0, int64(binary.LittleEndian.Uint64(p[0:8]))).UTC(),
		Raw:  p[8:],
	}, nil
}

// EncodeGap encodes a gap payload.
func EncodeGap(g Gap) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], uint64(g.At.UnixNano()))
	binary.LittleEndian.PutUint64(b[8:16], g.Expected)
	binary.LittleEndian.PutUint64(b[16:24], g.Got)
	return b
}

// DecodeGap decodes a gap payload.
func DecodeGap(p []byte) (Gap, error) {
	if len(p) < 24 {
		return Gap{}, ErrShortPayload
	}
	return Gap{
		At:       time.Unix(0, int64(binary.LittleEndian.Uint64(p[0:8]))).UTC(),
		Expected: binary.LittleEndian.Uint64(p[8:16]),
		Got:      binary.LittleEndian.Uint64(p[16:24]),
	}, nil
}

// EncodeReseed encodes a reseed payload.
func EncodeReseed(r Reseed) []byte {
	b := make([]byte, 8+len(r.Reason))
	binary.LittleEndian.PutUint64(b[0:8], uint64(r.At.UnixNano()))
	copy(b[8:], r.Reason)
	return b
}

// DecodeReseed decodes a reseed payload.
func DecodeReseed(p []byte) (Reseed, error) {
	if len(p) < 8 {
		return Reseed{}, ErrShortPayload
	}
	return Reseed{
		At:     time.Unix(0, int64(binary.LittleEndian.Uint64(p[0:8]))).UTC(),
		Reason: string(p[8:]),
	}, nil
}
