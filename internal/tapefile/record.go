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

// Gap records a discontinuity in the stream: the sequence number that should
// have come next, the one that actually arrived, and when we noticed.
//
// It is also what a frame this process itself discarded is recorded as. There
// is one record type for "the window is missing messages here" on purpose: a
// second one would need every existing check — replay stopping, verify exiting
// non-zero, the CloudWatch gap alarm at threshold zero — to be taught about it,
// and the check that was not taught is a silent gap. Dropped says which cause
// it was.
type Gap struct {
	At       time.Time
	Expected uint64
	Got      uint64

	// Dropped is how many frames capture itself discarded here, under a
	// backpressure policy that sheds load. Zero for a gap the exchange's
	// sequence numbers revealed, where Expected and Got carry the count.
	//
	// The two cannot be merged into one number. A sequence gap knows exactly
	// which messages are missing and nothing about why; a drop knows exactly
	// how many and exactly whose fault it was, and — on a monotonic feed, where
	// a skip proves nothing — is invisible to the sequence numbers entirely.
	Dropped uint64
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

// EncodeGap encodes a gap payload. The drop count is the record's optional
// tail, written only when there is one, so a sequence gap encodes to exactly
// the bytes it always did and every file captured before this existed stays
// byte-identical to what it would be captured as today.
func EncodeGap(g Gap) []byte {
	n := 24
	if g.Dropped != 0 {
		n = 32
	}
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b[0:8], uint64(g.At.UnixNano()))
	binary.LittleEndian.PutUint64(b[8:16], g.Expected)
	binary.LittleEndian.PutUint64(b[16:24], g.Got)
	if g.Dropped != 0 {
		binary.LittleEndian.PutUint64(b[24:32], g.Dropped)
	}
	return b
}

// DecodeGap decodes a gap payload. A payload with no tail is a gap with no
// drops, which is what every record written before the tail existed means.
func DecodeGap(p []byte) (Gap, error) {
	if len(p) < 24 {
		return Gap{}, ErrShortPayload
	}
	g := Gap{
		At:       time.Unix(0, int64(binary.LittleEndian.Uint64(p[0:8]))).UTC(),
		Expected: binary.LittleEndian.Uint64(p[8:16]),
		Got:      binary.LittleEndian.Uint64(p[16:24]),
	}
	if len(p) >= 32 {
		g.Dropped = binary.LittleEndian.Uint64(p[24:32])
	}
	return g, nil
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
