package feed

import (
	"math/rand/v2"
	"time"
)

// Reconnect timing. A public feed that has just dropped us is often dropping
// everyone, so the delay grows and carries jitter to avoid marching back in
// lockstep with every other client.
const (
	backoffBase = 500 * time.Millisecond
	backoffMax  = 30 * time.Second

	// stableFor is how long a connection must survive before the backoff is
	// considered settled. Without it, a socket that accepts and immediately
	// dies would reset the delay every time and produce a hot reconnect loop.
	stableFor = 60 * time.Second
)

// backoff produces exponentially growing delays with jitter.
type backoff struct {
	base, max time.Duration
	attempt   int

	// jitter returns a fraction in [0,1). Tests replace it to get a
	// deterministic sequence.
	jitter func() float64
}

func newBackoff(base, max time.Duration) *backoff {
	return &backoff{base: base, max: max, jitter: rand.Float64}
}

// next returns the delay before the next attempt and advances the sequence.
// The delay is drawn from the upper half of the current window: jitter spreads
// clients out without ever collapsing the delay to nothing.
func (b *backoff) next() time.Duration {
	d := b.base
	for i := 0; i < b.attempt && d < b.max; i++ {
		d *= 2
	}
	if d > b.max {
		d = b.max
	}
	b.attempt++
	half := d / 2
	return half + time.Duration(b.jitter()*float64(half))
}

// reset returns the sequence to its first delay.
func (b *backoff) reset() { b.attempt = 0 }
