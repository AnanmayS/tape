package capture

import (
	"fmt"
	"math"
	"math/bits"
	"time"
)

// Write latency is the second half of the backpressure picture. Queue depth
// says the writer fell behind; this says by how much and how often, which is
// the difference between "a batch flush cost 8 ms once" and "every write costs
// 8 ms". An average hides exactly that, because the interesting write is the
// rare one: the record that closed a columnar batch pays for encoding and
// compressing four thousand rows, and it is the only record that does.
//
// The histogram is a bucketed one with 3 bits of mantissa — every bucket is at
// most 12.5% wider than the value it holds, so a quantile is accurate to about
// an eighth of itself, which is far finer than anything a decision here turns
// on. It costs a 512-entry array and one branch-free index computation per
// write, and it is always on: a number that only exists when a flag was passed
// is a number nobody has when they need it.
const (
	histSubBits = 3
	histSub     = 1 << histSubBits

	// histBuckets covers every non-negative int64 nanosecond value: the largest
	// index a duration can produce is 487.
	histBuckets = 512
)

// hist is a bucketed latency histogram. It is written by the capture writer
// goroutine and read after that goroutine has stopped, so it needs no lock.
type hist struct {
	buckets [histBuckets]int64
	count   int64
	sum     int64 // nanoseconds
	max     int64
}

func (h *hist) observe(d time.Duration) {
	v := int64(d)
	if v < 0 {
		// A clock that went backwards is not a negative latency. Record it as
		// the zero it is closest to rather than corrupting the sum.
		v = 0
	}
	h.count++
	h.sum += v
	if v > h.max {
		h.max = v
	}
	h.buckets[histIndex(v)]++
}

// histIndex is the bucket for v: the position of its high bit, plus the three
// bits below it. Values under histSub are their own buckets, which makes the
// bottom of the range exact.
func histIndex(v int64) int {
	if v < histSub {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1
	m := int(v>>uint(e-histSubBits)) & (histSub - 1)
	return (e-histSubBits+1)*histSub + m
}

// histUpper is the largest value bucket i holds.
func histUpper(i int) int64 {
	if i < histSub {
		return int64(i)
	}
	e := i/histSub + histSubBits - 1
	m := i % histSub
	return int64(histSub+m+1)<<uint(e-histSubBits) - 1
}

// quantile returns the upper bound of the bucket the q-th value falls in,
// capped at the exact maximum so a reported p99.9 is never larger than a
// latency that actually happened.
func (h *hist) quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	want := int64(math.Ceil(q * float64(h.count)))
	if want < 1 {
		want = 1
	}
	var cum int64
	for i := range h.buckets {
		cum += h.buckets[i]
		if cum >= want {
			if u := histUpper(i); u < h.max {
				return time.Duration(u)
			}
			break
		}
	}
	return time.Duration(h.max)
}

func (h *hist) summary() Latency {
	if h.count == 0 {
		return Latency{}
	}
	return Latency{
		Count: h.count,
		Mean:  time.Duration(h.sum / h.count),
		P50:   h.quantile(0.50),
		P90:   h.quantile(0.90),
		P99:   h.quantile(0.99),
		P999:  h.quantile(0.999),
		Max:   time.Duration(h.max),
	}
}

// Latency is the distribution of per-record write times over a session. Every
// field is measured; an unwritten session reports a zero Count and nothing
// else, because no observation is not an observation of zero.
type Latency struct {
	Count               int64
	Mean                time.Duration
	P50, P90, P99, P999 time.Duration
	Max                 time.Duration
}

// String renders the distribution for a log line, in the order a reader wants
// it: what a typical write cost, then the tail that decides whether the queue
// can drain.
func (l Latency) String() string {
	if l.Count == 0 {
		return "none"
	}
	return fmt.Sprintf("mean %s p50 %s p90 %s p99 %s p99.9 %s max %s",
		round(l.Mean), round(l.P50), round(l.P90), round(l.P99), round(l.P999), round(l.Max))
}

// round trims a duration to three significant figures' worth of unit, so a log
// line reads "4.6µs" rather than "4.601µs".
func round(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(time.Microsecond)
	case d >= time.Microsecond:
		return d.Round(10 * time.Nanosecond)
	default:
		return d
	}
}
