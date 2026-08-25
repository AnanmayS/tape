package capture

import (
	"math/rand/v2"
	"testing"
	"time"
)

// Buckets must tile the range: every value lands in a bucket whose upper bound
// is at least the value, and no wider than the promised 12.5%. A histogram that
// can put a value in the wrong bucket reports a quantile that never happened.
func TestHistBucketsTileTheRange(t *testing.T) {
	values := []int64{0, 1, 7, 8, 9, 15, 16, 17, 1_000, 999_999, 1 << 40, (1 << 62) - 1}
	for i := 0; i < 5000; i++ {
		values = append(values, rand.Int64N(1<<45))
	}
	for _, v := range values {
		i := histIndex(v)
		if i < 0 || i >= histBuckets {
			t.Fatalf("%d landed in bucket %d, outside 0..%d", v, i, histBuckets-1)
		}
		upper := histUpper(i)
		if upper < v {
			t.Fatalf("%d landed in bucket %d whose upper bound is %d", v, i, upper)
		}
		if i > 0 && histUpper(i-1) >= v {
			t.Fatalf("%d landed in bucket %d but bucket %d already covers it", v, i, i-1)
		}
		if v >= histSub && upper > v+v/histSub+1 {
			t.Fatalf("bucket %d is %d wide for %d, more than the promised eighth", i, upper-v, v)
		}
	}
}

func TestHistQuantiles(t *testing.T) {
	var h hist
	for i := 1; i <= 1000; i++ {
		h.observe(time.Duration(i) * time.Microsecond)
	}

	l := h.summary()
	if l.Count != 1000 {
		t.Fatalf("Count = %d, want 1000", l.Count)
	}
	if want := 500500 * time.Microsecond / 1000; l.Mean != want {
		t.Fatalf("Mean = %s, want %s", l.Mean, want)
	}
	if l.Max != time.Millisecond {
		t.Fatalf("Max = %s, want 1ms", l.Max)
	}
	// Each quantile is the top of the bucket the value fell in, so it is at or
	// above the true value and within an eighth of it.
	for _, c := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"p50", l.P50, 500 * time.Microsecond},
		{"p90", l.P90, 900 * time.Microsecond},
		{"p99", l.P99, 990 * time.Microsecond},
		{"p99.9", l.P999, 999 * time.Microsecond},
	} {
		if c.got < c.want || c.got > c.want+c.want/histSub+time.Microsecond {
			t.Fatalf("%s = %s, want %s or just above", c.name, c.got, c.want)
		}
	}
	if l.P50 > l.P90 || l.P90 > l.P99 || l.P99 > l.P999 || l.P999 > l.Max {
		t.Fatalf("quantiles are not monotone: %s", l)
	}
}

// No observation is not an observation of zero.
func TestHistEmpty(t *testing.T) {
	var h hist
	if got := h.summary(); got.Count != 0 || got.Max != 0 || got.String() != "none" {
		t.Fatalf("an unused histogram summarises as %+v", got)
	}
}

// A quantile must never exceed a latency that actually happened, even though
// the bucket it lands in is wider than the value.
func TestHistNeverReportsAboveTheMaximum(t *testing.T) {
	var h hist
	h.observe(9 * time.Microsecond)
	l := h.summary()
	if l.P999 != 9*time.Microsecond || l.Max != 9*time.Microsecond {
		t.Fatalf("single observation summarised as %s", l)
	}
}
