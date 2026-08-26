package termui

import (
	"testing"
	"time"
)

func TestCount(t *testing.T) {
	cases := map[int64]string{
		0:          "0",
		7:          "7",
		999:        "999",
		1000:       "1,000",
		12345:      "12,345",
		1234567:    "1,234,567",
		1000000000: "1,000,000,000",
		-4321:      "-4,321",
	}
	for in, want := range cases {
		if got := Count(in); got != want {
			t.Errorf("Count(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestBytes(t *testing.T) {
	cases := map[int64]string{
		0:       "0 B",
		512:     "512 B",
		1023:    "1023 B",
		1024:    "1.00 KiB",
		1536:    "1.50 KiB",
		10240:   "10.0 KiB",
		1048576: "1.00 MiB",
		1468006: "1.40 MiB",
	}
	for in, want := range cases {
		if got := Bytes(in); got != want {
			t.Errorf("Bytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestElapsedDoesNotChangeWidthEverySecond(t *testing.T) {
	cases := map[time.Duration]string{
		-time.Second:            "0:00:00",
		0:                       "0:00:00",
		1500 * time.Millisecond: "0:00:01",
		83 * time.Second:        "0:01:23",
		time.Hour + 2*time.Minute + 3*time.Second: "1:02:03",
	}
	for in, want := range cases {
		if got := Elapsed(in); got != want {
			t.Errorf("Elapsed(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestRate(t *testing.T) {
	cases := map[float64]string{
		0:    "0/s",
		-1:   "0/s",
		1.24: "1.2/s",
		63.4: "63/s",
		1e5:  "100000/s",
	}
	for in, want := range cases {
		if got := Rate(in); got != want {
			t.Errorf("Rate(%v) = %q, want %q", in, got, want)
		}
	}
}
