package replay

import (
	"io"
	"testing"
	"time"
)

// The two numbers worth having are how fast records come out of the iterator
// and how that compares to the wall-clock time the window took to capture. The
// second is the one that matters to a backtest: a window that replays 200x
// faster than it happened is a window you can iterate on.
//
//	go test ./internal/replay -bench . -benchtime 20x

// BenchmarkReplay measures the iterator alone: read, decode, sort, deliver.
func BenchmarkReplay(b *testing.B) {
	root := fixtureWindow(b)
	var records, passes int64
	var span time.Duration

	b.ReportAllocs()
	for b.Loop() {
		r, err := Open(root, WithContinueOnGap())
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		for {
			_, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Next: %v", err)
			}
			records++
		}
		span = r.Stats().Span()
		if err := r.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		passes++
	}
	report(b, records, passes, span)
}

// BenchmarkReplayCanonical measures what `tape replay` actually does: the
// iterator plus canonical NDJSON serialization.
func BenchmarkReplayCanonical(b *testing.B) {
	root := fixtureWindow(b)
	var records, passes int64
	var span time.Duration

	b.ReportAllocs()
	for b.Loop() {
		r, err := Open(root, WithContinueOnGap())
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		enc := NewCanonicalEncoder(io.Discard)
		for {
			rec, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Next: %v", err)
			}
			if err := enc.Encode(rec); err != nil {
				b.Fatalf("Encode: %v", err)
			}
			records++
		}
		span = r.Stats().Span()
		if err := r.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		passes++
	}
	report(b, records, passes, span)
}

// report turns the raw counts into the two numbers the README quotes.
func report(b *testing.B, records, passes int64, span time.Duration) {
	b.Helper()
	if passes == 0 || b.Elapsed() <= 0 {
		return
	}
	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(records)/elapsed, "events/s")
	perPass := elapsed / float64(passes)
	if span > 0 && perPass > 0 {
		b.ReportMetric(span.Seconds()/perPass, "x_realtime")
	}
}

// BenchmarkReplayColumnar and BenchmarkReplayColumnarCanonical are the same two
// measurements over the same window stored in the columnar format. Against the
// raw pair they are what the compression ratio costs on the read side: the
// frames have to be inflated before anything can decode them.
func BenchmarkReplayColumnar(b *testing.B) {
	benchReplay(b, materialize(b, columnarFixtureDir), false)
}

func BenchmarkReplayColumnarCanonical(b *testing.B) {
	benchReplay(b, materialize(b, columnarFixtureDir), true)
}

func benchReplay(b *testing.B, root string, canonical bool) {
	b.Helper()
	var records, passes int64
	var span time.Duration

	b.ReportAllocs()
	for b.Loop() {
		r, err := Open(root, WithContinueOnGap())
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		enc := NewCanonicalEncoder(io.Discard)
		for {
			rec, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Next: %v", err)
			}
			if canonical {
				if err := enc.Encode(rec); err != nil {
					b.Fatalf("Encode: %v", err)
				}
			}
			records++
		}
		span = r.Stats().Span()
		if err := r.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		passes++
	}
	report(b, records, passes, span)
}
