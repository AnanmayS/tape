package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/AnanmayS/tape/internal/replay"
)

func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	out := fs.String("o", "", "write NDJSON to this file instead of stdout")
	continueOnGap := fs.Bool("continue-on-gap", false,
		"read past gaps and reconnects instead of stopping; they are still emitted")
	reorder := fs.Int("reorder", replay.DefaultReorderWindow,
		"records held in the reorder buffer while sorting")
	quiet := fs.Bool("quiet", false, "do not print the summary on stderr")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape replay [flags] <window>\n\n"+
				"Replays a window — a directory of .tape files, or one file — to stdout as\n"+
				"canonical NDJSON, one object per record, in the fixed total order documented\n"+
				"in docs/replay.md. The same window always produces the same bytes.\n\n"+
				"Replay stops at a gap or a reconnect unless -continue-on-gap is given; either\n"+
				"way the gap and reseed records are part of the output.\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("replay takes exactly one window")
	}

	opts := []replay.Option{replay.WithReorderWindow(*reorder)}
	if *continueOnGap {
		opts = append(opts, replay.WithContinueOnGap())
	}
	r, err := replay.Open(fs.Arg(0), opts...)
	if err != nil {
		return err
	}
	defer r.Close()

	w := io.Writer(os.Stdout)
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriterSize(w, 256<<10)
	enc := replay.NewCanonicalEncoder(bw)

	started := time.Now()
	replayErr := drain(r, func(rec replay.Record) error { return enc.Encode(rec) })
	elapsed := time.Since(started)

	if err := bw.Flush(); err != nil {
		return err
	}
	if !*quiet {
		printSummary(os.Stderr, r, elapsed)
	}
	if errors.Is(replayErr, replay.ErrDiscontinuity) {
		return fmt.Errorf("%w\n       pass -continue-on-gap to read past it; "+
			"the gap and reseed records stay in the output either way", replayErr)
	}
	return replayErr
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	reorder := fs.Int("reorder", replay.DefaultReorderWindow,
		"records held in the reorder buffer while sorting")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape verify [flags] <window>\n\n"+
				"Reads a window end to end without writing it out, and reports what is in it:\n"+
				"counts, the SHA-256 of its canonical NDJSON, every gap and reconnect, and\n"+
				"replay throughput against the wall-clock time the window took to capture.\n\n"+
				"It exits non-zero if the window is discontinuous. A window with a gap in it\n"+
				"is untrustworthy — the public feed offers no backfill — and a verifier that\n"+
				"returned success on one would be worse than no verifier.\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("verify takes exactly one window")
	}

	// Verification reads the whole window on purpose: stopping at the first
	// discontinuity would report one gap and hide the rest.
	r, err := replay.Open(fs.Arg(0), replay.WithReorderWindow(*reorder), replay.WithContinueOnGap())
	if err != nil {
		return err
	}
	defer r.Close()

	h := sha256.New()
	enc := replay.NewCanonicalEncoder(h)
	started := time.Now()
	replayErr := drain(r, func(rec replay.Record) error { return enc.Encode(rec) })
	elapsed := time.Since(started)

	printSummary(os.Stdout, r, elapsed)
	fmt.Fprintf(os.Stdout, "  digest      sha256:%s\n", hex.EncodeToString(h.Sum(nil)))
	for _, d := range r.Discontinuities() {
		fmt.Fprintf(os.Stdout, "  ! %s\n", d)
	}
	if replayErr != nil {
		return replayErr
	}
	if n := len(r.Discontinuities()); n > 0 {
		return fmt.Errorf("window is untrustworthy: %d discontinuit%s",
			n, plural(n, "y", "ies"))
	}
	return nil
}

// drain pumps every record through fn, returning nil at a clean end of window.
func drain(r *replay.Reader, fn func(replay.Record) error) error {
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// printSummary reports what the replay saw. Every number is counted.
func printSummary(w io.Writer, r *replay.Reader, elapsed time.Duration) {
	st := r.Stats()
	fmt.Fprintf(w, "window %s\n", r.Root())
	fmt.Fprintf(w, "  files       %d\n", len(r.Files()))
	fmt.Fprintf(w, "  records     %d (%d messages, %d gaps, %d reseeds)\n",
		st.Records, st.Messages, st.Gaps, st.Reseeds)
	fmt.Fprintf(w, "  bytes       %d\n", st.Bytes)
	fmt.Fprintf(w, "  span        %s (%s to %s)\n",
		st.Span().Round(time.Millisecond),
		st.FirstTime.Format(time.RFC3339), st.LastTime.Format(time.RFC3339))
	fmt.Fprintf(w, "  replayed in %s\n", elapsed.Round(time.Microsecond))
	if s := elapsed.Seconds(); s > 0 {
		fmt.Fprintf(w, "  throughput  %.0f events/sec", float64(st.Records)/s)
		if st.Span() > 0 {
			fmt.Fprintf(w, ", %.0fx wall-clock", st.Span().Seconds()/s)
		}
		fmt.Fprintln(w)
	}
	if r.Trustworthy() {
		fmt.Fprintf(w, "  continuity  intact\n")
	} else {
		fmt.Fprintf(w, "  continuity  BROKEN — %d discontinuit%s\n",
			len(r.Discontinuities()), plural(len(r.Discontinuities()), "y", "ies"))
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
