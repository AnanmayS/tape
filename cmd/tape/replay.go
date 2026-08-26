package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/AnanmayS/tape/internal/replay"
	"github.com/AnanmayS/tape/internal/termui"
)

func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	out := fs.String("o", "", "write NDJSON to this file instead of stdout")
	continueOnGap := fs.Bool("continue-on-gap", false,
		"read past gaps and reconnects instead of stopping; they are still emitted")
	reorder := fs.Int("reorder", replay.DefaultReorderWindow,
		"records held in the reorder buffer while sorting")
	quiet := fs.Bool("quiet", false, "do not print the summary on stderr")
	pretty := fs.Bool("pretty", false,
		"write a readable, coloured event stream instead of canonical NDJSON")
	var store storeFlags
	store.register(fs, "replay from")
	var term termFlags
	term.register(fs)

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape replay [flags] <window>\n\n"+
				"Replays a window to stdout as canonical NDJSON, one object per record, in the\n"+
				"fixed total order documented in docs/replay.md. The same window always\n"+
				"produces the same bytes, and a window replayed from a bucket produces the\n"+
				"same bytes as the same window replayed from disk.\n\n"+
				"A window is a local directory of .tape files, or one .tape file. With\n"+
				"-s3-bucket it is instead a key prefix, e.g.\n"+
				"  v1/symbol=BTC-USD/date=2026-08-25\n"+
				"and objects are streamed as they are read rather than downloaded first.\n\n"+
				"Replay stops at a gap or a reconnect unless -continue-on-gap is given; either\n"+
				"way the gap and reseed records are part of the output.\n\n"+
				"-pretty writes a different stream: aligned, coloured rows meant to be read\n"+
				"rather than parsed, with gaps and reseeds as full-width banners you cannot\n"+
				"scroll past. It is a separate encoder, and the default output — the canonical\n"+
				"NDJSON the determinism digest is taken over — is not touched by it or by any\n"+
				"other flag here.\n\nflags:\n")
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
	r, err := openWindow(context.Background(), &store, fs.Arg(0), opts...)
	if err != nil {
		return err
	}
	defer r.Close()

	w := io.Writer(os.Stdout)
	caps := term.caps(os.Stdout)
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
		// A file is not a terminal, whatever stdout happens to be.
		caps = caps.Plain()
	}
	bw := bufio.NewWriterSize(w, 256<<10)

	started := time.Now()
	var replayErr error
	if *pretty {
		replayErr = prettyReplay(bw, r, caps)
	} else {
		// The canonical path. Nothing configurable reaches it: the same window
		// produces the same bytes it produced before this flag existed.
		enc := replay.NewCanonicalEncoder(bw)
		replayErr = drain(r, func(rec replay.Record) error { return enc.Encode(rec) })
	}
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
	var store storeFlags
	store.register(fs, "verify from")
	var chartOpt chartFlags
	chartOpt.register(fs)

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape verify [flags] <window>\n\n"+
				"Reads a window end to end without writing it out, and reports what is in it:\n"+
				"counts, the SHA-256 of its canonical NDJSON, every gap and reconnect, and\n"+
				"replay throughput against the wall-clock time the window took to capture.\n\n"+
				"The window is a local path, or a key prefix when -s3-bucket is given. The\n"+
				"digest is the same either way, which is how a stored copy is checked against\n"+
				"the local one it came from.\n\n"+
				"It exits non-zero if the window is discontinuous. A window with a gap in it\n"+
				"is untrustworthy — the public feed offers no backfill — and a verifier that\n"+
				"returned success on one would be worse than no verifier.\n\n"+
				"On a terminal it also draws the window's shape under that summary: message\n"+
				"density over time, one column per slice of the window, with file boundaries\n"+
				"marked and every gap marked in red where it happened. The summary above it\n"+
				"is unchanged and prints either way; -chart=false leaves only the summary.\n\nflags:\n")
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
	r, err := openWindow(context.Background(), &store, fs.Arg(0),
		replay.WithReorderWindow(*reorder), replay.WithContinueOnGap())
	if err != nil {
		return err
	}
	defer r.Close()

	caps := chartOpt.caps(os.Stdout)
	drawing := chartOpt.drawing(caps)

	// The chart is fed from the drain that was already reading every record. It
	// costs one bucket increment per record and holds a bounded histogram, not
	// the window.
	ch := newChart()
	h := sha256.New()
	enc := replay.NewCanonicalEncoder(h)
	started := time.Now()
	replayErr := drain(r, func(rec replay.Record) error {
		if drawing {
			ch.add(rec)
		}
		return enc.Encode(rec)
	})
	elapsed := time.Since(started)

	printSummary(os.Stdout, r, elapsed)
	fmt.Fprintf(os.Stdout, "  digest      sha256:%s\n", hex.EncodeToString(h.Sum(nil)))
	for _, d := range r.Discontinuities() {
		fmt.Fprintf(os.Stdout, "  ! %s\n", caps.Paint(termui.ColorRed, d.String()))
	}
	if drawing {
		ch.print(os.Stdout, caps)
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

// openWindow opens the window named by arg: a key prefix in the bucket if one
// was named, a local path otherwise. The two produce identical output, so this
// is the only place either command has to know which it got.
func openWindow(ctx context.Context, store *storeFlags, arg string, opts ...replay.Option) (*replay.Reader, error) {
	if !store.set() {
		return replay.Open(arg, opts...)
	}
	st, err := store.store(ctx)
	if err != nil {
		return nil, err
	}
	return replay.OpenStore(ctx, st, arg, opts...)
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
