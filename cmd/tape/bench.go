package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/AnanmayS/tape/internal/bench"
	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/tapefile"
)

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	dir := fs.String("dir", "", "where runs write their tape files (default: a temp directory)")
	keep := fs.Bool("keep", false, "keep what each run wrote instead of deleting it")
	product := fs.String("product", feed.CoinbaseProduct, "symbol the window holds")
	policies := fs.String("policy", "all", "backpressure policies to measure: block, drop, buffer, or all")
	formats := fs.String("format", "both", "on-disk formats to measure: raw, columnar, or both")
	repeat := fs.Int("repeat", 20, "how many times to push the window through per run")
	speed := fs.Float64("speed", 0, "replay at this multiple of the window's own wall clock; 0 is as fast as the queue accepts")
	window := fs.Duration("window", tapefile.DefaultWindow, "file rotation window")
	buffer := fs.Int("buffer", 4096, "reader-to-writer channel depth")
	flushEvery := fs.Duration("flush", time.Second, "maximum time a record sits in the write buffer")
	interval := fs.Duration("sample", bench.SampleInterval, "how often to sample the queue and the heap")
	logFormat := fs.String("log", "text", "log format: text or json")
	verbose := fs.Bool("v", false, "print the sampled queue depth and rate over time")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape bench [flags] <window>\n\n"+
				"Pushes a captured window back through the capture path — sequence tracking,\n"+
				"gap detection, rotation, tape or columnar writing — as fast as the\n"+
				"backpressure policy will accept it, and reports what each policy costs.\n\n"+
				"The load generator is this project: a window is read with the replay\n"+
				"library, held in memory, and offered to capture through the same sink a\n"+
				"live socket pushes into. Only the socket is substituted.\n\n"+
				"With -speed 0 the feed offers frames faster than any exchange sends them,\n"+
				"which is the point: the number worth having is not the rate at 40 messages\n"+
				"a second but where the writer stops keeping up and what each policy does\n"+
				"there. A speed above zero replays at that multiple of the window's own wall\n"+
				"clock, which is how the approach to that edge gets measured.\n\n"+
				"Write to a real disk. A run on tmpfs measures memory bandwidth.\n\n"+
				"flags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("bench takes exactly one window")
	}

	log, err := newLogger(*logFormat)
	if err != nil {
		return err
	}
	pols, err := parsePolicies(*policies)
	if err != nil {
		return err
	}
	fmts, err := parseFormats(*formats)
	if err != nil {
		return err
	}

	load, err := bench.LoadWindow(fs.Arg(0), *product)
	if err != nil {
		return err
	}

	root := *dir
	if root == "" {
		root, err = os.MkdirTemp("", "tape-bench-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(root)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("window   %s\n", fs.Arg(0))
	fmt.Printf("frames   %d messages, %s of raw frames, recorded over %s at %.1f msg/s\n",
		len(load.Frames), byteCount(load.Bytes), load.Span.Round(time.Second), load.Rate())
	fmt.Printf("load     %d passes = %d messages, %s offered per run\n",
		*repeat, int64(len(load.Frames))*int64(*repeat), byteCount(load.Bytes*int64(*repeat)))
	fmt.Printf("writing  %s\n\n", root)

	var results []bench.Result
	for _, f := range fmts {
		for _, p := range pols {
			runRoot := filepath.Join(root, fmt.Sprintf("%s-%s", f, p))
			res, err := bench.Run(ctx, load, bench.Config{
				Root:          runRoot,
				Policy:        p,
				Format:        f,
				Repeat:        *repeat,
				Speed:         *speed,
				Window:        *window,
				Buffer:        *buffer,
				FlushInterval: *flushEvery,
				Interval:      *interval,
				Log:           log,
			})
			if err != nil {
				return fmt.Errorf("%s/%s: %w", f, p, err)
			}
			results = append(results, res)
			if *verbose {
				printSamples(res)
			}
			if !*keep {
				if err := os.RemoveAll(runRoot); err != nil {
					return err
				}
			}
			if ctx.Err() != nil {
				break
			}
		}
	}
	printResults(results)
	return nil
}

func parsePolicies(s string) ([]capture.Policy, error) {
	if s == "all" {
		return capture.Policies, nil
	}
	var out []capture.Policy
	for _, name := range strings.Split(s, ",") {
		p := capture.Policy(strings.TrimSpace(name))
		found := false
		for _, known := range capture.Policies {
			if p == known {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown policy %q (want block, drop, buffer or all)", name)
		}
		out = append(out, p)
	}
	return out, nil
}

func parseFormats(s string) ([]capture.Format, error) {
	switch s {
	case "both":
		return []capture.Format{capture.FormatRaw, capture.FormatColumnar}, nil
	case string(capture.FormatRaw):
		return []capture.Format{capture.FormatRaw}, nil
	case string(capture.FormatColumnar):
		return []capture.Format{capture.FormatColumnar}, nil
	default:
		return nil, fmt.Errorf("unknown format %q (want raw, columnar or both)", s)
	}
}

// printResults is the table the decision gets made from. Offered against
// written is the loss; written against the window's own rate is the headroom.
func printResults(results []bench.Result) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "format\tpolicy\toffered/s\twritten/s\tdropped\tloss\tpeak depth\theap\tp50\tp99\tp99.9\tmax\tgaps\tbytes")
	for _, r := range results {
		l := r.Summary.WriteLatency
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			r.Format, r.Policy,
			count(r.OfferedRate()), count(r.WrittenRate()),
			r.Summary.Dropped, percent(r.LossFraction()),
			r.PeakDepth(), signedBytes(r.HeapGrowth()),
			l.P50, l.P99, l.P999, l.Max,
			r.Summary.Gaps, byteCount(r.Summary.Bytes))
	}
	w.Flush()
}

// printSamples is the queue over time: the shape of the fill, which a single
// peak cannot show.
func printSamples(r bench.Result) {
	fmt.Printf("%s/%s, %s in %s\n", r.Format, r.Policy,
		count(r.WrittenRate())+" msg/s", r.Elapsed.Round(time.Millisecond))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  at\tmsg/s\tmessages\tqueue depth\tgaps")
	var at time.Duration
	for _, s := range r.Samples {
		at += s.Duration()
		depth := "-"
		if s.QueueObserved {
			depth = fmt.Sprintf("%d", s.MaxQueueDepth)
		}
		fmt.Fprintf(w, "  %s\t%s\t%d\t%s\t%d\n",
			at.Round(time.Millisecond), count(s.MessagesPerSecond()), s.Messages, depth, s.Gaps)
	}
	w.Flush()
	fmt.Println()
}

func count(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func percent(f float64) string {
	if f == 0 {
		return "0"
	}
	return fmt.Sprintf("%.1f%%", f*100)
}

func signedBytes(n int64) string {
	if n < 0 {
		return "-" + byteCount(-n)
	}
	return "+" + byteCount(n)
}

// byteCount renders a size the way a reader wants to compare two of them:
// three significant figures and a unit, not eight digits.
func byteCount(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
