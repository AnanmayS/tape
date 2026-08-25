package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AnanmayS/tape/internal/capture"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

func runCapture(args []string) error {
	fs := flag.NewFlagSet("capture", flag.ExitOnError)
	dir := fs.String("dir", "data", "root directory for tape files")
	window := fs.Duration("window", tapefile.DefaultWindow, "wall-clock file rotation window")
	buffer := fs.Int("buffer", 4096, "reader-to-writer channel depth")
	flushEvery := fs.Duration("flush", time.Second, "maximum time a record sits in the write buffer")
	duration := fs.Duration("duration", 0, "stop after this long; 0 runs until interrupted")
	logFormat := fs.String("log", "text", "log format: text or json")
	var store storeFlags
	store.register(fs, "upload each closed file to")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(),
			"usage: tape capture [flags]\n\n"+
				"Captures %s %s (channels: %v) to length-prefixed tape files under -dir.\n"+
				"The exchange, product and channels are fixed for v1 and are not flags.\n\n"+
				"Files land at -dir plus their storage key:\n"+
				"  v1/symbol={symbol}/date={date}/hour={hour}/{window start}.tape\n\n"+
				"With -s3-bucket, each file is uploaded as it closes, under that same key.\n"+
				"Local disk stays the durable copy: an unreachable bucket costs a re-upload,\n"+
				"never a frame, and the upload is logged loudly rather than failing capture.\n\n"+
				"flags:\n", feed.CoinbaseURL, feed.CoinbaseProduct,
			[]string{feed.ChannelLevel2Batch, feed.ChannelMatches})
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	log, err := newLogger(*logFormat)
	if err != nil {
		return err
	}

	st, err := store.store(context.Background())
	if err != nil {
		return err
	}

	// SIGINT and SIGTERM cancel the context; the capture loop drains the
	// channel, flushes and closes the file, then reports.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	f := feed.NewCoinbase(log)
	log.Info("capture starting",
		"dir", *dir,
		"store", storeLabel(st),
		"window", window.String(),
		"buffer", *buffer,
		"duration", durationLabel(*duration),
		"seq_mode", f.SeqMode().String())

	sum, runErr := capture.Run(ctx, f, capture.Config{
		Root:          *dir,
		Store:         st,
		Window:        *window,
		Buffer:        *buffer,
		FlushInterval: *flushEvery,
		Log:           log,
	})

	// The summary is worth printing even when the session died.
	log.Info("session summary", sum.LogAttrs()...)
	for _, p := range sum.Files {
		log.Info("wrote file", "path", p)
	}
	return runErr
}

func newLogger(format string) (*slog.Logger, error) {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("unknown log format %q (want text or json)", format)
	}
}

func durationLabel(d time.Duration) string {
	if d <= 0 {
		return "until interrupted"
	}
	return d.String()
}

func storeLabel(st storage.Store) string {
	if st == nil {
		return "local only"
	}
	return st.String()
}
