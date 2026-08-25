package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AnanmayS/tape/internal/colfmt"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// runStat measures a window's storage: what the raw frames in it cost, what the
// columnar format costs, and where every byte of the second number goes.
//
// It is the compression ratio's provenance. The number in the README comes from
// running this on a real captured window, not from an estimate — and the
// per-column breakdown is here because "5.2x" without it is a claim rather than
// a measurement.
func runStat(args []string) error {
	fs := flag.NewFlagSet("stat", flag.ExitOnError)
	var store storeFlags
	store.register(fs, "measure from")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(),
			"usage: tape stat [flags] <window>\n\n"+
				"Measures what a window costs to store. For a window already in the columnar\n"+
				"format the numbers are read out of the files; for a raw v1 window they are\n"+
				"produced by encoding it, so the answer is what storing it columnar would\n"+
				"actually cost rather than an estimate of it.\n\n"+
				"The ratio is against the raw feed frames — the bytes a naive NDJSON store of\n"+
				"the same window would have to write — and, for a v1 window, against the tape\n"+
				"files on disk as well.\n\n"+
				"A window is a local directory of .tape files, one .tape file, or a key prefix\n"+
				"when -s3-bucket is given.\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("stat takes exactly one window")
	}

	ctx := context.Background()
	st, prefix, label, err := statWindow(ctx, &store, fs.Arg(0))
	if err != nil {
		return err
	}
	keys, err := windowKeys(ctx, st, prefix)
	if err != nil {
		return err
	}

	var total statTotals
	for _, key := range keys {
		s, err := statObject(ctx, st, key)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		total.add(s)
	}
	printStat(os.Stdout, label, total)
	return nil
}

// statTotals is the whole window's measurement, summed over its files.
type statTotals struct {
	Files    int
	Records  int64
	Batches  int
	Gaps     int
	Stored   int64 // the window as it is on disk now
	Columnar int64 // the window in the columnar format
	Frames   int64 // the raw feed frames inside it
	Framing  int64
	Columns  map[string]colfmt.ColumnSize
	Formats  map[uint16]int
}

func (t *statTotals) add(s fileStat) {
	if t.Columns == nil {
		t.Columns = map[string]colfmt.ColumnSize{}
		t.Formats = map[uint16]int{}
	}
	t.Files++
	t.Formats[s.version]++
	t.Stored += s.stored
	t.Columnar += s.profile.Bytes
	t.Frames += s.profile.FrameBytes
	t.Framing += s.profile.Framing
	t.Records += s.profile.Rows
	t.Batches += len(s.profile.Batches)
	for _, f := range s.profile.Batches {
		if f.HasGap() {
			t.Gaps++
		}
	}
	for _, c := range s.profile.Columns {
		cur := t.Columns[c.Name]
		cur.Name = c.Name
		cur.Raw += c.Raw
		cur.Encoded += c.Encoded
		t.Columns[c.Name] = cur
	}
}

// fileStat is one object's measurement.
type fileStat struct {
	version uint16
	stored  int64
	profile colfmt.Profile
}

// statObject measures one stored object, encoding it columnar first if it is
// not already.
//
// The object is read into memory. A window file is a few hundred kilobytes and
// this is a measurement command, not the replay path — which streams, and must.
func statObject(ctx context.Context, st storage.Store, key string) (fileStat, error) {
	rc, err := st.Open(ctx, key)
	if err != nil {
		return fileStat{}, err
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fileStat{}, err
	}
	version, err := tapefile.HeaderVersion(b)
	if err != nil {
		return fileStat{}, err
	}

	s := fileStat{version: version, stored: int64(len(b))}
	switch version {
	case colfmt.Version:
		s.profile, err = colfmt.ProfileFile(bytes.NewReader(b))
	case tapefile.Version:
		var encoded bytes.Buffer
		if err := transcodeToColumnar(bytes.NewReader(b), &encoded); err != nil {
			return fileStat{}, err
		}
		s.profile, err = colfmt.ProfileFile(bytes.NewReader(encoded.Bytes()))
	default:
		return fileStat{}, fmt.Errorf("%w: v%d", tapefile.ErrBadVersion, version)
	}
	return s, err
}

// transcodeToColumnar re-encodes a v1 stream as a columnar one. It reads
// records and writes records; nothing in them is reinterpreted.
func transcodeToColumnar(src io.Reader, dst io.Writer) error {
	rd, err := tapefile.NewReader(src)
	if err != nil {
		return err
	}
	w := colfmt.NewBatchWriter(dst)
	for {
		typ, payload, err := rd.Next()
		if err == io.EOF {
			return w.Close()
		}
		if err != nil {
			return err
		}
		if err := w.WriteRecord(typ, payload); err != nil {
			return err
		}
	}
}

// statWindow resolves the window argument to a store, a prefix and a label. A
// single local file is a window of one, which is how a file that looks wrong
// gets measured on its own.
func statWindow(ctx context.Context, store *storeFlags, arg string) (storage.Store, string, string, error) {
	if store.set() {
		st, err := store.store(ctx)
		if err != nil {
			return nil, "", "", err
		}
		return st, arg, strings.TrimSuffix(st.String(), "/") + "/" + arg, nil
	}
	fi, err := os.Stat(arg)
	if err != nil {
		return nil, "", "", err
	}
	if fi.IsDir() {
		root := filepath.Clean(arg)
		return storage.NewLocal(root), "", root, nil
	}
	dir, base := filepath.Split(arg)
	if dir == "" {
		dir = "."
	}
	return storage.NewLocal(filepath.Clean(dir)), base, arg, nil
}

// windowKeys lists the tape objects under prefix, sorted.
func windowKeys(ctx context.Context, st storage.Store, prefix string) ([]string, error) {
	keys, err := st.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		if filepath.Ext(k) == storage.Ext {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s objects under %s", storage.Ext, prefix)
	}
	sort.Strings(out)
	return out, nil
}

func printStat(w io.Writer, label string, t statTotals) {
	fmt.Fprintf(w, "window %s\n", label)
	fmt.Fprintf(w, "  files       %d (%s)\n", t.Files, formatLabel(t.Formats))
	fmt.Fprintf(w, "  records     %d in %d batches\n", t.Records, t.Batches)
	if t.Gaps > 0 {
		fmt.Fprintf(w, "  gaps        %d batches carry the gap flag\n", t.Gaps)
	}
	fmt.Fprintf(w, "  raw frames  %d bytes — what NDJSON would store\n", t.Frames)
	fmt.Fprintf(w, "  on disk     %d bytes\n", t.Stored)
	fmt.Fprintf(w, "  columnar    %d bytes\n", t.Columnar)
	if t.Columnar > 0 {
		fmt.Fprintf(w, "  ratio       %.2fx against the raw frames", ratio(t.Frames, t.Columnar))
		if t.Formats[tapefile.Version] > 0 {
			fmt.Fprintf(w, ", %.2fx against the tape files on disk", ratio(t.Stored, t.Columnar))
		}
		fmt.Fprintln(w)
	}

	cols := make([]colfmt.ColumnSize, 0, len(t.Columns))
	for _, c := range t.Columns {
		cols = append(cols, c)
	}
	// Largest first: the point of the breakdown is which columns cost anything.
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].Encoded != cols[j].Encoded {
			return cols[i].Encoded > cols[j].Encoded
		}
		return cols[i].Name < cols[j].Name
	})
	fmt.Fprintf(w, "\n  %-14s %12s %12s %8s %8s\n", "column", "encoded", "raw", "share", "ratio")
	for _, c := range cols {
		fmt.Fprintf(w, "  %-14s %12d %12d %7.1f%% %7.2fx\n",
			c.Name, c.Encoded, c.Raw,
			100*float64(c.Encoded)/float64(t.Columnar), ratio(c.Raw, c.Encoded))
	}
	fmt.Fprintf(w, "  %-14s %12d %12s %7.1f%%\n", "framing", t.Framing, "-",
		100*float64(t.Framing)/float64(t.Columnar))
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func formatLabel(formats map[uint16]int) string {
	var parts []string
	for _, v := range []uint16{tapefile.Version, colfmt.Version} {
		if n := formats[v]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d v%d", n, v))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}
