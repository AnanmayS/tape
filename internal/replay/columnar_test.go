package replay

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/AnanmayS/tape/internal/colfmt"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// The columnar fixture under testdata/window-columnar is the same window as the
// one under testdata/window, in the v2 format: the same frames, in the same
// order, in files with the same names. It is transcoded from the raw fixture
// rather than captured separately, which is the only way the comparison below
// means anything — two captures of "the same" live window are not the same
// bytes, and then a digest that matched would prove nothing.
//
// It is committed rather than generated at test time on purpose. A stored
// format is a promise to files that already exist, and a committed fixture is
// what notices when the encoder stops keeping it.
//
// To rebuild it, after a deliberate format change:
//
//	go test ./internal/replay -run TestGenerateColumnarFixture -fixture.columnar
const columnarFixtureDir = "testdata/window-columnar"

var regenColumnar = flag.Bool("fixture.columnar", false,
	"rebuild testdata/window-columnar from testdata/window")

// TestColumnarDeterminism is the M5 claim, and it is the M3 claim again with
// the storage format changed underneath it: the same window, stored columnar,
// replays to the same bytes and the same SHA-256 as the raw one.
//
// It compares against goldenDigest — the constant M3 wrote down, which M4
// carried through the storage move untouched — because anything less would be
// comparing the new format against itself.
func TestColumnarDeterminism(t *testing.T) {
	rawRoot := fixtureWindow(t)
	colRoot := materialize(t, columnarFixtureDir)

	raw, rawDigest := replayCanonical(t, rawRoot)
	col, colDigest := replayCanonical(t, colRoot)

	if colDigest != rawDigest {
		t.Fatalf("the same window replays differently from the two formats:\n raw %s\n col %s",
			rawDigest, colDigest)
	}
	if !bytes.Equal(raw, col) {
		t.Fatalf("digests match but bytes differ: %d vs %d bytes", len(raw), len(col))
	}
	if colDigest != goldenDigest {
		t.Errorf("columnar window digest is %s, want %s\n"+
			"the columnar format is not storing what the raw one stores",
			colDigest, goldenDigest)
	}

	// Twice, in the same binary, for the same reason M3 does it: the claim is
	// about repeated replays and not only about agreement between formats.
	if _, again := replayCanonical(t, colRoot); again != colDigest {
		t.Errorf("two replays of the columnar window disagree: %s vs %s", colDigest, again)
	}

	rawBytes, colBytes := windowBytes(t, rawRoot), windowBytes(t, colRoot)
	t.Logf("%d bytes of canonical NDJSON, sha256 %s; window is %d bytes raw, %d columnar (%.2fx)",
		len(col), colDigest, rawBytes, colBytes, float64(rawBytes)/float64(colBytes))
}

// TestMixedFormatWindow replays a window whose files are not all in the same
// format. Format is a property of a file, decided by its own version byte, so a
// capture that was restarted onto a new build leaves a window like this — and
// it has to replay as one window, byte for byte, like any other.
func TestMixedFormatWindow(t *testing.T) {
	rawRoot := fixtureWindow(t)
	colRoot := materialize(t, columnarFixtureDir)
	mixed := t.TempDir()

	files := windowRelFiles(t, rawRoot)
	if len(files) < 3 {
		t.Fatalf("fixture has %d files, need at least 3 to mix", len(files))
	}
	for i, rel := range files {
		src := rawRoot
		if i%2 == 1 {
			src = colRoot
		}
		dst := filepath.Join(mixed, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(src, rel))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	_, digest := replayCanonical(t, mixed)
	if digest != goldenDigest {
		t.Errorf("a window of mixed formats replayed to %s, want %s", digest, goldenDigest)
	}
}

// TestColumnarFixtureIsCurrent regenerates the columnar fixture in memory and
// requires it to be the bytes that are committed.
//
// This is the encoder's regression test. TestColumnarDeterminism would still
// pass if the encoder changed its output entirely, as long as the decoder
// changed with it — and that is exactly the change that silently orphans every
// file already written. Here, the committed bytes are the older build's output
// and they have to still be what this build produces.
func TestColumnarFixtureIsCurrent(t *testing.T) {
	rawRoot := fixtureWindow(t)
	committed := materialize(t, columnarFixtureDir)

	for _, rel := range windowRelFiles(t, rawRoot) {
		want, err := os.ReadFile(filepath.Join(committed, rel))
		if err != nil {
			t.Fatalf("read committed fixture %s: %v", rel, err)
		}
		var got bytes.Buffer
		if err := transcode(filepath.Join(rawRoot, rel), &got); err != nil {
			t.Fatalf("transcode %s: %v", rel, err)
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Errorf("%s: re-encoding the raw fixture produced %d bytes, the committed fixture has %d;\n"+
				"the columnar encoder's output moved, which orphans files already written — "+
				"rebuild with -fixture.columnar only if that was the intent",
				rel, got.Len(), len(want))
		}
	}
}

// TestGenerateColumnarFixture rebuilds testdata/window-columnar. It is skipped
// without -fixture.columnar.
func TestGenerateColumnarFixture(t *testing.T) {
	if !*regenColumnar {
		t.Skip("set -fixture.columnar to rebuild the columnar fixture")
	}
	rawRoot := fixtureWindow(t)
	if err := os.RemoveAll(columnarFixtureDir); err != nil {
		t.Fatalf("clear fixture dir: %v", err)
	}
	for _, rel := range windowRelFiles(t, rawRoot) {
		out := filepath.Join(columnarFixtureDir, rel+".gz")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		plain := filepath.Join(t.TempDir(), filepath.Base(rel))
		f, err := os.Create(plain)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := transcode(filepath.Join(rawRoot, rel), f); err != nil {
			f.Close()
			t.Fatalf("transcode %s: %v", rel, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		n, err := gzipTo(plain, out)
		if err != nil {
			t.Fatalf("gzip: %v", err)
		}
		src, err := os.Stat(filepath.Join(rawRoot, rel))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		col, err := os.Stat(plain)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		t.Logf("%s: %d bytes raw -> %d columnar (%.2fx), %d gzipped in the fixture",
			rel, src.Size(), col.Size(), float64(src.Size())/float64(col.Size()), n)
	}
}

// transcode rewrites one v1 tape file as a v2 columnar one.
//
// It reads records and writes records: the file's contents are re-encoded and
// nothing in them is re-interpreted, so the record ordinals, the file
// boundaries and the frames are the ones the source file had. That is what lets
// the transcoded window carry the same replay digest — replay names records by
// their position in their file, and those positions are preserved here.
func transcode(src string, dst io.Writer) error {
	rd, err := tapefile.Open(src)
	if err != nil {
		return err
	}
	defer rd.Close()
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

// windowRelFiles lists a materialized window's files, relative to its root.
func windowRelFiles(t testing.TB, root string) []string {
	t.Helper()
	r, err := Open(root, WithContinueOnGap())
	if err != nil {
		t.Fatalf("Open %s: %v", root, err)
	}
	defer r.Close()
	return r.Files()
}

func windowBytes(t testing.TB, root string) int64 {
	t.Helper()
	var total int64
	for _, rel := range windowRelFiles(t, root) {
		fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		total += fi.Size()
	}
	return total
}
