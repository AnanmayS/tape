package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math/rand/v2"
	"path"
	"sort"
	"testing"

	"github.com/AnanmayS/tape/internal/storage"
)

// goldenDigest is the SHA-256 of the canonical NDJSON of testdata/window,
// replayed with WithContinueOnGap.
//
// The run-to-run comparisons below are the invariant. This constant is the
// regression guard on top of it: two replays in the same binary would still
// agree if the ordering rules were changed, and this notices that they were.
// It changes only when the fixture or the canonical form deliberately changes,
// and changing it is a decision, not a fix.
const goldenDigest = "ee9576040361b07272db0cb6e614b02cef53dec1fcc772aeea1fa609b4fb7a21"

// TestDeterminism is the invariant this project exists for: replaying one
// stored window twice produces byte-identical output.
//
// It is never skipped. It uses the real captured fixture, not a synthetic one,
// because the ordering cases that actually bite — messages with no sequence,
// records with no exchange timestamp, two channels interleaved across three
// files, a reconnect in the middle — are the ones real data supplies.
func TestDeterminism(t *testing.T) {
	root := fixtureWindow(t)

	first, firstDigest := replayCanonical(t, root)
	second, secondDigest := replayCanonical(t, root)

	if firstDigest != secondDigest {
		t.Fatalf("two replays of one window differ:\n first  %s\n second %s", firstDigest, secondDigest)
	}
	if !bytes.Equal(first, second) {
		// Cannot happen while the digests match, but the claim is about bytes,
		// so the bytes are what gets compared.
		t.Fatalf("digests match but bytes differ: %d vs %d bytes", len(first), len(second))
	}
	if len(first) == 0 {
		t.Fatal("replay produced no output")
	}
	t.Logf("replayed %d bytes of canonical NDJSON, sha256 %s", len(first), firstDigest)

	if firstDigest != goldenDigest {
		t.Errorf("window digest changed\n got  %s\n want %s\n"+
			"the ordering rules or the canonical form moved; that is a decision, not a fix",
			firstDigest, goldenDigest)
	}
}

// TestDeterminismThroughStore is the M4 claim: moving storage behind an
// interface changed nothing about what comes out.
//
// The same fixture is replayed twice — once by local path, once through a
// storage.Store — and both must produce the golden digest. The store is the
// filesystem one here, which is the implementation CI runs and the one that
// needs no credentials; the S3 implementation is held to the same digest
// against an in-process fake in internal/storage/s3store.
func TestDeterminismThroughStore(t *testing.T) {
	root := fixtureWindow(t)
	direct, directDigest := replayCanonical(t, root)
	stored, storedDigest := replayCanonicalStore(t, storage.NewLocal(root), "")

	if storedDigest != directDigest {
		t.Fatalf("replay through a store differs from replay by path:\n path  %s\n store %s",
			directDigest, storedDigest)
	}
	if !bytes.Equal(direct, stored) {
		t.Fatalf("digests match but bytes differ: %d vs %d bytes", len(direct), len(stored))
	}
	if storedDigest != goldenDigest {
		t.Errorf("window digest through a store is %s, want %s", storedDigest, goldenDigest)
	}
	t.Logf("replayed %d bytes through %s, sha256 %s", len(stored), storage.NewLocal(root), storedDigest)
}

// TestStoreWindowPrefix checks that a window living under a key prefix replays
// the same as one living at the root of a store. Files are named relative to
// the window, so the prefix a window happens to be filed under is not
// something its replay can reveal — which is what lets a bucket holding every
// symbol and every day still replay one window byte-identically.
func TestStoreWindowPrefix(t *testing.T) {
	root := fixtureWindow(t)
	flat, flatDigest := replayCanonicalStore(t, storage.NewLocal(root), "")

	// Re-file the same objects under a deep prefix, alongside a decoy window
	// that the prefix scan must not pick up.
	deep := t.TempDir()
	st := storage.NewLocal(deep)
	const prefix = "v1/symbol=BTC-USD/date=2026-08-25/hour=03/"
	ctx := context.Background()
	for _, rel := range mustList(t, storage.NewLocal(root), "") {
		copyObject(t, storage.NewLocal(root), rel, st, prefix+path.Base(rel))
	}
	if err := st.Put(ctx, "v1/symbol=ETH-USD/date=2026-08-25/hour=03/decoy.tape",
		bytes.NewReader([]byte("not this window"))); err != nil {
		t.Fatalf("Put decoy: %v", err)
	}

	prefixed, prefixedDigest := replayCanonicalStore(t, st, prefix)
	if prefixedDigest == flatDigest {
		t.Fatal("the two layouts nest the files at different depths, so the file names " +
			"in the output must differ; identical digests mean the names are not in the output")
	}

	// What must be identical is everything except the file names: strip those
	// and the two windows are the same bytes.
	if a, b := withoutFileNames(flat), withoutFileNames(prefixed); !bytes.Equal(a, b) {
		t.Fatalf("a window under a prefix replayed differently: %d vs %d bytes", len(a), len(b))
	}
	if _, again := replayCanonicalStore(t, st, prefix); again != prefixedDigest {
		t.Errorf("two replays under a prefix disagree: %s vs %s", prefixedDigest, again)
	}
	t.Logf("window under %q replays to sha256 %s", prefix, prefixedDigest)
}

// withoutFileNames blanks the "file" field of every canonical record, leaving
// the part of the output that must not depend on where a window is stored.
func withoutFileNames(ndjson []byte) []byte {
	var out bytes.Buffer
	for _, line := range bytes.Split(ndjson, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		i := bytes.Index(line, []byte(`"file":"`))
		if i < 0 {
			out.Write(line)
			out.WriteByte('\n')
			continue
		}
		j := bytes.IndexByte(line[i+len(`"file":"`):], '"')
		out.Write(line[:i])
		out.Write(line[i+len(`"file":"`)+j:])
		out.WriteByte('\n')
	}
	return out.Bytes()
}

func mustList(t *testing.T, st storage.Store, prefix string) []string {
	t.Helper()
	keys, err := st.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return keys
}

func copyObject(t *testing.T, from storage.Store, fromKey string, to storage.Store, toKey string) {
	t.Helper()
	ctx := context.Background()
	rc, err := from.Open(ctx, fromKey)
	if err != nil {
		t.Fatalf("Open %s: %v", fromKey, err)
	}
	defer rc.Close()
	if err := to.Put(ctx, toKey, rc); err != nil {
		t.Fatalf("Put %s: %v", toKey, err)
	}
}

// replayCanonicalStore is replayCanonical through a storage.Store.
func replayCanonicalStore(t *testing.T, st storage.Store, prefix string) ([]byte, string) {
	t.Helper()
	r, err := OpenStore(context.Background(), st, prefix, WithContinueOnGap())
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer r.Close()
	return drainCanonical(t, r)
}

// TestDeterminismAcrossCodePaths replays the same window a second way: every
// record is read out, shuffled, and sorted with an unstable sort on the same
// comparator. The two paths share the comparator and nothing else — one is an
// incremental heap fed in arrival order, the other is a whole-window sort fed
// in a scrambled order.
//
// It is the shuffle that earns this test its keep. If the ordering key were not
// a strict total order, two records would compare equal, an unstable sort would
// be free to keep whichever the shuffle happened to put first, and the outputs
// would diverge. Agreement across five seeds says the key leaves no tie behind.
func TestDeterminismAcrossCodePaths(t *testing.T) {
	root := fixtureWindow(t)
	_, streamed := replayCanonical(t, root)

	for _, seed := range []uint64{1, 2, 3, 5, 8} {
		sorted := sortWholeWindow(t, root, seed)
		if sorted != streamed {
			t.Fatalf("seed %d: whole-window sort disagrees with streaming replay:\n sorted  %s\n stream  %s",
				seed, sorted, streamed)
		}
	}
}

// replayCanonical replays the window and returns the canonical NDJSON and its
// digest. WithContinueOnGap is used deliberately: the fixture contains a
// reconnect and a gap, and the point is that they are delivered through the
// iterator and serialized like everything else rather than skipped.
func replayCanonical(t *testing.T, root string) ([]byte, string) {
	t.Helper()
	r, err := Open(root, WithContinueOnGap())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	return drainCanonical(t, r)
}

// drainCanonical serializes a whole replay and returns the bytes and their
// digest.
func drainCanonical(t *testing.T, r *Reader) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	enc := NewCanonicalEncoder(&buf)
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if err := enc.Encode(rec); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	return buf.Bytes(), digest(buf.Bytes())
}

// sortWholeWindow is the second code path: read everything, scramble it, sort
// it with sort.Slice, serialize. sort.Slice is not stable, which is the point.
func sortWholeWindow(t *testing.T, root string, seed uint64) string {
	t.Helper()
	src, err := newSource(root)
	if err != nil {
		t.Fatalf("newSource: %v", err)
	}
	defer src.Close()

	var all []pending
	for {
		p, ok, err := src.next()
		if err != nil {
			t.Fatalf("source.next: %v", err)
		}
		if !ok {
			break
		}
		all = append(all, p)
	}

	rng := rand.New(rand.NewPCG(seed, seed*2+1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	sort.Slice(all, func(i, j int) bool { return compareKeys(all[i].key, all[j].key) < 0 })

	h := sha256.New()
	enc := NewCanonicalEncoder(h)
	for i, p := range all {
		rec := p.rec
		rec.Index = int64(i)
		if err := enc.Encode(rec); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	return hexSum(h)
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hexSum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// TestFixtureShape asserts the fixture still contains what the other tests
// assume: real messages across several files, one gap, and two reseeds.
func TestFixtureShape(t *testing.T) {
	root := fixtureWindow(t)
	r, err := Open(root, WithContinueOnGap())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if got := len(r.Files()); got < 3 {
		t.Errorf("fixture spans %d files, want at least 3", got)
	}

	var opening int
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if rec.Kind == KindReseed && rec.Opening {
			opening++
		}
	}

	st := r.Stats()
	if st.Messages < 2000 {
		t.Errorf("fixture has %d messages, want a few thousand", st.Messages)
	}
	if st.Gaps != 1 {
		t.Errorf("fixture has %d gap records, want 1", st.Gaps)
	}
	if st.Reseeds != 2 {
		t.Errorf("fixture has %d reseed records, want 2", st.Reseeds)
	}
	if opening != 1 {
		t.Errorf("fixture has %d opening reseeds, want exactly 1", opening)
	}
	if len(r.Discontinuities()) != 2 {
		t.Errorf("fixture crossed %d discontinuities, want 2 (the reconnect reseed and its gap)",
			len(r.Discontinuities()))
	}
	if r.Trustworthy() {
		t.Error("a window containing a gap must not report as trustworthy")
	}
	t.Logf("fixture: %d records, %d messages, %d bytes, span %s, files %v",
		st.Records, st.Messages, st.Bytes, st.Span(), r.Files())
	for _, d := range r.Discontinuities() {
		t.Logf("discontinuity: %s", d)
	}
}

// TestGapStopsReplayByDefault holds invariant 2 in place: reading past a break
// in continuity is something a caller asks for, never something that happens.
func TestGapStopsReplayByDefault(t *testing.T) {
	root := fixtureWindow(t)
	r, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	var delivered int64
	var stop error
	for {
		rec, err := r.Next()
		if err != nil {
			stop = err
			break
		}
		delivered++
		_ = rec
	}

	if !errors.Is(stop, ErrDiscontinuity) {
		t.Fatalf("replay ended with %v, want a discontinuity error", stop)
	}
	if delivered == 0 {
		t.Fatal("replay stopped before delivering anything; the opening reseed is not a discontinuity")
	}

	var de *DiscontinuityError
	if !errors.As(stop, &de) {
		t.Fatalf("error %v is not a *DiscontinuityError", stop)
	}
	if de.D.Kind != DiscontinuityReseed {
		t.Errorf("stopped at a %s; the reconnect reseed is stored before the gap it causes", de.D.Kind)
	}
	t.Logf("stopped after %d records: %v", delivered, stop)

	// The error is sticky: a caller that ignores it and keeps iterating gets it
	// again rather than silently resuming on the far side of the break.
	if _, err := r.Next(); !errors.Is(err, ErrDiscontinuity) {
		t.Errorf("Next after a discontinuity returned %v, want the same error", err)
	}
}

// TestContinueOnGapDeliversTheGap checks the other half: continuing is allowed,
// hiding is not.
func TestContinueOnGapDeliversTheGap(t *testing.T) {
	root := fixtureWindow(t)
	recs := replayAll(t, root, WithContinueOnGap())

	var gaps, reseeds int
	for _, rec := range recs {
		switch rec.Kind {
		case KindGap:
			gaps++
			if rec.Gap.Got <= rec.Gap.Expected {
				t.Errorf("gap record has expected=%d got=%d", rec.Gap.Expected, rec.Gap.Got)
			}
		case KindReseed:
			reseeds++
		}
	}
	if gaps != 1 {
		t.Errorf("continued replay delivered %d gap records, want 1", gaps)
	}
	if reseeds != 2 {
		t.Errorf("continued replay delivered %d reseed records, want 2", reseeds)
	}
}
