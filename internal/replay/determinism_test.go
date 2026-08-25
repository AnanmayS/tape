package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math/rand/v2"
	"sort"
	"testing"
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
		rec, key, ok, err := src.next()
		if err != nil {
			t.Fatalf("source.next: %v", err)
		}
		if !ok {
			break
		}
		all = append(all, pending{rec: rec, key: key})
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
