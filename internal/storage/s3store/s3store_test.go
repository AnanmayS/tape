package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/storage/s3store/fakes3"
)

// Everything here runs against an in-process fake. No test in this repository
// needs an AWS account, a credential or a container, which is what keeps the S3
// path a thing CI actually exercises rather than a thing CI skips.

func newFake(t *testing.T, prefix string) (*fakes3.Server, *Store) {
	t.Helper()
	f := fakes3.New("tape-test")
	t.Cleanup(f.Close)
	return f, NewWithClient(f.Client(), "tape-test", prefix)
}

func mustTime(t testing.TB, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return ts
}

func TestPutListOpen(t *testing.T) {
	ctx := context.Background()
	f, st := newFake(t, "")

	start := mustTime(t, "2026-08-25T14:00:00Z")
	keys := []string{
		storage.Key("BTC-USD", start),
		storage.Key("BTC-USD", start.Add(5*time.Minute)),
		storage.Key("BTC-USD", start.Add(90*time.Minute)),
		storage.Key("ETH-USD", start),
	}
	for i, k := range keys {
		if err := st.Put(ctx, k, strings.NewReader(k+"\n")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		if got := len(f.Keys()); got != i+1 {
			t.Fatalf("after %d puts the bucket holds %d objects", i+1, got)
		}
	}

	// The fake pages two keys at a time, so this listing is several round
	// trips and the paginator is doing real work.
	btc, err := st.List(ctx, storage.SymbolPrefix("BTC-USD"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(btc) != 3 {
		t.Fatalf("List(BTC-USD) = %v, want 3 keys", btc)
	}
	for i := 1; i < len(btc); i++ {
		if btc[i-1] >= btc[i] {
			t.Errorf("List is not sorted: %v", btc)
		}
	}

	hour, err := st.List(ctx, storage.HourPrefix("BTC-USD", start))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(hour) != 2 {
		t.Errorf("List(hour) = %v, want the 2 objects in that hour", hour)
	}

	rc, err := st.Open(ctx, keys[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	body, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != keys[0]+"\n" {
		t.Errorf("object body is %q", body)
	}
}

// TestStorePrefix checks that a bucket prefix behaves like a filesystem store's
// root: keys go in and come out relative to it.
func TestStorePrefix(t *testing.T) {
	ctx := context.Background()
	f, st := newFake(t, "captures")

	key := storage.Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	if err := st.Put(ctx, key, strings.NewReader("body")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if got := f.Keys(); len(got) != 1 || got[0] != "captures/"+key {
		t.Fatalf("bucket holds %v, want the key under the store prefix", got)
	}
	listed, err := st.List(ctx, storage.SymbolPrefix("BTC-USD"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0] != key {
		t.Errorf("List = %v, want [%s] — keys come back relative to the store prefix", listed, key)
	}
}

func TestOpenMissing(t *testing.T) {
	_, st := newFake(t, "")
	if _, err := st.Open(context.Background(), "v1/symbol=NOPE/x.tape"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of a missing key gave %v, want fs.ErrNotExist", err)
	}
}

func TestListMissingPrefix(t *testing.T) {
	_, st := newFake(t, "")
	keys, err := st.List(context.Background(), "v1/symbol=NOPE/")
	if err != nil {
		t.Fatalf("List of a missing prefix: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("List = %v, want empty", keys)
	}
}

// TestConditionalPut is the append-only invariant at the bucket: the second Put
// of a key is refused by S3 itself, and the stored object is still the first
// one's bytes.
func TestConditionalPut(t *testing.T) {
	ctx := context.Background()
	f, st := newFake(t, "")
	key := storage.Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))

	if err := st.Put(ctx, key, strings.NewReader("first")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Put(ctx, key, strings.NewReader("second")); !errors.Is(err, storage.ErrExists) {
		t.Fatalf("second Put returned %v, want ErrExists", err)
	}

	body, ok := f.Object(key)
	if !ok {
		t.Fatal("object is gone")
	}
	if string(body) != "first" {
		t.Errorf("stored object is %q; the refused put still changed it", body)
	}
	if got := f.Keys(); len(got) != 1 {
		t.Errorf("bucket holds %v, want exactly one object", got)
	}
}

// TestConcurrentPutsLeaveOneObject is the same rule under a race. Sixteen
// writers, one key: exactly one stores it, the rest are told it exists, and the
// bucket holds one object.
func TestConcurrentPutsLeaveOneObject(t *testing.T) {
	ctx := context.Background()
	f, st := newFake(t, "")
	key := storage.Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))

	const writers = 16
	results := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = st.Put(ctx, key, bytes.NewReader(bytes.Repeat([]byte{byte('a' + i)}, 64)))
		}()
	}
	close(start)
	wg.Wait()

	var won, existed int
	for i, err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, storage.ErrExists):
			existed++
		default:
			t.Errorf("writer %d: %v", i, err)
		}
	}
	if won != 1 {
		t.Errorf("%d writers stored the object, want exactly 1", won)
	}
	if existed != writers-1 {
		t.Errorf("%d writers were told it exists, want %d", existed, writers-1)
	}
	if got := f.Keys(); len(got) != 1 || got[0] != key {
		t.Errorf("bucket holds %v, want exactly [%s]", got, key)
	}
	if puts, _, _ := f.Requests(); puts != writers {
		t.Errorf("bucket saw %d put requests, want %d", puts, writers)
	}
}

// TestRetriedUploadLeavesOneObject is the end-to-end version of the same
// claim, through the uploader that capture actually uses: a transport failure
// after the object landed makes the uploader try again, and the retry must find
// the object rather than replace it.
func TestRetriedUploadLeavesOneObject(t *testing.T) {
	f, st := newFake(t, "")
	key := storage.Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	path := stagedFile(t, "the only copy")

	// The object is already in the bucket and the next request fails: between
	// them, that is an attempt that landed and then lost its response. The
	// uploader cannot tell that from an attempt that never landed, and does not
	// have to — the retry asks S3, and S3 answers 412.
	f.Put(key, []byte("the only copy"))
	f.FailNextPuts(1)

	u := storage.NewUploader(st, testUploadConfig())
	u.Add(path, key)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := u.Stats()
	if s.Existed != 1 || s.Uploaded != 0 || s.Failed != 0 {
		t.Errorf("stats = %+v, want the upload to find its object already stored", s)
	}
	if puts, _, _ := f.Requests(); puts != 2 {
		t.Errorf("bucket saw %d put requests, want 2 (the failure and the retry)", puts)
	}
	if got := f.Keys(); len(got) != 1 || got[0] != key {
		t.Errorf("bucket holds %v, want exactly [%s]", got, key)
	}
	body, _ := f.Object(key)
	if string(body) != "the only copy" {
		t.Errorf("object is %q; the retry overwrote it", body)
	}
}

// TestUploadRetriesThroughFailures checks the ordinary retry: the store is
// unreachable for a while, then is not, and exactly one object results.
func TestUploadRetriesThroughFailures(t *testing.T) {
	f, st := newFake(t, "")
	key := storage.Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	path := stagedFile(t, "window bytes")

	f.FailNextPuts(3)
	u := storage.NewUploader(st, testUploadConfig())
	u.Add(path, key)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := u.Stats()
	if s.Uploaded != 1 || s.Retries != 3 {
		t.Errorf("stats = %+v, want one upload after three retries", s)
	}
	if puts, _, _ := f.Requests(); puts != 4 {
		t.Errorf("bucket saw %d put requests, want 4", puts)
	}
	if got := f.Keys(); len(got) != 1 || got[0] != key {
		t.Errorf("bucket holds %v, want exactly [%s]", got, key)
	}
	body, _ := f.Object(key)
	if string(body) != "window bytes" {
		t.Errorf("object is %q", body)
	}
}

func stagedFile(t testing.TB, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "20260825T140000Z.tape")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("stage file: %v", err)
	}
	return p
}

// testUploadConfig runs the production retry loop with the backoff wound down
// to microseconds, so a retry sequence costs no wall-clock time.
func testUploadConfig() storage.UploadConfig {
	return storage.UploadConfig{
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Base:  time.Microsecond,
		Max:   time.Microsecond,
		Drain: 10 * time.Second,
	}
}
