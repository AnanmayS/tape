package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// flaky wraps a Store and stages failures in front of it, so that the retry
// path is exercised against a store that otherwise behaves exactly like the
// real one.
type flaky struct {
	Store

	mu sync.Mutex

	// fail is how many Puts still fail before one is allowed through.
	fail int

	// writeThenFail makes a Put store the object and then report a failure:
	// the lost-response case, and the whole reason Put is conditional.
	writeThenFail int

	puts int
}

var errFlaky = errors.New("flaky: store unreachable")

func (f *flaky) Put(ctx context.Context, key string, r io.Reader) error {
	f.mu.Lock()
	f.puts++
	fail, writeThenFail := f.fail, f.writeThenFail
	if fail > 0 {
		f.fail--
	} else if writeThenFail > 0 {
		f.writeThenFail--
	}
	f.mu.Unlock()

	switch {
	case fail > 0:
		return errFlaky
	case writeThenFail > 0:
		if err := f.Store.Put(ctx, key, r); err != nil {
			return err
		}
		return errFlaky
	default:
		return f.Store.Put(ctx, key, r)
	}
}

func (f *flaky) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testUploadConfig is the shared configuration: no real sleeping, so a six
// attempt backoff sequence costs nothing.
func testUploadConfig() UploadConfig {
	return UploadConfig{
		Log:   quietLog(),
		Drain: 10 * time.Second,
		sleep: func(time.Duration) bool { return true },
	}
}

// stagedFile writes a local file to stand in for a closed tape file.
func stagedFile(t testing.TB, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "20260825T140000Z.tape")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write staged file: %v", err)
	}
	return p
}

func objectBody(t testing.TB, st Store, key string) string {
	t.Helper()
	rc, err := st.Open(context.Background(), key)
	if err != nil {
		t.Fatalf("Open %s: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll %s: %v", key, err)
	}
	return string(b)
}

func TestUploaderRetriesUntilItLands(t *testing.T) {
	dst := NewLocal(t.TempDir())
	st := &flaky{Store: dst, fail: 3}
	key := Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	path := stagedFile(t, "window bytes")

	u := NewUploader(st, testUploadConfig())
	u.Add(path, key)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := u.Stats()
	if s.Uploaded != 1 || s.Failed != 0 {
		t.Errorf("stats = %+v, want one upload and no failures", s)
	}
	if s.Retries != 3 {
		t.Errorf("retries = %d, want 3", s.Retries)
	}
	if s.Bytes != int64(len("window bytes")) {
		t.Errorf("bytes = %d, want %d", s.Bytes, len("window bytes"))
	}
	if got := objectBody(t, dst, key); got != "window bytes" {
		t.Errorf("stored %q, want the file's bytes", got)
	}
}

// TestUploaderRetryAfterLostResponse is the case the conditional put exists
// for. The first attempt stores the object and then reports a failure, exactly
// as a dropped response would. The retry must find the object already there,
// must not overwrite it, and must count as a success rather than an error.
func TestUploaderRetryAfterLostResponse(t *testing.T) {
	root := t.TempDir()
	dst := NewLocal(root)
	st := &flaky{Store: dst, writeThenFail: 1}
	key := Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	path := stagedFile(t, "stored once")

	u := NewUploader(st, testUploadConfig())
	u.Add(path, key)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := u.Stats()
	if s.Existed != 1 {
		t.Errorf("stats = %+v, want the retry to find the object already stored", s)
	}
	if s.Failed != 0 {
		t.Errorf("a retry that finds its own object is not a failure: %+v", s)
	}
	if st.attempts() != 2 {
		t.Errorf("store saw %d puts, want 2", st.attempts())
	}
	if got := objectBody(t, dst, key); got != "stored once" {
		t.Errorf("stored %q, want the first attempt's bytes", got)
	}
	if n := countObjects(t, root); n != 1 {
		t.Errorf("a retried upload left %d objects, want exactly 1", n)
	}
}

// TestUploaderGivesUpLoudlyWithoutLosingData holds the second rule: an upload
// that cannot be made to work costs a re-run, never a capture and never a byte.
func TestUploaderGivesUpLoudlyWithoutLosingData(t *testing.T) {
	root := t.TempDir()
	dst := NewLocal(root)
	st := &flaky{Store: dst, fail: 1000}
	key := Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))
	path := stagedFile(t, "never uploaded")

	cfg := testUploadConfig()
	cfg.Attempts = 4
	u := NewUploader(st, cfg)
	u.Add(path, key)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := u.Stats()
	if s.Failed != 1 || s.Uploaded != 0 {
		t.Errorf("stats = %+v, want exactly one failure", s)
	}
	if st.attempts() != 4 {
		t.Errorf("store saw %d puts, want the configured 4 attempts", st.attempts())
	}
	if n := countObjects(t, root); n != 0 {
		t.Errorf("store holds %d objects after a failed upload, want 0", n)
	}

	// The whole point: the local file is untouched and complete.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("local file gone after a failed upload: %v", err)
	}
	if string(b) != "never uploaded" {
		t.Errorf("local file is %q, want it intact", b)
	}
}

// TestUploaderAddNeverBlocks checks that a backed-up queue drops files loudly
// instead of stalling the goroutine that is reading the socket.
func TestUploaderAddNeverBlocks(t *testing.T) {
	block := make(chan struct{})
	st := &blocking{Store: NewLocal(t.TempDir()), release: block}

	cfg := testUploadConfig()
	cfg.Queue = 2
	cfg.Drain = 5 * time.Second
	u := NewUploader(st, cfg)

	path := stagedFile(t, "x")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 32 {
			u.Add(path, Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z").Add(time.Duration(i)*time.Minute)))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Add blocked; capture must never wait on an upload")
	}

	close(block)
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s := u.Stats()
	if s.Added != 32 {
		t.Errorf("added = %d, want 32", s.Added)
	}
	if s.Dropped == 0 {
		t.Error("no file was dropped; the queue cannot have been the thing that gave way")
	}
	if s.Pending() != 0 {
		t.Errorf("stats do not account for every file: %+v", s)
	}
}

// blocking holds the first Put until release is closed, so the queue behind it
// fills up.
type blocking struct {
	Store
	release chan struct{}
}

func (b *blocking) Put(ctx context.Context, key string, r io.Reader) error {
	<-b.release
	return b.Store.Put(ctx, key, r)
}

// TestUploaderPutsEveryWindowUnderItsOwnKey checks the ordinary path: several
// rotations, several keys, one object each.
func TestUploaderPutsEveryWindowUnderItsOwnKey(t *testing.T) {
	root := t.TempDir()
	dst := NewLocal(root)
	u := NewUploader(dst, testUploadConfig())

	start := mustTime(t, "2026-08-25T13:55:00Z")
	var keys []string
	for i := range 4 {
		at := start.Add(time.Duration(i) * 5 * time.Minute)
		key := Key("BTC-USD", at)
		keys = append(keys, key)
		u.Add(stagedFile(t, key), key)
	}
	if err := u.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if s := u.Stats(); s.Uploaded != 4 {
		t.Fatalf("stats = %+v, want 4 uploads", s)
	}
	got, err := dst.List(context.Background(), SymbolPrefix("BTC-USD"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("store holds %v, want 4 objects", got)
	}
	for _, k := range keys {
		if body := objectBody(t, dst, k); body != k {
			t.Errorf("object %s holds %q", k, body)
		}
	}
	// The rotation at 14:00 crosses an hour boundary, so the four windows are
	// split across two hour partitions rather than filed under the hour capture
	// happened to start in.
	hours := map[string]int{}
	for _, k := range got {
		hours[filepath.Base(filepath.Dir(filepath.FromSlash(k)))]++
	}
	if hours["hour=13"] != 1 || hours["hour=14"] != 3 {
		t.Errorf("hour partitions = %v, want 1 in hour=13 and 3 in hour=14", hours)
	}
}

func countObjects(t testing.TB, root string) int {
	t.Helper()
	var n int
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return n
}
