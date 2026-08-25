package capture

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/storage"
)

// rotatingFeed is the shape every test here uses: enough frames, on a fixed
// clock, to fill three one-minute windows exactly. The initial reseed takes the
// 04:00:00 tick, so the data frames run 04:00:01 through 04:02:59.
func rotatingFeed() *feed.Synthetic {
	return &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     179,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
}

// testUploadConfig runs the production retry loop with the backoff wound down
// to microseconds, so a retry sequence costs no wall-clock time.
func testUploadConfig() storage.UploadConfig {
	return storage.UploadConfig{
		Log:   quietLogger(),
		Base:  time.Microsecond,
		Max:   time.Microsecond,
		Drain: 10 * time.Second,
	}
}

// TestCaptureUploadsEveryClosedFile is the M4 capture path: each window is
// uploaded as it closes, under the key its local path already spells, and the
// last window — the one closed by shutdown rather than by a rotation — is not
// the one that gets left behind.
func TestCaptureUploadsEveryClosedFile(t *testing.T) {
	root := t.TempDir()
	bucket := storage.NewLocal(t.TempDir())

	sum, err := Run(context.Background(), rotatingFeed(), Config{
		Root:          root,
		Store:         bucket,
		Upload:        testUploadConfig(),
		Window:        time.Minute,
		FlushInterval: time.Hour,
		Log:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(sum.Files) != 3 {
		t.Fatalf("captured %v, want 3 files", sum.Files)
	}
	if sum.Upload.Uploaded != 3 || sum.Upload.Failed != 0 || sum.Upload.Dropped != 0 {
		t.Fatalf("upload stats = %+v, want 3 uploads and nothing lost", sum.Upload)
	}

	keys, err := bucket.List(context.Background(), storage.SymbolPrefix("BTC-USD"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("store holds %v, want 3 objects", keys)
	}
	want := []string{
		"v1/symbol=BTC-USD/date=2026-08-25/hour=04/20260825T040000Z.tape",
		"v1/symbol=BTC-USD/date=2026-08-25/hour=04/20260825T040100Z.tape",
		"v1/symbol=BTC-USD/date=2026-08-25/hour=04/20260825T040200Z.tape",
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("object %d is %q, want %q", i, keys[i], k)
		}
	}

	// Every object must be the file, byte for byte. An upload that truncated
	// a window would look exactly like a successful one from the counters.
	for i, key := range keys {
		local, err := os.ReadFile(sum.Files[i])
		if err != nil {
			t.Fatalf("read %s: %v", sum.Files[i], err)
		}
		if got := filepath.ToSlash(strings.TrimPrefix(sum.Files[i], root+string(filepath.Separator))); got != key {
			t.Errorf("file %s does not sit at its own key %s", got, key)
		}
		rc, err := bucket.Open(context.Background(), key)
		if err != nil {
			t.Fatalf("Open %s: %v", key, err)
		}
		stored, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("ReadAll %s: %v", key, err)
		}
		if !bytes.Equal(local, stored) {
			t.Errorf("object %s is %d bytes, the file is %d", key, len(stored), len(local))
		}
	}
	if sum.Upload.Bytes != sum.Bytes {
		t.Errorf("uploaded %d bytes, wrote %d", sum.Upload.Bytes, sum.Bytes)
	}
}

// TestCaptureSurvivesAnUnreachableStore is the rule that matters most: an
// unreachable bucket costs a re-upload, never a frame. Capture must finish,
// every record must be on disk, and the failure must be reported rather than
// swallowed.
func TestCaptureSurvivesAnUnreachableStore(t *testing.T) {
	root := t.TempDir()
	dead := &deadStore{}

	cfg := testUploadConfig()
	cfg.Attempts = 2
	sum, err := Run(context.Background(), rotatingFeed(), Config{
		Root:          root,
		Store:         dead,
		Upload:        cfg,
		Window:        time.Minute,
		FlushInterval: time.Hour,
		Log:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("Run: %v; an unreachable store must not fail a capture", err)
	}

	if sum.Messages != 179 {
		t.Errorf("captured %d messages, want 179 — the store took data with it", sum.Messages)
	}
	if sum.Upload.Failed != 3 || sum.Upload.Uploaded != 0 {
		t.Errorf("upload stats = %+v, want three loud failures", sum.Upload)
	}
	if sum.Upload.Pending() != 0 {
		t.Errorf("upload stats do not account for every file: %+v", sum.Upload)
	}

	// The point of surviving: everything is still on local disk, complete, and
	// still readable as the window it is.
	got := readAll(t, sum.Files)
	if got.messages != int(sum.Messages) {
		t.Fatalf("on disk %d messages, summary says %d", got.messages, sum.Messages)
	}
}

// deadStore is a store that is never reachable.
type deadStore struct{}

var errDead = errors.New("deadStore: unreachable")

func (deadStore) Put(context.Context, string, io.Reader) error { return errDead }
func (deadStore) List(context.Context, string) ([]string, error) {
	return nil, errDead
}
func (deadStore) Open(context.Context, string) (io.ReadCloser, error) { return nil, errDead }
func (deadStore) String() string                                      { return "dead://nowhere" }

// TestCaptureWithoutAStoreUploadsNothing checks the default path is untouched:
// no bucket configured, no uploader, no counters, and the same files on disk.
func TestCaptureWithoutAStoreUploadsNothing(t *testing.T) {
	sum, got := runCapture(t, rotatingFeed(), Config{Window: time.Minute, FlushInterval: time.Hour})
	if sum.Store != "" {
		t.Errorf("summary names store %q with none configured", sum.Store)
	}
	if (sum.Upload != storage.UploadStats{}) {
		t.Errorf("upload stats = %+v with no store configured", sum.Upload)
	}
	if got.messages != 179 {
		t.Errorf("messages on disk = %d, want 179", got.messages)
	}
}
