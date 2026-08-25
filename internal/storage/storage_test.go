package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mustTime(t testing.TB, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return ts
}

// TestKeyLayout pins the key layout down. It is the one thing local disk and S3
// have to agree on, and a window stored under a key nobody can guess again is
// a window that is gone.
func TestKeyLayout(t *testing.T) {
	start := mustTime(t, "2026-08-25T14:35:00Z")

	if got, want := Key("BTC-USD", start),
		"v1/symbol=BTC-USD/date=2026-08-25/hour=14/20260825T143500Z.tape"; got != want {
		t.Errorf("Key = %q, want %q", got, want)
	}
	if got, want := SymbolPrefix("BTC-USD"), "v1/symbol=BTC-USD/"; got != want {
		t.Errorf("SymbolPrefix = %q, want %q", got, want)
	}
	if got, want := DatePrefix("BTC-USD", start), "v1/symbol=BTC-USD/date=2026-08-25/"; got != want {
		t.Errorf("DatePrefix = %q, want %q", got, want)
	}
	if got, want := HourPrefix("BTC-USD", start),
		"v1/symbol=BTC-USD/date=2026-08-25/hour=14/"; got != want {
		t.Errorf("HourPrefix = %q, want %q", got, want)
	}

	// Every prefix has to actually be a prefix of the key it claims to cover,
	// because a prefix scan is the only query this project has.
	k := Key("BTC-USD", start)
	for _, p := range []string{
		SymbolPrefix("BTC-USD"),
		DatePrefix("BTC-USD", start),
		HourPrefix("BTC-USD", start),
	} {
		if len(k) <= len(p) || k[:len(p)] != p {
			t.Errorf("%q is not a prefix of %q", p, k)
		}
	}
}

// TestKeyIsLocalTime checks that a key is built from UTC regardless of the
// instant's own zone. A key that moved with the machine's clock would file the
// same window under two names.
func TestKeyIsUTC(t *testing.T) {
	zone := time.FixedZone("UTC+9", 9*3600)
	utc := mustTime(t, "2026-08-25T23:30:00Z")
	if a, b := Key("BTC-USD", utc), Key("BTC-USD", utc.In(zone)); a != b {
		t.Errorf("same instant in two zones gave two keys:\n %s\n %s", a, b)
	}
	if want := "v1/symbol=BTC-USD/date=2026-08-25/hour=23/20260825T233000Z.tape"; Key("BTC-USD", utc) != want {
		t.Errorf("Key = %q, want %q", Key("BTC-USD", utc), want)
	}
}

// TestKeysSortChronologically is the property replay's file ordering rests on:
// sorting keys as strings must be sorting windows by time, with no parsing.
func TestKeysSortChronologically(t *testing.T) {
	start := mustTime(t, "2026-08-25T22:55:00Z")
	var keys []string
	for i := 0; i < 20; i++ {
		keys = append(keys, Key("BTC-USD", start.Add(time.Duration(i)*5*time.Minute)))
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("keys do not sort chronologically across an hour and a day boundary:\n %s\n %s",
				keys[i-1], keys[i])
		}
	}
}

func TestValidateSymbol(t *testing.T) {
	if err := ValidateSymbol("BTC-USD"); err != nil {
		t.Errorf("BTC-USD rejected: %v", err)
	}
	for _, bad := range []string{"", "a/b", "sym=bol"} {
		if err := ValidateSymbol(bad); err == nil {
			t.Errorf("symbol %q accepted; it would forge a key component", bad)
		}
	}
}

func TestLocalPutListOpen(t *testing.T) {
	ctx := context.Background()
	st := NewLocal(t.TempDir())

	start := mustTime(t, "2026-08-25T14:00:00Z")
	keys := []string{
		Key("BTC-USD", start),
		Key("BTC-USD", start.Add(90*time.Minute)),
		Key("ETH-USD", start),
	}
	for i, k := range keys {
		if err := st.Put(ctx, k, bytes.NewReader([]byte{byte(i), byte(i)})); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	all, err := st.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("List(\"\") = %v, want 3 keys", all)
	}

	btc, err := st.List(ctx, SymbolPrefix("BTC-USD"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(btc) != 2 {
		t.Fatalf("List(BTC-USD) = %v, want 2 keys", btc)
	}
	for i := 1; i < len(btc); i++ {
		if btc[i-1] >= btc[i] {
			t.Errorf("List is not sorted: %v", btc)
		}
	}

	hour, err := st.List(ctx, HourPrefix("BTC-USD", start))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(hour) != 1 || hour[0] != keys[0] {
		t.Errorf("List(hour) = %v, want [%s]", hour, keys[0])
	}

	// A prefix is a string prefix, not a directory path.
	partial, err := st.List(ctx, "v1/symbol=BTC")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(partial) != 2 {
		t.Errorf("List(v1/symbol=BTC) = %v, want the 2 BTC-USD keys", partial)
	}

	rc, err := st.Open(ctx, keys[2])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, []byte{2, 2}) {
		t.Errorf("Open returned %v, want [2 2]", got)
	}
}

// TestLocalListMissingPrefix checks that a prefix naming nothing is an empty
// answer rather than an error. S3 answers that question that way and the two
// stores have to agree.
func TestLocalListMissingPrefix(t *testing.T) {
	st := NewLocal(t.TempDir())
	keys, err := st.List(context.Background(), "v1/symbol=NOPE/")
	if err != nil {
		t.Fatalf("List of a missing prefix: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("List = %v, want empty", keys)
	}
}

func TestLocalOpenMissing(t *testing.T) {
	st := NewLocal(t.TempDir())
	if _, err := st.Open(context.Background(), "v1/symbol=NOPE/x.tape"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of a missing key gave %v, want fs.ErrNotExist", err)
	}
}

// TestLocalPutIsConditional is invariant 3 at the store level: a key, once
// occupied, is never rewritten. The second Put must fail and the stored bytes
// must be the first Put's.
func TestLocalPutIsConditional(t *testing.T) {
	ctx := context.Background()
	st := NewLocal(t.TempDir())
	key := Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))

	if err := st.Put(ctx, key, bytes.NewReader([]byte("first"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Put(ctx, key, bytes.NewReader([]byte("second"))); !errors.Is(err, ErrExists) {
		t.Fatalf("second Put returned %v, want ErrExists", err)
	}

	rc, err := st.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("stored object is %q; the second Put overwrote it", got)
	}
}

// TestLocalPutConcurrentLeavesOneObject is the same rule under a race: many
// writers, one key, exactly one winner and exactly one object. The others must
// all see ErrExists, which is what makes a retried upload safe.
func TestLocalPutConcurrentLeavesOneObject(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := NewLocal(root)
	key := Key("BTC-USD", mustTime(t, "2026-08-25T14:00:00Z"))

	const writers = 16
	var wg sync.WaitGroup
	results := make([]error, writers)
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = st.Put(ctx, key, bytes.NewReader(bytes.Repeat([]byte{byte(i)}, 128)))
		}()
	}
	close(start)
	wg.Wait()

	var won, existed int
	for i, err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrExists):
			existed++
		default:
			t.Errorf("writer %d: %v", i, err)
		}
	}
	if won != 1 {
		t.Errorf("%d writers stored the object, want exactly 1", won)
	}
	if existed != writers-1 {
		t.Errorf("%d writers saw ErrExists, want %d", existed, writers-1)
	}

	// Nothing but the one object may be left in the tree: a temporary that
	// survived would be a half-written object waiting to be mistaken for a real
	// one.
	var files []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(files) != 1 || files[0] != key {
		t.Errorf("store holds %v, want exactly [%s]", files, key)
	}
}
