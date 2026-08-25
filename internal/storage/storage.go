// Package storage is the object store this project keeps tape files in, and
// the key layout it keeps them under.
//
// The access pattern is the whole design brief. Capture writes an immutable
// object once; replay lists the objects under a prefix and streams them back.
// That is three operations, and the interface has three operations. There is no
// delete, no overwrite, no rename and no metadata call, because adding one
// would be adding a way to break invariant 3 — capture is append-only — and the
// cheapest way to keep a rule is to leave no method that can break it.
//
// # Keys
//
//	v1/symbol=BTC-USD/date=2026-08-25/hour=14/20260825T140000Z.tape
//
// One layout, on local disk and on S3 alike. A local directory is a Store
// rooted at a path; an S3 bucket is a Store rooted at a bucket and prefix. The
// same key names the same window in both, so a window can be replayed from
// either without the reader knowing which it got.
//
// Every component is a fixed-width, byte-sortable field. Sorting keys as
// strings puts them in chronological order without parsing a timestamp or
// consulting a clock, which is what lets replay's file ordering be a promise
// rather than a hope. The Hive-style key=value components are not decoration:
// a prefix scan for one symbol, one day or one hour is then a literal string
// prefix, which is the only query this project has.
//
// The leading v1 is the layout version, not the file format version. A future
// repartitioning writes v2 alongside and leaves v1 readable, because rewriting
// stored objects is the one thing this project does not do.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Ext is the extension every stored tape object carries.
const Ext = ".tape"

// LayoutVersion is the leading key component. See the package comment.
const LayoutVersion = "v1"

// TimeLayout formats the window start that names an object. It is fixed-width
// and UTC, so keys sort lexicographically into chronological order.
const TimeLayout = "20060102T150405Z"

// Store is an object store holding immutable tape objects.
//
// Keys are slash-separated and relative to the store's own root — a directory
// for a filesystem store, a bucket and prefix for an S3 one. A caller therefore
// never has to know which kind it holds.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Put stores r under key. It is conditional and always conditional: if key
	// already exists, nothing is written and the error is ErrExists. That is
	// what makes a retried upload safe — the second attempt cannot overwrite
	// the object the first one landed — and it is invariant 3 enforced by the
	// store rather than by the caller remembering.
	Put(ctx context.Context, key string, r io.Reader) error

	// List returns every key beginning with prefix, sorted byte-wise. A prefix
	// matching nothing returns an empty list and no error: absence of objects
	// is an answer, not a failure.
	List(ctx context.Context, prefix string) ([]string, error)

	// Open streams the object at key. The caller closes it. A missing key
	// reports an error satisfying errors.Is(err, fs.ErrNotExist).
	Open(ctx context.Context, key string) (io.ReadCloser, error)

	// String names the store for logs and for a replay summary.
	String() string
}

// ErrExists is returned by Put when the key is already occupied. It is not
// necessarily a failure: an upload retried after an ambiguous error is expected
// to find its own object already there, and treating that as success is how a
// retry stays idempotent. See Upload.
var ErrExists = errors.New("storage: object already exists")

// Key returns the object key for the window of symbol starting at start.
func Key(symbol string, start time.Time) string {
	t := start.UTC()
	return HourPrefix(symbol, t) + t.Format(TimeLayout) + Ext
}

// SymbolPrefix is the prefix covering every window of one symbol.
func SymbolPrefix(symbol string) string {
	return LayoutVersion + "/symbol=" + symbol + "/"
}

// DatePrefix is the prefix covering one UTC day of one symbol. It is the usual
// unit of replay: "give me BTC-USD for the 25th".
func DatePrefix(symbol string, day time.Time) string {
	return SymbolPrefix(symbol) + "date=" + day.UTC().Format("2006-01-02") + "/"
}

// HourPrefix is the prefix covering one UTC hour of one symbol.
func HourPrefix(symbol string, hour time.Time) string {
	return DatePrefix(symbol, hour) + "hour=" + hour.UTC().Format("15") + "/"
}

// ValidateSymbol rejects a symbol that would not survive the key layout. A
// slash would invent a partition level and an "=" would forge a field, so
// neither is allowed anywhere near a key.
func ValidateSymbol(symbol string) error {
	switch {
	case symbol == "":
		return errors.New("storage: symbol must not be empty")
	case strings.ContainsAny(symbol, "/="):
		return fmt.Errorf("storage: symbol %q must not contain / or =", symbol)
	default:
		return nil
	}
}

// joinPrefix glues a store-relative prefix to a key, tolerating a prefix given
// with or without its trailing slash. Every caller-facing prefix in this
// package ends in one; a human typing one on a command line often does not.
func joinPrefix(prefix, key string) string {
	switch {
	case prefix == "":
		return key
	case strings.HasSuffix(prefix, "/"):
		return prefix + key
	default:
		return prefix + "/" + key
	}
}
