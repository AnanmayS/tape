package replay

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/AnanmayS/tape/internal/storage"
)

// DefaultReorderWindow is how many records the reorder buffer holds.
//
// Stored order is arrival order and sorted order is exchange order; they differ
// by however far a message can be displaced, which on Coinbase is one
// level2_batch interval — tens of milliseconds, a handful of records. 4096 is
// several thousand times that margin and costs a few MiB of pointers. It is a
// ceiling that has to be crossed by something genuinely pathological, and if it
// is crossed the replay fails rather than lying.
const DefaultReorderWindow = 4096

// Option configures a Reader.
type Option func(*config)

type config struct {
	reorder       int
	continueOnGap bool
}

// WithReorderWindow sets how many records the reorder buffer holds. A larger
// window tolerates more displacement between stored and sorted order at the
// cost of memory; it does not change the order that comes out.
func WithReorderWindow(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.reorder = n
		}
	}
}

// WithContinueOnGap lets replay read past a discontinuity.
//
// It does not hide one. Gap and reseed records are still delivered through
// Next, and every discontinuity crossed is listed by Discontinuities, so a
// caller that continues still knows exactly which window it is holding. This
// option is the only way past a gap; there is no path that continues silently.
func WithContinueOnGap() Option {
	return func(c *config) { c.continueOnGap = true }
}

// DiscontinuityKind says what broke.
type DiscontinuityKind uint8

const (
	// DiscontinuityGap is a stored gap record: sequence numbers prove messages
	// were lost.
	DiscontinuityGap DiscontinuityKind = iota + 1

	// DiscontinuityReseed is a stored reseed record other than the one opening
	// the window: the book was rebuilt here, so nothing across it is continuous.
	DiscontinuityReseed
)

func (k DiscontinuityKind) String() string {
	if k == DiscontinuityGap {
		return "gap"
	}
	return "reseed"
}

// Discontinuity is a break in the window's continuity. Its presence makes the
// window untrustworthy: the public feed offers no backfill, so nothing can
// reconstruct what is missing.
type Discontinuity struct {
	Kind     DiscontinuityKind
	Position Position

	// At is when capture noticed it, on the local clock.
	At time.Time

	// Expected and Got are the sequence numbers of a gap.
	Expected, Got uint64

	// Dropped is how many frames capture discarded here under a backpressure
	// policy that sheds load. Non-zero means the loss was this process's doing
	// rather than the exchange's, which is a different conversation to have and
	// so a different number to report.
	Dropped uint64

	// Reason is the reconnect reason of a reseed.
	Reason string
}

func (d Discontinuity) String() string {
	switch d.Kind {
	case DiscontinuityGap:
		if d.Dropped > 0 {
			return fmt.Sprintf("gap at %s record %d: capture dropped %d frames at %s",
				d.Position.File, d.Position.Record, d.Dropped, d.At.Format(time.RFC3339Nano))
		}
		return fmt.Sprintf("gap at %s record %d: expected sequence %d, got %d (%d missing) at %s",
			d.Position.File, d.Position.Record, d.Expected, d.Got,
			int64(d.Got)-int64(d.Expected), d.At.Format(time.RFC3339Nano))
	default:
		return fmt.Sprintf("reseed at %s record %d: %s at %s",
			d.Position.File, d.Position.Record, d.Reason, d.At.Format(time.RFC3339Nano))
	}
}

// ErrDiscontinuity is returned, wrapped in a DiscontinuityError, when replay
// reaches a gap or a mid-window reseed without WithContinueOnGap.
var ErrDiscontinuity = errors.New("replay: window is discontinuous here")

// ErrOutOfOrder is returned, wrapped in an OutOfOrderError, when the reorder
// buffer was too small to sort the window.
var ErrOutOfOrder = errors.New("replay: record emerged out of order")

// DiscontinuityError stops a replay at a gap or a mid-window reseed.
type DiscontinuityError struct {
	D Discontinuity
}

// Error deliberately says only what happened. The way past a discontinuity is
// spelled differently by the library and by the command line, so each names its
// own remedy rather than this text naming one of them.
func (e *DiscontinuityError) Error() string { return e.D.String() }

func (e *DiscontinuityError) Unwrap() error { return ErrDiscontinuity }

// OutOfOrderError reports that the reorder buffer was smaller than the window's
// displacement between stored and sorted order. It is a failure, not a warning:
// the alternative is an output that is quietly not in the documented order.
type OutOfOrderError struct {
	Position Position
	Window   int
}

func (e *OutOfOrderError) Error() string {
	return fmt.Sprintf(
		"replay: %s record %d sorts before a record already emitted; "+
			"reorder window of %d is too small for this window",
		e.Position.File, e.Position.Record, e.Window)
}

func (e *OutOfOrderError) Unwrap() error { return ErrOutOfOrder }

// Stats counts what a replay delivered. Every field is counted, not estimated,
// and reflects records actually emitted — a replay stopped at a gap reports the
// part it read.
type Stats struct {
	Records  int64
	Messages int64
	Gaps     int64
	Reseeds  int64

	// Bytes is the stored size of the records delivered: payloads plus the
	// per-record type and length prefix. It excludes each file's 8-byte header,
	// so a complete replay reports the capture writer's byte count less 8 per
	// file. A replay stopped at a gap reports only what it handed over.
	Bytes int64

	// FirstTime and LastTime bound the window on the local receive clock. Their
	// difference is the wall-clock time the window took to capture, which is
	// what replay throughput is worth comparing against.
	FirstTime, LastTime time.Time
}

// Span is the wall-clock duration the window covers.
func (s Stats) Span() time.Duration {
	if s.FirstTime.IsZero() || s.LastTime.IsZero() {
		return 0
	}
	return s.LastTime.Sub(s.FirstTime)
}

// Reader replays one window in the package's total order.
//
// It is an iterator rather than a range-over-func sequence on purpose. A
// replay can stop for reasons the caller must act on — a gap, an unsortable
// window — and `for rec, err := range` makes ignoring the second value a
// single keystroke. Next returns an error the caller has to look at, which is
// the whole point of invariant 2.
//
// A Reader is not safe for concurrent use.
type Reader struct {
	src *source
	cfg config

	buf     pendingHeap
	last    orderKey
	hasLast bool
	emitted int64

	// sticky is returned by every Next after the first failure or io.EOF, so a
	// caller that keeps iterating past a gap cannot accidentally resume.
	sticky error

	disc  []Discontinuity
	stats Stats
}

// Open opens a window held on local disk. root is a directory of tape files or
// a single tape file. Close the Reader when done.
func Open(root string, opts ...Option) (*Reader, error) {
	src, err := newSource(root)
	if err != nil {
		return nil, err
	}
	return newReader(src, opts), nil
}

// OpenStore opens the window held under prefix in st — a bucket, a directory,
// anything satisfying storage.Store. Objects are streamed as they are read, not
// downloaded first: a day of BTC-USD does not fit anywhere a replay should
// insist on putting it.
//
// The output is identical to Open's for the same window. Files are named
// relative to prefix, and every other field of the canonical form comes from
// bytes in the objects, so where a window was stored is not something a
// replay of it can reveal.
func OpenStore(ctx context.Context, st storage.Store, prefix string, opts ...Option) (*Reader, error) {
	src, err := newStoreSource(ctx, st, prefix)
	if err != nil {
		return nil, err
	}
	return newReader(src, opts), nil
}

func newReader(src *source, opts []Option) *Reader {
	cfg := config{reorder: DefaultReorderWindow}
	for _, o := range opts {
		o(&cfg)
	}
	return &Reader{src: src, cfg: cfg, buf: make(pendingHeap, 0, cfg.reorder+1)}
}

// Files returns the window's files, relative to its root, in read order.
func (r *Reader) Files() []string {
	return append([]string(nil), r.src.files...)
}

// Root names the window the files are relative to: a local directory, or a
// store and prefix.
func (r *Reader) Root() string { return r.src.root }

// Stats returns what has been delivered so far.
func (r *Reader) Stats() Stats { return r.stats }

// Discontinuities returns every gap and mid-window reseed reached so far.
// Non-empty means the window is untrustworthy.
func (r *Reader) Discontinuities() []Discontinuity {
	return append([]Discontinuity(nil), r.disc...)
}

// Trustworthy reports whether the replay has crossed no discontinuity.
func (r *Reader) Trustworthy() bool { return len(r.disc) == 0 }

// Next returns the next record in the total order. It returns io.EOF at the end
// of the window, and keeps returning whatever error it first returned.
func (r *Reader) Next() (Record, error) {
	if r.sticky != nil {
		return Record{}, r.sticky
	}
	if err := r.fill(); err != nil {
		return Record{}, r.fail(err)
	}
	if len(r.buf) == 0 {
		return Record{}, r.fail(io.EOF)
	}

	p := heap.Pop(&r.buf).(pending)
	if r.hasLast && compareKeys(p.key, r.last) < 0 {
		return Record{}, r.fail(&OutOfOrderError{Position: p.rec.Position, Window: r.cfg.reorder})
	}
	r.last, r.hasLast = p.key, true

	rec := p.rec
	rec.Index = r.emitted
	r.emitted++
	r.count(rec, p.size)

	if d, ok := discontinuityOf(rec); ok {
		r.disc = append(r.disc, d)
		if !r.cfg.continueOnGap {
			return Record{}, r.fail(&DiscontinuityError{D: d})
		}
	}
	return rec, nil
}

// fill tops the reorder buffer back up to its capacity. Holding the buffer full
// before every pop is what makes the emitted order a function of the stored
// records alone.
func (r *Reader) fill() error {
	for len(r.buf) < r.cfg.reorder {
		p, ok, err := r.src.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		heap.Push(&r.buf, p)
	}
	return nil
}

func (r *Reader) count(rec Record, size int64) {
	r.stats.Records++
	r.stats.Bytes += size
	switch rec.Kind {
	case KindMessage:
		r.stats.Messages++
	case KindGap:
		r.stats.Gaps++
	case KindReseed:
		r.stats.Reseeds++
	}
	if t := rec.Time(); !t.IsZero() {
		if r.stats.FirstTime.IsZero() || t.Before(r.stats.FirstTime) {
			r.stats.FirstTime = t
		}
		if t.After(r.stats.LastTime) {
			r.stats.LastTime = t
		}
	}
}

// fail records and returns the terminal error.
func (r *Reader) fail(err error) error {
	r.sticky = err
	return err
}

// Close releases the open file. It is safe to call more than once.
func (r *Reader) Close() error { return r.src.Close() }

// discontinuityOf reports whether a record breaks the window's continuity.
// The reseed that opens a window does not: there is nothing before it.
func discontinuityOf(rec Record) (Discontinuity, bool) {
	switch {
	case rec.Kind == KindGap:
		return Discontinuity{
			Kind:     DiscontinuityGap,
			Position: rec.Position,
			At:       rec.Gap.At,
			Expected: rec.Gap.Expected,
			Got:      rec.Gap.Got,
			Dropped:  rec.Gap.Dropped,
		}, true
	case rec.Kind == KindReseed && !rec.Opening:
		return Discontinuity{
			Kind:     DiscontinuityReseed,
			Position: rec.Position,
			At:       rec.Reseed.At,
			Reason:   rec.Reseed.Reason,
		}, true
	default:
		return Discontinuity{}, false
	}
}

// pending is a record on its way through the reorder buffer: the record, the
// key it sorts by, and the number of bytes it occupied on disk.
type pending struct {
	rec  Record
	key  orderKey
	size int64
}

// pendingHeap is a min-heap on the ordering key. The key is a strict total
// order, so the heap never has to break a tie and its unstable-by-nature
// ordering is still deterministic.
type pendingHeap []pending

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return compareKeys(h[i].key, h[j].key) < 0 }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(pending)) }
func (h *pendingHeap) Pop() (x any)      { old := *h; n := len(old); x, *h = old[n-1], old[:n-1]; return x }
