package replay

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Ext is the file extension a stored window is made of.
const Ext = storage.Ext

// windowFiles lists the tape objects of a window, in the order they are read.
//
// The window is everything under prefix in st. Files are named by their key
// with the prefix taken off, and sorted byte-wise. Capture names objects
// .../hour={hour}/{20060102T150405Z}.tape, and those names sort
// lexicographically into chronological order — a fixed-width UTC timestamp is
// chosen precisely so that sorting the names needs no clock and no parsing.
//
// Naming files relative to the window is what lets the same window replay
// byte-identically out of a local directory and out of a bucket: the store and
// the prefix differ, and nothing the reader emits does.
//
// A window is one symbol. Pointing this at a prefix holding several would
// concatenate them symbol by symbol, which is not a meaningful stream.
func windowFiles(ctx context.Context, st storage.Store, prefix string) ([]string, error) {
	keys, err := st.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var rel []string
	for _, k := range keys {
		if filepath.Ext(k) != Ext {
			continue
		}
		rel = append(rel, strings.TrimPrefix(strings.TrimPrefix(k, prefix), "/"))
	}
	if len(rel) == 0 {
		return nil, fmt.Errorf("replay: no %s objects under %s", Ext, displayRoot(st, prefix))
	}
	// List already sorts; sorting anyway means the order is this package's
	// promise rather than a detail of another one.
	sort.Strings(rel)
	return rel, nil
}

// displayRoot names a window for error messages and for a replay summary.
func displayRoot(st storage.Store, prefix string) string {
	if l, ok := st.(*storage.Local); ok && prefix == "" {
		// A plain local window is identified by the path the caller typed;
		// dressing it up as a URL would only make it harder to paste back.
		return l.Root()
	}
	if prefix == "" {
		return st.String()
	}
	return strings.TrimSuffix(st.String(), "/") + "/" + prefix
}

// source is the arrival-order cursor over a window: it reads the objects in
// order, decodes each record and assigns its ordering key. It is the only place
// that knows about the pinning rule, because pinning is a property of arrival
// order and nothing downstream sees arrival order again.
type source struct {
	ctx    context.Context
	st     storage.Store
	prefix string
	root   string
	files  []string

	idx int // index into files of the open file, or len(files) when done
	rd  *tapefile.Reader
	rec int64 // ordinal of the next record in the open file

	// base is the ordering content of the last record that carried ordering
	// information of its own. Records without any inherit it. It deliberately
	// survives a file change: a window that rotates mid-reconnect must not
	// fling the reseed record back to the start of time.
	base orderKey

	// seenMessage records whether any message has been read yet, which is what
	// distinguishes the reseed that opens a window from one that breaks it.
	seenMessage bool
}

// newSource opens a window held on local disk. root is a directory of tape
// files or a single tape file.
//
// A local directory is a Store rooted at that directory, so the local path and
// the object-store path are the same code from here down. The one thing this
// adds is the single-file case, which has no equivalent in a bucket and is
// worth keeping: pointing the command line at one file is how a window gets
// bisected.
func newSource(root string) (*source, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		if filepath.Ext(root) != Ext {
			return nil, fmt.Errorf("replay: %s is not a %s file", root, Ext)
		}
		dir, base := filepath.Split(root)
		if dir == "" {
			dir = "."
		}
		dir = filepath.Clean(dir)
		return &source{
			ctx:   context.Background(),
			st:    storage.NewLocal(dir),
			root:  dir,
			files: []string{base},
			idx:   -1,
		}, nil
	}
	return newStoreSource(context.Background(), storage.NewLocal(filepath.Clean(root)), "")
}

// newStoreSource opens the window held under prefix in st.
func newStoreSource(ctx context.Context, st storage.Store, prefix string) (*source, error) {
	files, err := windowFiles(ctx, st, prefix)
	if err != nil {
		return nil, err
	}
	return &source{
		ctx:    ctx,
		st:     st,
		prefix: prefix,
		root:   displayRoot(st, prefix),
		files:  files,
		idx:    -1,
	}, nil
}

// key is the store key of the window file at index i.
func (s *source) key(i int) string {
	if s.prefix == "" {
		return s.files[i]
	}
	return strings.TrimSuffix(s.prefix, "/") + "/" + s.files[i]
}

// next returns the next record in arrival order, with its ordering key and its
// stored size. ok is false at the end of the window.
func (s *source) next() (p pending, ok bool, err error) {
	for {
		if s.rd == nil {
			if err := s.openNext(); err != nil {
				return pending{}, false, err
			}
			if s.rd == nil {
				return pending{}, false, nil
			}
		}

		t, payload, err := s.rd.Next()
		if err == io.EOF {
			if cerr := s.rd.Close(); cerr != nil {
				return pending{}, false, cerr
			}
			s.rd = nil
			continue
		}
		if err != nil {
			return pending{}, false, fmt.Errorf("%s: record %d: %w", s.files[s.idx], s.rec, err)
		}

		ordinal := s.rec
		s.rec++

		rec, err := s.decode(t, payload, ordinal)
		if err != nil {
			return pending{}, false, err
		}
		return pending{
			rec:  rec,
			key:  s.keyFor(rec),
			size: int64(len(payload)) + recordOverhead,
		}, true, nil
	}
}

// recordOverhead is the stored type byte plus length prefix, counted so that
// the bytes a replay reports match the bytes capture reported writing.
const recordOverhead = 5

func (s *source) decode(t tapefile.RecordType, payload []byte, ordinal int64) (Record, error) {
	rec := Record{
		Position: Position{File: s.files[s.idx], FileIndex: s.idx, Record: ordinal},
	}
	switch t {
	case tapefile.RecordMessage:
		m, err := tapefile.DecodeMessage(payload)
		if err != nil {
			return Record{}, fmt.Errorf("%s: record %d: %w", s.files[s.idx], ordinal, err)
		}
		rec.Kind = KindMessage
		e, derr := event.Decode(m.Raw, m.Recv)
		if derr != nil {
			// Surfaced, never swallowed: the frame is delivered with the
			// complaint attached so a caller can see the feed changed shape.
			rec.DecodeError = derr.Error()
		}
		rec.Event = e
		s.seenMessage = true

	case tapefile.RecordGap:
		g, err := tapefile.DecodeGap(payload)
		if err != nil {
			return Record{}, fmt.Errorf("%s: record %d: %w", s.files[s.idx], ordinal, err)
		}
		rec.Kind, rec.Gap = KindGap, g

	case tapefile.RecordReseed:
		r, err := tapefile.DecodeReseed(payload)
		if err != nil {
			return Record{}, fmt.Errorf("%s: record %d: %w", s.files[s.idx], ordinal, err)
		}
		rec.Kind, rec.Reseed = KindReseed, r
		rec.Opening = !s.seenMessage

	default:
		// An unknown record type is a file written by a newer build. Guessing
		// at its meaning would be worse than stopping.
		return Record{}, fmt.Errorf("replay: %s: record %d: unknown record type %s",
			s.files[s.idx], ordinal, t)
	}
	return rec, nil
}

// keyFor builds the ordering key and advances the pinning anchor. See the
// package comment for the rules it implements.
func (s *source) keyFor(rec Record) orderKey {
	k := orderKey{file: rec.Position.FileIndex, record: rec.Position.Record}
	if t, ok := rec.exchangeTime(); ok {
		k.exchange = t.UnixNano()
		if rec.Kind == KindMessage && rec.Event.HasSequence {
			k.seqRank, k.sequence = 1, rec.Event.Sequence
		}
		k.channel = rec.Channel()
		s.base = k.content()
		return k
	}
	c := s.base
	k.exchange, k.seqRank, k.sequence, k.channel = c.exchange, c.seqRank, c.sequence, c.channel
	return k
}

// openNext streams the next object in the window. It is a stream, never a
// download: a day of BTC-USD is gigabytes and a replay must not need room for
// it, so the object is read through the same bounded buffer a local file is.
func (s *source) openNext() error {
	s.idx++
	if s.idx >= len(s.files) {
		return nil
	}
	key := s.key(s.idx)
	rc, err := s.st.Open(s.ctx, key)
	if err != nil {
		return err
	}
	rd, err := tapefile.OpenReader(rc)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	s.rd, s.rec = rd, 0
	return nil
}

func (s *source) Close() error {
	if s.rd == nil {
		return nil
	}
	err := s.rd.Close()
	s.rd = nil
	return err
}
