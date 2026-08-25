package replay

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Ext is the file extension a stored window is made of.
const Ext = ".tape"

// windowFiles lists the tape files of a window, in the order they are read.
//
// root may be a single file or a directory; a directory is walked recursively.
// Files are sorted by their path relative to root, compared byte-wise. Capture
// names files {symbol}/{date}/{20060102T150405Z}.tape, and those names sort
// lexicographically into chronological order — a fixed-width UTC timestamp is
// chosen precisely so that sorting the names needs no clock and no parsing.
//
// A window is one symbol. Pointing this at a root holding several would
// concatenate them symbol by symbol, which is not a meaningful stream.
func windowFiles(root string) (string, []string, error) {
	st, err := os.Stat(root)
	if err != nil {
		return "", nil, err
	}
	if !st.IsDir() {
		if filepath.Ext(root) != Ext {
			return "", nil, fmt.Errorf("replay: %s is not a %s file", root, Ext)
		}
		dir, base := filepath.Split(root)
		if dir == "" {
			dir = "."
		}
		return filepath.Clean(dir), []string{base}, nil
	}

	var rel []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(p) != Ext {
			return nil
		}
		r, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = append(rel, filepath.ToSlash(r))
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	if len(rel) == 0 {
		return "", nil, fmt.Errorf("replay: no %s files under %s", Ext, root)
	}
	// WalkDir already walks in lexical order; sorting anyway means the order is
	// this package's promise rather than a detail of another one.
	sort.Strings(rel)
	return filepath.Clean(root), rel, nil
}

// source is the arrival-order cursor over a window: it reads the files in
// order, decodes each record and assigns its ordering key. It is the only place
// that knows about the pinning rule, because pinning is a property of arrival
// order and nothing downstream sees arrival order again.
type source struct {
	root  string
	files []string

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

	bytes int64
}

func newSource(root string) (*source, error) {
	base, files, err := windowFiles(root)
	if err != nil {
		return nil, err
	}
	return &source{root: base, files: files, idx: -1}, nil
}

// next returns the next record in arrival order with its key. ok is false at
// the end of the window.
func (s *source) next() (rec Record, key orderKey, ok bool, err error) {
	for {
		if s.rd == nil {
			if err := s.openNext(); err != nil {
				return Record{}, orderKey{}, false, err
			}
			if s.rd == nil {
				return Record{}, orderKey{}, false, nil
			}
		}

		t, payload, err := s.rd.Next()
		if err == io.EOF {
			if cerr := s.rd.Close(); cerr != nil {
				return Record{}, orderKey{}, false, cerr
			}
			s.rd = nil
			continue
		}
		if err != nil {
			return Record{}, orderKey{}, false, fmt.Errorf("%s: record %d: %w", s.files[s.idx], s.rec, err)
		}

		ordinal := s.rec
		s.rec++
		s.bytes += int64(len(payload)) + recordOverhead

		rec, err := s.decode(t, payload, ordinal)
		if err != nil {
			return Record{}, orderKey{}, false, err
		}
		return rec, s.keyFor(rec), true, nil
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

func (s *source) openNext() error {
	s.idx++
	if s.idx >= len(s.files) {
		return nil
	}
	rd, err := tapefile.Open(filepath.Join(s.root, filepath.FromSlash(s.files[s.idx])))
	if err != nil {
		return err
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
