package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Local is a Store backed by a directory tree. Keys become paths under root.
//
// It is not a stand-in for the real thing that tests tolerate and production
// avoids. It is the path every test in this repository runs on, which is what
// keeps the test suite free of AWS credentials, and it is a working store in
// its own right: a capture with no bucket configured keeps its windows here.
type Local struct {
	root string
}

// NewLocal returns a Store rooted at root. The directory is created on demand.
func NewLocal(root string) *Local { return &Local{root: root} }

func (l *Local) String() string { return "file://" + l.root }

// Root returns the directory the store is rooted at.
func (l *Local) Root() string { return l.root }

// path maps a store key to a filesystem path.
func (l *Local) path(key string) string {
	return filepath.Join(l.root, filepath.FromSlash(key))
}

// Put writes r to key, and fails with ErrExists if key is taken.
//
// The write goes to a temporary file which is then hard-linked into place.
// Link is the filesystem's own atomic exclusive-create: it fails rather than
// replacing an existing name, so two racing Puts of the same key cannot both
// win and a Put interrupted halfway leaves no half-written object behind. That
// is the same guarantee S3 gives for a conditional put, which is the point —
// the two stores have to behave alike or the local one is not a test of
// anything.
func (l *Local) Put(ctx context.Context, key string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dst := l.path(key)
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	// The temporary lands beside the destination so the link stays within one
	// filesystem, and carries no Ext so a crash cannot leave something a
	// window listing would mistake for a tape file.
	tmp, err := os.CreateTemp(dir, ".tmp-put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Link(tmpName, dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrExists
		}
		return err
	}
	return nil
}

// List returns every key under prefix, sorted byte-wise.
func (l *Local) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// A prefix is a string prefix, not a directory: "date=2026-08-2" is a legal
	// scan. Walking from the last complete directory component and filtering
	// the rest keeps that exactly true while not walking the whole store.
	base := l.root
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		base = l.path(prefix[:i+1])
	}

	var keys []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A prefix naming nothing is an empty result, not a failure: that
			// is what listing an empty S3 prefix does, and the two stores have
			// to answer the same question the same way.
			if errors.Is(err, fs.ErrNotExist) && p == base {
				return filepath.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		if k := filepath.ToSlash(rel); strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// WalkDir already walks lexically, but sorting makes the order this
	// package's promise rather than a detail of another one.
	sort.Strings(keys)
	return keys, nil
}

// Open streams the object at key.
func (l *Local) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(l.path(key))
}
