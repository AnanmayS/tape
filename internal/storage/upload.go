package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

// Uploader copies closed tape files into a Store, off the capture path.
//
// Two rules shape it, and both come from the same place: capture is the only
// part of this system that cannot be re-run. A frame missed while a goroutine
// waited on S3 is gone for good, and no object in a bucket is worth one.
//
//  1. Adding a file never blocks and never fails. Uploads happen on a worker
//     goroutine; the capture goroutine hands over a path and returns.
//  2. An upload that cannot be made to work is loud, not fatal. The local file
//     is still on disk and still complete — it is the same bytes the object
//     would have been — so a failed upload costs a later re-run, not data.
//
// A retry that finds its object already stored is a success. Put is
// conditional, so the second attempt of an upload whose first attempt landed
// but whose response was lost comes back ErrExists, and that is the append-only
// invariant working rather than an error to report.
type Uploader struct {
	st  Store
	cfg UploadConfig

	jobs chan job
	done chan struct{}

	// ctx is cancelled only when a drain runs out of patience. It is not the
	// capture context: shutdown is exactly when the last file gets uploaded, so
	// an uploader wired to a cancelled context would abandon the one upload it
	// most needs to finish.
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	stats UploadStats
}

type job struct {
	path string
	key  string
}

// UploadConfig configures an Uploader. The zero value is usable; every field
// falls back to a documented default.
type UploadConfig struct {
	Log *slog.Logger

	// Queue is how many closed files may wait for the worker. Files close once
	// per rotation window — minutes apart — so a queue this deep only fills if
	// the store has been unreachable for hours.
	Queue int

	// Attempts is how many times one file is tried before it is given up on
	// and logged. With the default backoff, 6 attempts spans about a minute.
	Attempts int

	// Base and Max bound the exponential backoff between attempts.
	Base, Max time.Duration

	// Timeout bounds a single upload attempt.
	Timeout time.Duration

	// Drain is how long Close waits for queued uploads to finish.
	Drain time.Duration
}

func (c *UploadConfig) withDefaults() {
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Queue <= 0 {
		c.Queue = 64
	}
	if c.Attempts <= 0 {
		c.Attempts = 6
	}
	if c.Base <= 0 {
		c.Base = 500 * time.Millisecond
	}
	if c.Max <= 0 {
		c.Max = 30 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 2 * time.Minute
	}
	if c.Drain <= 0 {
		c.Drain = 30 * time.Second
	}
}

// UploadStats counts what an Uploader did. Every field is counted, not
// estimated.
type UploadStats struct {
	// Added is how many files were handed to the uploader.
	Added int64

	// Uploaded is how many objects this process stored.
	Uploaded int64

	// Existed is how many uploads found the object already present. A
	// conditional put makes that the safe outcome of a retry, not a collision.
	Existed int64

	// Failed is how many files ran out of attempts. Each one is still on local
	// disk, and each one was logged at error level.
	Failed int64

	// Dropped is how many files were never attempted because the queue was
	// full. Also still on local disk, also logged.
	Dropped int64

	// Retries counts attempts after the first, across all files.
	Retries int64

	// Bytes is how many bytes of object body this process stored.
	Bytes int64
}

// Pending reports files added but not yet resolved one way or the other.
func (s UploadStats) Pending() int64 {
	return s.Added - (s.Uploaded + s.Existed + s.Failed + s.Dropped)
}

// LogAttrs renders the stats for a structured logger.
func (s UploadStats) LogAttrs() []any {
	return []any{
		"added", s.Added,
		"uploaded", s.Uploaded,
		"already_present", s.Existed,
		"failed", s.Failed,
		"dropped", s.Dropped,
		"retries", s.Retries,
		"bytes", s.Bytes,
	}
}

// NewUploader starts an uploader writing to st. Close it when capture ends.
func NewUploader(st Store, cfg UploadConfig) *Uploader {
	cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	u := &Uploader{
		st:     st,
		cfg:    cfg,
		jobs:   make(chan job, cfg.Queue),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	go u.run()
	return u
}

// Add queues a closed file for upload under key. It never blocks: a full queue
// means the store has been unreachable long enough to back up, and the answer
// to that is a loud log line, not a stalled capture.
func (u *Uploader) Add(path, key string) {
	u.bump(func(s *UploadStats) { s.Added++ })
	select {
	case u.jobs <- job{path: path, key: key}:
	default:
		u.bump(func(s *UploadStats) { s.Dropped++ })
		u.cfg.Log.Error("upload queue full, file not uploaded",
			"path", path, "key", key, "store", u.st.String(), "queue", u.cfg.Queue,
			"remedy", "the local file is complete; upload it once the store is reachable")
	}
}

// Close stops accepting files and waits for the queued ones, up to Drain. It
// reports an error if the drain did not finish, naming how many files are still
// only on local disk.
func (u *Uploader) Close() error {
	close(u.jobs)
	select {
	case <-u.done:
		u.cancel()
		return nil
	case <-time.After(u.cfg.Drain):
	}

	// Out of patience. Cut the in-flight attempt short and say what is left
	// behind rather than hanging a shutdown on an unreachable bucket.
	u.cancel()
	<-u.done
	n := u.Stats().Pending()
	u.cfg.Log.Error("upload drain timed out",
		"pending", n, "drain", u.cfg.Drain.String(), "store", u.st.String())
	return fmt.Errorf("storage: %d file(s) not uploaded within %s; they remain on local disk",
		n, u.cfg.Drain)
}

// Stats returns a snapshot of the counters.
func (u *Uploader) Stats() UploadStats {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.stats
}

func (u *Uploader) bump(fn func(*UploadStats)) {
	u.mu.Lock()
	fn(&u.stats)
	u.mu.Unlock()
}

func (u *Uploader) run() {
	defer close(u.done)
	for j := range u.jobs {
		u.upload(j)
	}
}

// upload stores one file, retrying with backoff. It returns only when the file
// is stored, is already stored, or has run out of attempts.
func (u *Uploader) upload(j job) {
	log := u.cfg.Log.With("path", j.path, "key", j.key, "store", u.st.String())
	for attempt := 1; ; attempt++ {
		size, err := u.attempt(j)
		switch {
		case err == nil:
			u.bump(func(s *UploadStats) { s.Uploaded++; s.Bytes += size })
			log.Info("uploaded", "bytes", size, "attempts", attempt)
			return

		case errors.Is(err, ErrExists):
			// The key is taken, which under a conditional put means the object
			// is already there — most likely stored by an earlier attempt of
			// this same upload whose answer got lost. Nothing was overwritten,
			// and nothing needs to be.
			u.bump(func(s *UploadStats) { s.Existed++ })
			log.Info("object already stored, upload not repeated", "attempts", attempt)
			return

		case attempt >= u.cfg.Attempts:
			u.bump(func(s *UploadStats) { s.Failed++ })
			log.Error("upload failed, giving up",
				"err", err, "attempts", attempt,
				"remedy", "the local file is complete; capture is unaffected")
			return
		}

		d := backoffDelay(u.cfg.Base, u.cfg.Max, attempt-1)
		u.bump(func(s *UploadStats) { s.Retries++ })
		log.Warn("upload failed, retrying", "err", err, "attempt", attempt, "retry_in", d.String())
		if !u.wait(d) {
			u.bump(func(s *UploadStats) { s.Failed++ })
			log.Error("upload abandoned during shutdown",
				"err", err, "attempts", attempt,
				"remedy", "the local file is complete; capture is unaffected")
			return
		}
	}
}

// attempt performs one upload and reports the bytes stored.
func (u *Uploader) attempt(j job) (int64, error) {
	f, err := os.Open(j.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(u.ctx, u.cfg.Timeout)
	defer cancel()

	// A retried attempt re-reads from the start rather than from wherever the
	// failed one stopped.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if err := u.st.Put(ctx, j.key, f); err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// wait sleeps between attempts, reporting false if a shutdown cut it short.
func (u *Uploader) wait(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-u.ctx.Done():
		return false
	}
}

// backoffDelay grows exponentially and carries jitter, drawn from the upper
// half of the window so that spreading retries out never collapses the delay to
// nothing.
func backoffDelay(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(rand.Float64()*float64(half))
}
