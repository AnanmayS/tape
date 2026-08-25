package replay

import (
	"bytes"
	"context"
	"testing"

	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/storage/s3store"
	"github.com/AnanmayS/tape/internal/storage/s3store/fakes3"
)

// TestDeterminismThroughS3 is the invariant carried across the storage move:
// the golden fixture replayed out of a bucket is byte-identical to the same
// fixture replayed off local disk, and both hash to the digest M3 recorded.
//
// The bucket is an in-process fake — see internal/storage/s3store/fakes3 — so
// this runs in CI with no AWS account and no credentials. What it exercises is
// the real client, the real request signing, the real ListObjectsV2 pagination
// and the real GetObject streaming; only the far end of the socket is
// substituted.
//
// The objects are stored under the keys the fixture files already have, so the
// two windows are named identically and every byte of the output must match,
// file names included. That the digest is also goldenDigest is what says S3
// changed nothing about replay rather than merely changing it consistently.
func TestDeterminismThroughS3(t *testing.T) {
	root := fixtureWindow(t)
	local := storage.NewLocal(root)

	fake := fakes3.New("tape-determinism")
	defer fake.Close()
	remote := s3store.NewWithClient(fake.Client(), "tape-determinism", "captures")

	ctx := context.Background()
	keys, err := local.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) < 3 {
		t.Fatalf("fixture has %d files, want at least 3", len(keys))
	}
	for _, k := range keys {
		copyObject(t, local, k, remote, k)
	}

	fromDisk, diskDigest := replayCanonicalStore(t, local, "")
	fromS3, s3Digest := replayCanonicalStore(t, remote, "")

	if s3Digest != diskDigest {
		t.Fatalf("a window replayed from S3 differs from the same window on disk:\n disk %s\n s3   %s",
			diskDigest, s3Digest)
	}
	if !bytes.Equal(fromDisk, fromS3) {
		t.Fatalf("digests match but bytes differ: %d vs %d bytes", len(fromDisk), len(fromS3))
	}
	if s3Digest != goldenDigest {
		t.Errorf("window digest through S3 is %s, want the golden %s", s3Digest, goldenDigest)
	}

	// The listing has to have gone round the paginator and the objects have to
	// have been streamed one by one, or this proved less than it looks.
	_, gets, lists := fake.Requests()
	if lists < 2 {
		t.Errorf("the window was listed in %d requests; the fixture is more than one page", lists)
	}
	if gets != len(keys) {
		t.Errorf("replay issued %d GetObject requests for %d objects", gets, len(keys))
	}
	t.Logf("replayed %d bytes from s3://tape-determinism/captures in %d list and %d get requests, sha256 %s",
		len(fromS3), lists, gets, s3Digest)
}

// TestS3ReplayIsRepeatable checks the same window twice out of the same bucket.
// Two replays of one window are the project's whole reason to exist, and the
// bucket is now part of the path they run through.
func TestS3ReplayIsRepeatable(t *testing.T) {
	root := fixtureWindow(t)
	local := storage.NewLocal(root)

	fake := fakes3.New("tape-repeat")
	defer fake.Close()
	remote := s3store.NewWithClient(fake.Client(), "tape-repeat", "")

	for _, k := range mustList(t, local, "") {
		copyObject(t, local, k, remote, k)
	}

	first, firstDigest := replayCanonicalStore(t, remote, "")
	second, secondDigest := replayCanonicalStore(t, remote, "")
	if firstDigest != secondDigest {
		t.Fatalf("two replays out of one bucket differ:\n first  %s\n second %s", firstDigest, secondDigest)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("digests match but bytes differ: %d vs %d bytes", len(first), len(second))
	}
	if firstDigest != goldenDigest {
		t.Errorf("bucket replay digest is %s, want %s", firstDigest, goldenDigest)
	}
}
