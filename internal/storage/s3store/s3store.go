// Package s3store implements storage.Store on S3.
//
// It is a thin wrapper and stays one. The three operations map to PutObject,
// ListObjectsV2 and GetObject, and the only judgement in the package is what
// to make of a failed conditional put.
//
// # Append-only, enforced by the bucket
//
// Every Put carries If-None-Match: "*", so S3 itself refuses to overwrite an
// object that already exists. The upload path therefore cannot violate
// invariant 3 even if it tries: a retried upload whose first attempt actually
// landed comes back 412, which the uploader reads as "already stored" and
// treats as success. Without the condition, the same retry would silently
// replace a complete object with a second copy of itself — harmless the day it
// happens, and indistinguishable from the day it replaces a complete object
// with a truncated one.
//
// A 409 is a different animal and is deliberately not treated as success: it
// means two conditional writes to one key raced and S3 could not say which won,
// so the answer is to try again, not to assume the object is there.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awshttp "github.com/aws/smithy-go/transport/http"

	"github.com/AnanmayS/tape/internal/storage"
)

// Store is an S3 bucket, optionally rooted at a key prefix.
//
// Keys handed to it are relative to that prefix, exactly as a filesystem
// store's keys are relative to its directory, so the two are interchangeable
// and a caller cannot tell which it is holding.
type Store struct {
	client *s3.Client
	bucket string
	prefix string
}

var _ storage.Store = (*Store)(nil)

// New builds a Store from the ambient AWS configuration: environment,
// profile, or the task role an ECS Fargate task runs under. Nothing in this
// project ever takes a credential as an argument.
func New(ctx context.Context, bucket, prefix string) (*Store, error) {
	if bucket == "" {
		return nil, errors.New("s3store: bucket must not be empty")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3store: loading AWS configuration: %w", err)
	}
	return NewWithClient(s3.NewFromConfig(cfg), bucket, prefix), nil
}

// NewWithClient wraps an already-built client. It is how the tests point the
// same code at an in-process fake.
func NewWithClient(c *s3.Client, bucket, prefix string) *Store {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Store{client: c, bucket: bucket, prefix: prefix}
}

func (s *Store) String() string { return "s3://" + s.bucket + "/" + s.prefix }

// Bucket returns the bucket the store writes to.
func (s *Store) Bucket() string { return s.bucket }

// full turns a store-relative key into the object's real key.
func (s *Store) full(key string) string { return s.prefix + key }

// Put stores r at key, and never overwrites: an occupied key is ErrExists.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.full(key)),
		Body:   r,

		// The whole append-only guarantee, in one header. S3 evaluates it
		// atomically, so this holds against a concurrent writer and not merely
		// against a careless one.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return storage.ErrExists
		}
		return fmt.Errorf("s3store: put %s: %w", s.full(key), err)
	}
	return nil
}

// List returns every key under prefix, relative to the store's own prefix,
// sorted byte-wise.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.full(prefix)),
	})
	var keys []string
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: list %s: %w", s.full(prefix), err)
		}
		for _, o := range page.Contents {
			if o.Key == nil {
				continue
			}
			keys = append(keys, strings.TrimPrefix(*o.Key, s.prefix))
		}
	}
	// S3 lists in UTF-8 binary order already. Sorting anyway makes the order
	// this package's promise rather than a detail of a service's.
	sort.Strings(keys)
	return keys, nil
}

// Open streams the object at key. The body is read as it is consumed; nothing
// downloads a whole object into memory on the way.
func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.full(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("s3store: open %s: %w", s.full(key), fs.ErrNotExist)
		}
		return nil, fmt.Errorf("s3store: open %s: %w", s.full(key), err)
	}
	return out.Body, nil
}

// isPreconditionFailed reports whether err is S3 refusing a conditional put
// because the key is taken.
//
// Both the error code and the status are checked. The code is what the SDK
// models, the status is what the protocol guarantees, and an S3-compatible
// endpoint that gets one of the two right is more likely than one that gets
// neither.
func isPreconditionFailed(err error) bool {
	if code := errorCode(err); code == "PreconditionFailed" {
		return true
	}
	return statusCode(err) == http.StatusPreconditionFailed
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	if code := errorCode(err); code == "NoSuchKey" || code == "NotFound" {
		return true
	}
	return statusCode(err) == http.StatusNotFound
}

func errorCode(err error) string {
	var api interface{ ErrorCode() string }
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}

func statusCode(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}
