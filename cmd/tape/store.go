package main

import (
	"context"
	"flag"

	"github.com/AnanmayS/tape/internal/storage"
	"github.com/AnanmayS/tape/internal/storage/s3store"
)

// storeFlags are the two flags every subcommand that touches an object store
// takes. Two, not five: the region, the endpoint and the credentials all come
// from the ambient AWS configuration, which is what an ECS task role supplies
// and what a developer's profile supplies. A flag for any of them would be a
// second way to say something the environment already says.
type storeFlags struct {
	bucket string
	prefix string
}

func (s *storeFlags) register(fs *flag.FlagSet, verb string) {
	fs.StringVar(&s.bucket, "s3-bucket", "", "S3 bucket to "+verb+"; empty stays entirely local")
	fs.StringVar(&s.prefix, "s3-prefix", "", "key prefix within the bucket")
}

// set reports whether a bucket was named.
func (s *storeFlags) set() bool { return s.bucket != "" }

// store builds the store, or returns nil if no bucket was named. Credentials
// come from the environment, a profile, or the task role; this command never
// takes one as an argument.
func (s *storeFlags) store(ctx context.Context) (storage.Store, error) {
	if !s.set() {
		return nil, nil
	}
	return s3store.New(ctx, s.bucket, s.prefix)
}
