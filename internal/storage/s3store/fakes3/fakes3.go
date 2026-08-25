// Package fakes3 is an in-process S3, good enough for the four requests this
// project makes of the real one.
//
// It exists so that the S3 code path is covered by tests that need no AWS
// account, no credentials and no container. That is not a convenience: a test
// suite that needs a bucket is a test suite that stops being run, and the
// conditional put is exactly the behaviour nobody would notice had broken.
//
// It implements PutObject with If-None-Match, ListObjectsV2 with pagination,
// GetObject, and nothing else. It is not an S3 emulator and must never grow
// into one — the moment a test needs a fifth operation is the moment to ask
// whether the project needs it either.
package fakes3

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Server is a running fake. Close it when the test is done.
type Server struct {
	bucket string
	srv    *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	gets    int
	lists   int
	failPut int
}

// New starts a fake holding one bucket.
func New(bucket string) *Server {
	f := &Server{bucket: bucket, objects: make(map[string][]byte)}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// URL is the endpoint to point an S3 client at.
func (f *Server) URL() string { return f.srv.URL }

// Close shuts the fake down.
func (f *Server) Close() { f.srv.Close() }

// Client returns an S3 client wired to this fake.
//
// Retries are turned off. The SDK's own retryer would quietly paper over
// injected failures, and the layer being tested here — the uploader's retry
// loop, and what a retried conditional put does — is the one that has to be
// seen doing the retrying.
func (f *Server) Client(opts ...func(*s3.Options)) *s3.Client {
	o := s3.Options{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("fake", "fake", ""),
		BaseEndpoint:     aws.String(f.srv.URL),
		UsePathStyle:     true,
		HTTPClient:       f.srv.Client(),
		RetryMaxAttempts: 1,
	}
	return s3.New(o, opts...)
}

// FailNextPuts makes the next n PutObject requests fail with a 500 after
// reading the body, without storing anything.
func (f *Server) FailNextPuts(n int) {
	f.mu.Lock()
	f.failPut = n
	f.mu.Unlock()
}

// Keys returns every stored key, sorted.
func (f *Server) Keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Object returns a stored object's bytes.
func (f *Server) Object(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	return append([]byte(nil), b...), ok
}

// Put stores an object directly, without going through HTTP. It is how a test
// stages a window into the fake.
func (f *Server) Put(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
}

// Requests reports how many of each operation the fake has served. Puts counts
// requests, including the ones it refused — which is what a test asserting
// "the retry did not overwrite" needs to see.
func (f *Server) Requests() (puts, gets, lists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts, f.gets, f.lists
}

func (f *Server) serve(w http.ResponseWriter, r *http.Request) {
	bucket, key, ok := splitPath(r.URL.Path)
	if !ok || bucket != f.bucket {
		writeError(w, http.StatusNotFound, "NoSuchBucket", "the specified bucket does not exist")
		return
	}

	switch {
	case r.Method == http.MethodPut && key != "":
		f.put(w, r, key)
	case r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2":
		f.list(w, r)
	case r.Method == http.MethodGet && key != "":
		f.get(w, key)
	default:
		writeError(w, http.StatusNotImplemented, "NotImplemented",
			fmt.Sprintf("fakes3 serves PutObject, ListObjectsV2 and GetObject; got %s %s",
				r.Method, r.URL.RequestURI()))
	}
}

func (f *Server) put(w http.ResponseWriter, r *http.Request, key string) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	f.mu.Lock()
	f.puts++
	if f.failPut > 0 {
		f.failPut--
		f.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalError", "injected failure")
		return
	}
	_, exists := f.objects[key]
	cond := r.Header.Get("If-None-Match")
	if cond == "*" && exists {
		f.mu.Unlock()
		// This is the response the whole append-only story rests on: a key
		// that is taken is not written again, and the writer is told so.
		writeError(w, http.StatusPreconditionFailed, "PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold")
		return
	}
	f.objects[key] = body
	f.mu.Unlock()

	w.Header().Set("ETag", etag(body))
	w.WriteHeader(http.StatusOK)
}

func (f *Server) get(w http.ResponseWriter, key string) {
	f.mu.Lock()
	f.gets++
	body, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("ETag", etag(body))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// listResult is the ListObjectsV2 response shape.
type listResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	KeyCount              int      `xml:"KeyCount"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []object `xml:"Contents"`
}

type object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (f *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")

	// A deliberately small page size, so that a listing of more than a couple
	// of objects goes round the pagination loop. A window is many files and
	// paging is not a case worth discovering in production.
	maxKeys := 2
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxKeys = n
		}
	}

	f.mu.Lock()
	f.lists++
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sizes := make(map[string]int64, len(keys))
	tags := make(map[string]string, len(keys))
	for _, k := range keys {
		sizes[k] = int64(len(f.objects[k]))
		tags[k] = etag(f.objects[k])
	}
	f.mu.Unlock()
	sort.Strings(keys)

	// The continuation token is the key to resume after, which is all a
	// lexicographically ordered listing needs.
	if after := q.Get("continuation-token"); after != "" {
		i := sort.SearchStrings(keys, after)
		for i < len(keys) && keys[i] <= after {
			i++
		}
		keys = keys[i:]
	}

	res := listResult{
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:    f.bucket,
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}
	if len(keys) > maxKeys {
		res.IsTruncated = true
		res.NextContinuationToken = keys[maxKeys-1]
		keys = keys[:maxKeys]
	}
	for _, k := range keys {
		res.Contents = append(res.Contents, object{
			Key:          k,
			LastModified: time.Unix(0, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         tags[k],
			Size:         sizes[k],
			StorageClass: "STANDARD",
		})
	}
	res.KeyCount = len(res.Contents)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, xml.Header)
	xml.NewEncoder(w).Encode(res)
}

// readBody reads a request body, undoing aws-chunked framing if the SDK used
// it. Nothing in this project depends on which encoding the SDK picks, and a
// fake that only understood one of them would fail for a reason that had
// nothing to do with the code under test.
func readBody(r *http.Request) ([]byte, error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") &&
		!strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return raw, nil
	}
	return dechunk(raw)
}

// dechunk decodes aws-chunked framing: repeated "{hex length}[;ext]\r\n{data}
// \r\n", ending with a zero-length chunk followed by optional trailers.
func dechunk(raw []byte) ([]byte, error) {
	br := bufio.NewReader(bytes.NewReader(raw))
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF && strings.TrimSpace(line) == "" {
				return out.Bytes(), nil
			}
			return nil, fmt.Errorf("fakes3: aws-chunked body: %w", err)
		}
		head := strings.TrimSpace(line)
		if head == "" {
			continue
		}
		if i := strings.IndexByte(head, ';'); i >= 0 {
			head = head[:i]
		}
		n, err := strconv.ParseInt(head, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("fakes3: aws-chunked size %q: %w", head, err)
		}
		if n == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, br, n); err != nil {
			return nil, fmt.Errorf("fakes3: aws-chunked payload: %w", err)
		}
	}
}

// splitPath splits a path-style request path into bucket and key.
func splitPath(p string) (bucket, key string, ok bool) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", "", false
	}
	bucket, key, _ = strings.Cut(p, "/")
	return bucket, key, true
}

func etag(body []byte) string {
	sum := md5.Sum(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

type errorResult struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	xml.NewEncoder(w).Encode(errorResult{Code: code, Message: msg})
}
