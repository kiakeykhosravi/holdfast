package s3store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"holdfast/interfaces"
	"holdfast/interfaces/storetest"
)

// The S3 tests run against an in-process fake (gofakes3) by default, so they
// need no infrastructure. To run them against a real S3-compatible server,
// which is what actually proves the multipart path, point them at it:
//
//	docker run --rm -p 7070:7070 -e ROOT_ACCESS_KEY=k -e ROOT_SECRET_KEY=s \
//	  versity/versitygw posix /tmp
//	HOLDFAST_S3_TEST_ENDPOINT=http://127.0.0.1:7070 \
//	HOLDFAST_S3_TEST_BUCKET=holdfast-test \
//	HOLDFAST_S3_TEST_ACCESS_KEY=k HOLDFAST_S3_TEST_SECRET_KEY=s \
//	go test ./interfaces/s3store/
//
// The bucket is created if missing.
func testOptions(t *testing.T) Options {
	t.Helper()
	opts := Options{
		Region:       "us-east-1",
		UsePathStyle: true,
		// The smallest part S3 accepts, so storetest.LargeObjectSize spans
		// two parts and the multipart path is the one under test.
		PartSize:    5 << 20,
		Concurrency: 2,
	}
	if endpoint := os.Getenv("HOLDFAST_S3_TEST_ENDPOINT"); endpoint != "" {
		opts.Endpoint = endpoint
		opts.Bucket = os.Getenv("HOLDFAST_S3_TEST_BUCKET")
		opts.AccessKeyID = os.Getenv("HOLDFAST_S3_TEST_ACCESS_KEY")
		opts.SecretAccessKey = os.Getenv("HOLDFAST_S3_TEST_SECRET_KEY")
		if r := os.Getenv("HOLDFAST_S3_TEST_REGION"); r != "" {
			opts.Region = r
		}
		// Some S3-compatible servers reject the SDK's default trailing
		// checksums; HOLDFAST_S3_TEST_LEGACY=1 turns them off.
		opts.LegacyCompat = os.Getenv("HOLDFAST_S3_TEST_LEGACY") != ""
		return opts
	}
	opts.Endpoint = storetest.FakeS3Endpoint(t)
	opts.Bucket = "holdfast-test"
	opts.AccessKeyID = "fake"
	opts.SecretAccessKey = "fake-secret"
	return opts
}

var testSeq int

// newTestStore returns a store on a fresh prefix and deletes everything under
// it when the test ends.
func newTestStore(t *testing.T, mutate func(*Options)) *Store {
	t.Helper()
	opts := testOptions(t)
	testSeq++
	opts.Prefix = fmt.Sprintf("storetest/%d-%d", time.Now().UnixNano(), testSeq)
	if mutate != nil {
		mutate(&opts)
	}
	ctx := context.Background()
	s, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	ensureBucket(t, s.client, opts.Bucket)
	t.Cleanup(func() {
		objs, err := s.List(ctx, "")
		if err != nil {
			t.Errorf("cleanup: list: %v", err)
		}
		for _, o := range objs {
			if err := s.Delete(ctx, o.Name); err != nil {
				t.Errorf("cleanup: delete %s: %v", o.Name, err)
			}
		}
		// A negative age puts the cutoff in the future, so every upload
		// under this prefix is aborted regardless of clock skew between this
		// machine and the server. A leftover here is a finding, not noise:
		// against one provider a retried CreateMultipartUpload orphaned an
		// upload for an object that had completed successfully.
		s.staleUploadAge = -24 * time.Hour
		if err := s.Sweep(ctx); err != nil {
			t.Errorf("cleanup: sweep: %v", err)
		}
		if left := listUploads(t, s); len(left) > 0 {
			t.Errorf("cleanup: %d multipart upload(s) still open under %s after sweep: %v", len(left), s.prefix, left)
		}
	})
	return s
}

// listUploads returns the keys of multipart uploads open under the store's
// prefix.
func listUploads(t *testing.T, s *Store) []string {
	t.Helper()
	out, err := s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix),
	})
	if err != nil {
		t.Errorf("list multipart uploads: %v", err)
		return nil
	}
	var keys []string
	for _, u := range out.Uploads {
		keys = append(keys, aws.ToString(u.Key))
	}
	return keys
}

func ensureBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return
	}
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	var owned *s3types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
}

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) interfaces.Storer {
		return newTestStore(t, nil)
	})
}

func TestLocationAndPrefixNormalisation(t *testing.T) {
	s := NewWithClient(nil, Options{Bucket: "b", Prefix: "/pg/prod/"})
	if s.Location() != "s3://b/pg/prod/" {
		t.Fatalf("Location = %q", s.Location())
	}
	k, _ := s.key("x")
	if k != "pg/prod/x" {
		t.Fatalf("key = %q", k)
	}
	if NewWithClient(nil, Options{Bucket: "b"}).Location() != "s3://b/" {
		t.Fatal("empty prefix location")
	}
}

// A run killed after the first part has been uploaded leaves an incomplete
// multipart upload: invisible to List, billed forever. Sweep must abort it,
// and the orphaned writer must then be unable to complete.
func TestSweepAbortsStaleMultipartUploads(t *testing.T) {
	s := newTestStore(t, func(o *Options) { o.StaleUploadAge = time.Nanosecond })
	ctx := context.Background()

	w, err := s.Create(ctx, "backup_crash.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	// One full part plus one byte: the uploader has to commit to multipart.
	if _, err := w.Write(make([]byte, 5<<20+1)); err != nil {
		t.Fatal(err)
	}
	waitForUpload := func() bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			out, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
				Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix),
			})
			if err == nil && len(out.Uploads) > 0 {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}
	if !waitForUpload() {
		t.Fatal("no multipart upload appeared")
	}

	if err := s.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	out, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Uploads) != 0 {
		t.Fatalf("%d upload(s) survived sweep", len(out.Uploads))
	}
	if err := w.Commit(); err == nil {
		t.Fatal("commit after sweep succeeded")
	}
	if _, err := s.Stat(ctx, "backup_crash.zip.enc"); err == nil {
		t.Fatal("partial object became visible")
	}
}

// A fresh upload must survive a sweep: the age threshold is what protects a
// run on another host that is legitimately mid-upload.
func TestSweepKeepsRecentUploads(t *testing.T) {
	s := newTestStore(t, func(o *Options) { o.StaleUploadAge = time.Hour })
	ctx := context.Background()
	w, err := s.Create(ctx, "backup_live.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	data := storetest.LargeObjectSize
	if _, err := w.Write(make([]byte, data)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit after a sweep that should have left it alone: %v", err)
	}
	info, err := s.Stat(ctx, "backup_live.zip.enc")
	if err != nil || info.Size != int64(data) {
		t.Fatalf("stat = %+v, %v", info, err)
	}
}

// The bucket not existing is the likeliest misconfiguration. It must surface
// as an error from Write or Commit, never as a hang: the encryptor writing
// into this would otherwise block forever holding the dump open.
func TestMissingBucketDoesNotHang(t *testing.T) {
	opts := testOptions(t)
	opts.Bucket = "holdfast-does-not-exist-" + fmt.Sprint(time.Now().UnixNano())
	ctx := context.Background()
	s, err := New(ctx, opts) // deliberately not through newTestStore, which creates the bucket
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.Create(ctx, "backup_nb.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			if _, err := w.Write(make([]byte, 1<<20)); err != nil {
				done <- err
				return
			}
		}
		done <- w.Commit()
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("upload to a missing bucket succeeded")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("writer hung on a missing bucket")
	}
}

// An S3-compatible endpoint with no region must work: compatible servers
// take any region, and asking operators for a meaningless value was the
// first thing a storage-only sync config tripped over.
func TestEndpointWithoutRegionWorks(t *testing.T) {
	opts := testOptions(t)
	opts.Region = ""
	opts.Prefix = fmt.Sprintf("noregion/%d", time.Now().UnixNano())
	s, err := New(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	ensureBucket(t, s.client, opts.Bucket)
	t.Cleanup(func() { _ = s.Delete(context.Background(), "backup_r.zip.enc") })
	if err := interfaces.PutObject(context.Background(), s, "backup_r.zip.enc", []byte("x")); err != nil {
		t.Fatalf("put with defaulted region: %v", err)
	}
}

// No credentials anywhere must be an immediate, explicit error, not a
// metadata-service timeout inside the first request. This is what a sync
// host hits when sudo drops the environment.
func TestNoCredentialsFailsFastAtConstruction(t *testing.T) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_WEB_IDENTITY_TOKEN_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	start := time.Now()
	_, err := New(context.Background(), Options{Bucket: "b", Endpoint: "http://127.0.0.1:1"})
	if err == nil {
		t.Fatal("New succeeded with no credentials")
	}
	if !strings.Contains(err.Error(), "no usable S3 credentials") {
		t.Fatalf("error does not say what is missing: %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %s to notice missing credentials; the metadata lookup was not disabled", d)
	}
}
