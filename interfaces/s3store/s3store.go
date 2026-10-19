// Package s3store is the Storer backed by an S3 bucket, or anything that
// speaks the S3 API (MinIO, Ceph RGW, Cloudflare R2, Backblaze B2).
//
// Objects are flat keys under one prefix: <prefix>/backup_<id>.zip.enc and so
// on. Nothing about chains or retention lives here; the sidecars still carry
// all of it, so a bucket is reconstructible the same way a directory is.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"holdfast/interfaces"
	"holdfast/types"
)

const (
	// defaultPartSize bounds the largest object this store can write: S3
	// allows 10,000 parts, so 64 MiB parts give 640 GiB per archive. The SDK
	// default of 5 MiB would cap a physical backup at 50 GiB, which a
	// moderately sized cluster exceeds.
	defaultPartSize = 64 << 20

	// defaultConcurrency is how many parts are in flight at once; memory use
	// is roughly PartSize * Concurrency.
	defaultConcurrency = 4

	// defaultStaleUploadAge is how old an incomplete multipart upload must be
	// before Sweep aborts it. Non-zero because the run lock only serialises
	// this host, and another host may legitimately be mid-upload into the
	// same bucket. A bucket lifecycle rule (AbortIncompleteMultipartUpload)
	// is the belt to this braces.
	defaultStaleUploadAge = 24 * time.Hour
)

// DefaultCompatRegion is the region used for S3-compatible endpoints when
// none is configured.
const DefaultCompatRegion = "us-east-1"

// credentialTimeout bounds the startup credential lookup.
const credentialTimeout = 10 * time.Second

// Options configures a Store. Zero values mean "use the SDK default chain":
// with no credentials here the SDK looks at the environment, shared config,
// IRSA, and the instance metadata service, which is what production should
// use. Static credentials exist for MinIO in tests and air-gapped setups.
type Options struct {
	Bucket string
	// Prefix is the key prefix objects are stored under, e.g. "pg/prod". It
	// may be empty. Leading and trailing slashes are normalised.
	Prefix string
	Region string

	// Endpoint overrides the service URL for S3-compatible stores, e.g.
	// "http://minio:9000". Most of them also need UsePathStyle.
	Endpoint     string
	UsePathStyle bool

	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// LegacyCompat turns off the SDK's default request checksums and response
	// validation. Needed for S3-compatible servers that predate the 2025
	// integrity protections and reject the trailing checksum headers.
	LegacyCompat bool

	PartSize       int64
	Concurrency    int
	StaleUploadAge time.Duration
}

// Store is an S3 bucket behind the Storer interface.
type Store struct {
	client         *s3.Client
	uploader       *manager.Uploader
	bucket         string
	prefix         string // "" or "a/b/" (always slash-terminated when set)
	staleUploadAge time.Duration
}

var _ interfaces.Storer = (*Store)(nil)

// New builds a client from opts and returns a store. It does not touch the
// bucket: a backup host that cannot reach S3 at startup should fail at the
// first operation with that operation's name in the error, not here.
func New(ctx context.Context, opts Options) (*Store, error) {
	if strings.TrimSpace(opts.Bucket) == "" {
		return nil, fmt.Errorf("s3store: bucket is required")
	}
	// An S3-compatible endpoint has no real region, but SigV4 signing needs
	// one in the request; every such server accepts us-east-1. Real AWS must
	// be told the bucket's region, or every request earns a redirect the SDK
	// does not follow, so no default is applied without an endpoint.
	region := opts.Region
	if region == "" && opts.Endpoint != "" {
		region = DefaultCompatRegion
	}
	var loadOpts []func(*awsconfig.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	if opts.AccessKeyID != "" || opts.SecretAccessKey != "" {
		if opts.AccessKeyID == "" || opts.SecretAccessKey == "" {
			return nil, fmt.Errorf("s3store: access key ID and secret must be given together")
		}
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, opts.SessionToken)))
	}
	if opts.LegacyCompat {
		loadOpts = append(loadOpts,
			awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
			awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired))
	}
	if opts.Endpoint != "" {
		// A custom endpoint is not AWS, so an instance role cannot apply.
		// Without this, a host with no credentials configured spends the
		// metadata-service timeout on every call before failing.
		loadOpts = append(loadOpts, awsconfig.WithEC2IMDSClientEnableState(imds.ClientDisabled))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3store: load AWS config: %w", err)
	}

	// Resolve credentials once, now, so "none configured" is a clear error
	// at startup rather than a timeout inside the first request. A bounded
	// context keeps a slow credential provider from stalling the run.
	credCtx, cancel := context.WithTimeout(ctx, credentialTimeout)
	defer cancel()
	if _, err := cfg.Credentials.Retrieve(credCtx); err != nil {
		return nil, fmt.Errorf("s3store: no usable S3 credentials: set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY in the environment (remember that sudo drops it), or storage.s3.access_key_id/secret_access_key, or configure the AWS SDK chain: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = opts.UsePathStyle
	})
	return NewWithClient(client, opts), nil
}

// NewWithClient wraps an existing client. Only Bucket, Prefix, PartSize,
// Concurrency and StaleUploadAge are read from opts.
func NewWithClient(client *s3.Client, opts Options) *Store {
	partSize := opts.PartSize
	if partSize <= 0 {
		partSize = defaultPartSize
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	stale := opts.StaleUploadAge
	if stale <= 0 {
		stale = defaultStaleUploadAge
	}
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
		// A failed upload must not leave parts behind to be billed forever.
		u.LeavePartsOnError = false
	})
	return &Store{
		client:         client,
		uploader:       uploader,
		bucket:         opts.Bucket,
		prefix:         normalisePrefix(opts.Prefix),
		staleUploadAge: stale,
	}
}

func normalisePrefix(p string) string {
	p = strings.Trim(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func (s *Store) Name() string { return "s3" }

func (s *Store) Location() string {
	return "s3://" + s.bucket + "/" + s.prefix
}

func (s *Store) key(name string) (string, error) {
	if err := types.ValidateObjectName(name); err != nil {
		return "", err
	}
	return s.prefix + name, nil
}

// Create starts a streaming upload. Bytes written go to the uploader through a
// pipe; the SDK buffers them into parts and begins a multipart upload once
// the first part fills, or does a single PUT if the writer closes before that.
// Either way nothing is visible under the key until Commit completes it.
func (s *Store) Create(ctx context.Context, name string) (interfaces.ObjectWriter, error) {
	k, err := s.key(name)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	w := &writer{pw: pw, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(k),
			Body:   pr,
		})
		if err != nil {
			w.uploadErr = fmt.Errorf("s3store: upload %s: %w", name, err)
			// Unblock a producer stuck in Write. Without this, the same
			// deadlock writeEncryptedArchive once had — encryptor blocked in
			// pw.Write while nobody reads — would recur, with the dump held
			// open against the source database.
			pr.CloseWithError(w.uploadErr)
			return
		}
		pr.Close()
	}()
	return w, nil
}

type writer struct {
	pw        *io.PipeWriter
	done      chan struct{}
	uploadErr error
	finished  bool
}

var (
	errWriterDone = errors.New("s3store: writer already committed or aborted")
	errAborted    = errors.New("s3store: upload aborted by caller")
)

func (w *writer) Write(p []byte) (int, error) {
	if w.finished {
		return 0, errWriterDone
	}
	n, err := w.pw.Write(p)
	if err != nil {
		// A failed upload closed the read side; surface its error rather
		// than io.ErrClosedPipe.
		select {
		case <-w.done:
			if w.uploadErr != nil {
				return n, w.uploadErr
			}
		default:
		}
	}
	return n, err
}

// Commit signals EOF to the uploader and waits for it to complete the object.
// When it returns nil the CompleteMultipartUpload (or the single PUT) has been
// acknowledged, which is S3's durability guarantee.
func (w *writer) Commit() error {
	if w.finished {
		return errWriterDone
	}
	w.finished = true
	w.pw.Close()
	<-w.done
	return w.uploadErr
}

// Abort fails the uploader's read, which makes the SDK abort the multipart
// upload (LeavePartsOnError is false) or never issue the single PUT.
func (w *writer) Abort() error {
	if w.finished {
		return nil
	}
	w.finished = true
	w.pw.CloseWithError(errAborted)
	<-w.done
	// uploadErr is the expected abort failure; it is not a cleanup problem.
	return nil
}

func (s *Store) Open(ctx context.Context, name string) (io.ReadCloser, int64, error) {
	k, err := s.key(name)
	if err != nil {
		return nil, 0, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(k),
	})
	if err != nil {
		return nil, 0, wrapNotExist(name, err)
	}
	if out.ContentLength == nil {
		out.Body.Close()
		return nil, 0, fmt.Errorf("s3store: open %s: response has no Content-Length", name)
	}
	return out.Body, *out.ContentLength, nil
}

func (s *Store) Stat(ctx context.Context, name string) (types.ObjectInfo, error) {
	k, err := s.key(name)
	if err != nil {
		return types.ObjectInfo{}, err
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(k),
	})
	if err != nil {
		return types.ObjectInfo{}, wrapNotExist(name, err)
	}
	return types.ObjectInfo{
		Name:    name,
		Size:    aws.ToInt64(out.ContentLength),
		ModTime: aws.ToTime(out.LastModified),
	}, nil
}

// List pages through every key under the prefix. The delimiter makes nested
// "directories" come back as CommonPrefixes, which are ignored: this store is
// flat, and anything deeper was not written by it.
func (s *Store) List(ctx context.Context, prefix string) ([]types.ObjectInfo, error) {
	if strings.ContainsAny(prefix, `/\`) {
		return nil, fmt.Errorf("s3store: list prefix %q must not contain path separators", prefix)
	}
	var out []types.ObjectInfo
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(s.prefix + prefix),
		Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: list %s: %w", s.Location(), err)
		}
		for _, o := range page.Contents {
			name := strings.TrimPrefix(aws.ToString(o.Key), s.prefix)
			if name == "" || strings.Contains(name, "/") {
				continue
			}
			out = append(out, types.ObjectInfo{
				Name:    name,
				Size:    aws.ToInt64(o.Size),
				ModTime: aws.ToTime(o.LastModified),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete HEADs first because S3's DeleteObject succeeds on a missing key, and
// retention counts what it actually removed. The extra round trip is paid
// only on deletion, which is rare.
func (s *Store) Delete(ctx context.Context, name string) error {
	if _, err := s.Stat(ctx, name); err != nil {
		return err
	}
	k, _ := s.key(name)
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(k),
	}); err != nil {
		return fmt.Errorf("s3store: delete %s: %w", name, err)
	}
	return nil
}

// Sweep aborts incomplete multipart uploads under the prefix that are older
// than StaleUploadAge. An incomplete upload is invisible to List and to
// retention, is billed for its parts indefinitely, and is exactly what a run
// killed mid-archive leaves behind.
func (s *Store) Sweep(ctx context.Context) error {
	cutoff := time.Now().Add(-s.staleUploadAge)
	in := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.prefix),
	}
	var errs []error
	for {
		page, err := s.client.ListMultipartUploads(ctx, in)
		if err != nil {
			// Some S3 fakes answer an empty listing with NoSuchUpload; real
			// servers return an empty page. Nothing to sweep either way.
			var none *s3types.NoSuchUpload
			if errors.As(err, &none) {
				break
			}
			return fmt.Errorf("s3store: list multipart uploads: %w", err)
		}
		for _, u := range page.Uploads {
			if u.Initiated != nil && u.Initiated.After(cutoff) {
				continue
			}
			_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(s.bucket),
				Key:      u.Key,
				UploadId: u.UploadId,
			})
			if err != nil {
				var gone *s3types.NoSuchUpload
				if !errors.As(err, &gone) {
					errs = append(errs, fmt.Errorf("abort %s: %w", aws.ToString(u.Key), err))
				}
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		in.KeyMarker = page.NextKeyMarker
		in.UploadIdMarker = page.NextUploadIdMarker
	}
	return errors.Join(errs...)
}

func wrapNotExist(name string, err error) error {
	var noKey *s3types.NoSuchKey
	var notFound *s3types.NotFound
	if errors.As(err, &noKey) || errors.As(err, &notFound) {
		return fmt.Errorf("%s: %w", name, types.ErrObjectNotExist)
	}
	return fmt.Errorf("s3store: %s: %w", name, err)
}

// CreateBucket creates opts.Bucket. It exists for tests against a fresh
// server; production buckets are provisioned with their lifecycle rules,
// versioning and access policy, none of which this tool should decide.
func CreateBucket(ctx context.Context, opts Options) error {
	s, err := New(ctx, opts)
	if err != nil {
		return err
	}
	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	var owned *s3types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		return fmt.Errorf("s3store: create bucket %s: %w", s.bucket, err)
	}
	return nil
}
