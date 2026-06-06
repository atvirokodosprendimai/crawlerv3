// Package s3 implements lake.BlobStore on top of AWS S3 (or any S3-compatible
// service via a custom endpoint, e.g. MinIO).
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// Config controls Store construction.
type Config struct {
	Bucket          string
	Region          string
	Endpoint        string // optional override (MinIO, R2, etc.)
	AccessKeyID     string // optional; falls back to env / SDK chain
	SecretAccessKey string
	UsePathStyle    bool // true for MinIO and many self-hosted services
}

// Store is an S3-backed BlobStore.
type Store struct {
	bucket string
	client *awss3.Client
}

// New constructs a Store. Empty AccessKeyID/SecretAccessKey defers to the
// AWS SDK's default credential chain.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 store: Bucket required")
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	cfgAWS, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 store: load config: %w", err)
	}
	client := awss3.NewFromConfig(cfgAWS, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})
	return &Store{bucket: cfg.Bucket, client: client}, nil
}

// Backend implements lake.BlobStore.
func (s *Store) Backend() string { return "s3" }

// Put streams r to S3 and verifies the SHA matches the provided meta (if any).
//
// The S3 SDK requires a seekable body for non-multipart PutObject; we buffer
// the content while computing sha256 in one pass.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, m lake.PutMeta) (lake.Stat, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64*1024))
	h := sha256.New()
	tee := io.TeeReader(r, h)
	n, err := io.Copy(buf, tee)
	if err != nil {
		return lake.Stat{}, fmt.Errorf("s3 Put: read: %w", err)
	}
	got := h.Sum(nil)
	if len(m.SHA256) > 0 && !equalBytes(got, m.SHA256) {
		return lake.Stat{}, errors.New("s3 Put: sha256 mismatch")
	}
	in := &awss3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: nilIfEmpty(m.ContentType),
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return lake.Stat{}, fmt.Errorf("s3 Put: %w", err)
	}
	return lake.Stat{
		Size:        n,
		ContentType: m.ContentType,
		SHA256:      got,
		ModTime:     time.Now().UTC(),
	}, nil
}

// Get fetches the object body. The caller must Close it.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, lake.Stat, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, lake.Stat{}, err
	}
	st := lake.Stat{
		Size:    aws.ToInt64(out.ContentLength),
		ModTime: time.Now().UTC(),
	}
	if out.ContentType != nil {
		st.ContentType = *out.ContentType
	}
	if out.LastModified != nil {
		st.ModTime = out.LastModified.UTC()
	}
	return out.Body, st, nil
}

// Stat returns metadata via HEAD.
func (s *Store) Stat(ctx context.Context, key string) (lake.Stat, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return lake.Stat{}, err
	}
	st := lake.Stat{Size: aws.ToInt64(out.ContentLength)}
	if out.ContentType != nil {
		st.ContentType = *out.ContentType
	}
	if out.LastModified != nil {
		st.ModTime = out.LastModified.UTC()
	}
	return st, nil
}

// Delete removes the object.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return nil
	}
	return err
}

// keep http imported in case we need it for future presign helpers
var _ = http.StatusOK

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
