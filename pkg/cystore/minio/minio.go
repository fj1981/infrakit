package minio

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	. "github.com/fj1981/infrakit/pkg/cystore"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioProvider implements the Provider interface for MinIO/S3 storage
type MinioProvider struct {
	client *minio.Client
	config *Config
}

func init() {
	RegistProvider(string(ProviderMinio), NewMinioProvider)
}

// cleanEndpoint removes any path components from the endpoint URL
func cleanEndpoint(endpoint string) (string, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %v", err)
	}
	return u.Host, nil
}

// NewMinioProvider creates a new MinIO storage provider
func NewMinioProvider(config *Config) (Provider, error) {
	// Clean up the endpoint URL
	endpoint, err := cleanEndpoint(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %v", err)
	}

	// Create MinIO client
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, config.SessionToken),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	return &MinioProvider{
		client: client,
		config: config,
	}, nil
}

// BucketExists checks if a bucket exists
func (p *MinioProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return p.client.BucketExists(ctx, bucketName)
}

// CreateBucket creates a new bucket
func (p *MinioProvider) CreateBucket(ctx context.Context, bucketName string) error {
	return p.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
}

// RemoveBucket removes a bucket
func (p *MinioProvider) RemoveBucket(ctx context.Context, bucketName string) error {
	return p.client.RemoveBucket(ctx, bucketName)
}

// ListBuckets lists all buckets
func (p *MinioProvider) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	buckets, err := p.client.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]BucketInfo, len(buckets))
	for i, bucket := range buckets {
		result[i] = BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		}
	}

	return result, nil
}

// PutObject uploads an object to a bucket
func (p *MinioProvider) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts PutObjectOptions) (ObjectInfo, error) {
	// Convert options
	minioOpts := minio.PutObjectOptions{
		ContentType:  opts.ContentType,
		UserMetadata: opts.Metadata,
	}

	// Upload object
	info, err := p.client.PutObject(ctx, bucketName, objectName, reader, objectSize, minioOpts)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:       info.Bucket,
		Name:         info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
		LastModified: time.Now(), // MinIO doesn't return LastModified in PutObjectResult
		ContentType:  opts.ContentType,
		Metadata:     opts.Metadata,
	}, nil
}

// GetObject downloads an object from a bucket
func (p *MinioProvider) GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, error) {
	// Convert options
	minioOpts := minio.GetObjectOptions{}
	if opts.Range != "" {
		// Parse range format "bytes=start-end"
		start, end := parseRangeHeader(opts.Range)
		err := minioOpts.SetRange(start, end)
		if err != nil {
			return nil, ObjectInfo{}, err
		}
	}
	if opts.MatchETag != "" {
		minioOpts.SetMatchETag(opts.MatchETag)
	}
	if opts.NotMatchETag != "" {
		minioOpts.SetMatchETag(opts.NotMatchETag)
	}

	// Get object
	obj, err := p.client.GetObject(ctx, bucketName, objectName, minioOpts)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	// Get object info
	stat, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, ObjectInfo{}, err
	}

	info := ObjectInfo{
		Bucket:       bucketName, // Use bucketName parameter instead of stat.Bucket
		Name:         stat.Key,
		ETag:         stat.ETag,
		Size:         stat.Size,
		LastModified: stat.LastModified,
		ContentType:  stat.ContentType,
		Metadata:     stat.UserMetadata,
	}

	return obj, info, nil
}

// StatObject gets object metadata
func (p *MinioProvider) StatObject(ctx context.Context, bucketName, objectName string) (ObjectInfo, error) {
	info, err := p.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:       bucketName, // Use bucketName parameter instead of info.Bucket
		Name:         info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
		Metadata:     info.UserMetadata,
	}, nil
}

// RemoveObject removes an object from a bucket
func (p *MinioProvider) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	return p.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
}

// ListObjects lists objects in a bucket
func (p *MinioProvider) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) <-chan ObjectInfo {
	resultCh := make(chan ObjectInfo)

	go func() {
		defer close(resultCh)

		opts := minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: recursive,
		}

		for object := range p.client.ListObjects(ctx, bucketName, opts) {
			if object.Err != nil {
				// Just skip objects with errors
				continue
			}

			resultCh <- ObjectInfo{
				Bucket:       bucketName,
				Name:         object.Key,
				ETag:         object.ETag,
				Size:         object.Size,
				LastModified: object.LastModified,
				ContentType:  object.ContentType,
			}
		}
	}()

	return resultCh
}

// PresignedGetObject generates a presigned URL for GET operation
func (p *MinioProvider) PresignedGetObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	url, err := p.client.PresignedGetObject(ctx, bucketName, objectName, expires, nil)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// PresignedPutObject generates a presigned URL for PUT operation
func (p *MinioProvider) PresignedPutObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	url, err := p.client.PresignedPutObject(ctx, bucketName, objectName, expires)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// parseRangeHeader parses range header format "bytes=start-end" and returns start, end
func parseRangeHeader(rangeHeader string) (int64, int64) {
	// Default values
	var start, end int64 = 0, -1

	// Remove "bytes=" prefix if present
	if strings.HasPrefix(rangeHeader, "bytes=") {
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
	}

	// Split by "-"
	parts := strings.Split(rangeHeader, "-")
	if len(parts) == 2 {
		if parts[0] != "" {
			if s, err := fmt.Sscanf(parts[0], "%d", &start); err == nil && s == 1 {
				// Valid start
			}
		}
		if parts[1] != "" {
			if s, err := fmt.Sscanf(parts[1], "%d", &end); err == nil && s == 1 {
				// Valid end
			}
		}
	}

	return start, end
}
