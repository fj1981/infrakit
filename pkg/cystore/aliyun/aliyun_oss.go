package aliyun

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	. "github.com/fj1981/infrakit/pkg/cystore"
)

// AliyunOSSProvider implements the Provider interface for Alibaba Cloud OSS
type AliyunOSSProvider struct {
	client *oss.Client
	config *Config
}

func init() {
	RegistProvider(string(ProviderAliyunOSS), NewOSSProvider)
}

// NewAliyunOSSProvider creates a new Alibaba Cloud OSS provider
func NewOSSProvider(config *Config) (Provider, error) {
	// Create OSS client
	var options []oss.ClientOption
	if config.SessionToken != "" {
		options = append(options, oss.SecurityToken(config.SessionToken))
	}

	client, err := oss.New(config.Endpoint, config.AccessKey, config.SecretKey, options...)
	if err != nil {
		return nil, err
	}

	return &AliyunOSSProvider{
		client: client,
		config: config,
	}, nil
}

// BucketExists checks if a bucket exists
func (p *AliyunOSSProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return p.client.IsBucketExist(bucketName)
}

// CreateBucket creates a new bucket
func (p *AliyunOSSProvider) CreateBucket(ctx context.Context, bucketName string) error {
	return p.client.CreateBucket(bucketName)
}

// RemoveBucket removes a bucket
func (p *AliyunOSSProvider) RemoveBucket(ctx context.Context, bucketName string) error {
	return p.client.DeleteBucket(bucketName)
}

// ListBuckets lists all buckets
func (p *AliyunOSSProvider) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	listResult, err := p.client.ListBuckets()
	if err != nil {
		return nil, err
	}

	result := make([]BucketInfo, len(listResult.Buckets))
	for i, bucket := range listResult.Buckets {
		result[i] = BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		}
	}

	return result, nil
}

// PutObject uploads an object to a bucket
func (p *AliyunOSSProvider) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts PutObjectOptions) (ObjectInfo, error) {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Convert options
	options := []oss.Option{
		oss.ContentType(opts.ContentType),
	}

	// Add metadata
	for k, v := range opts.Metadata {
		options = append(options, oss.Meta(k, v))
	}

	// Upload object
	err = bucket.PutObject(objectName, reader, options...)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Get object info
	header, err := bucket.GetObjectDetailedMeta(objectName)
	if err != nil {
		// Return basic info if we can't get detailed metadata
		return ObjectInfo{
			Bucket:       bucketName,
			Name:         objectName,
			Size:         objectSize,
			LastModified: time.Now(),
			ContentType:  opts.ContentType,
			Metadata:     opts.Metadata,
		}, nil
	}

	// Parse ETag (remove quotes)
	etag := header.Get("ETag")
	etag = strings.Trim(etag, "\"")

	// Parse last modified
	lastModified, _ := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	return ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         etag,
		Size:         objectSize,
		LastModified: lastModified,
		ContentType:  header.Get("Content-Type"),
		Metadata:     extractMetadata(header),
	}, nil
}

// GetObject downloads an object from a bucket
func (p *AliyunOSSProvider) GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, error) {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	// Convert options
	options := []oss.Option{}
	if opts.Range != "" {
		// Parse range format "bytes=start-end"
		start, end := parseRangeHeader(opts.Range)
		options = append(options, oss.Range(start, end))
	}
	if opts.MatchETag != "" {
		options = append(options, oss.IfMatch(opts.MatchETag))
	}
	if opts.NotMatchETag != "" {
		options = append(options, oss.IfNoneMatch(opts.NotMatchETag))
	}

	// Get object
	object, err := bucket.GetObject(objectName, options...)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	// Get object metadata
	header, err := bucket.GetObjectDetailedMeta(objectName)
	if err != nil {
		// Close the object if we can't get metadata
		object.Close()
		return nil, ObjectInfo{}, err
	}

	// Parse ETag (remove quotes)
	etag := header.Get("ETag")
	etag = strings.Trim(etag, "\"")

	// Parse content length
	contentLength := int64(0)
	if lenStr := header.Get("Content-Length"); lenStr != "" {
		contentLength = parseContentLength(lenStr)
	}

	// Parse last modified
	lastModified, _ := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	info := ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         etag,
		Size:         contentLength,
		LastModified: lastModified,
		ContentType:  header.Get("Content-Type"),
		Metadata:     extractMetadata(header),
	}

	return object, info, nil
}

// StatObject gets object metadata
func (p *AliyunOSSProvider) StatObject(ctx context.Context, bucketName, objectName string) (ObjectInfo, error) {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Get object metadata
	header, err := bucket.GetObjectDetailedMeta(objectName)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Parse ETag (remove quotes)
	etag := header.Get("ETag")
	etag = strings.Trim(etag, "\"")

	// Parse content length
	contentLength := int64(0)
	if lenStr := header.Get("Content-Length"); lenStr != "" {
		contentLength = parseContentLength(lenStr)
	}

	// Parse last modified
	lastModified, _ := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	return ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         etag,
		Size:         contentLength,
		LastModified: lastModified,
		ContentType:  header.Get("Content-Type"),
		Metadata:     extractMetadata(header),
	}, nil
}

// RemoveObject removes an object from a bucket
func (p *AliyunOSSProvider) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return err
	}

	// Delete object
	return bucket.DeleteObject(objectName)
}

// ListObjects lists objects in a bucket
func (p *AliyunOSSProvider) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) <-chan ObjectInfo {
	resultCh := make(chan ObjectInfo)

	go func() {
		defer close(resultCh)

		// Get bucket
		bucket, err := p.client.Bucket(bucketName)
		if err != nil {
			return
		}

		// Set listing options
		options := oss.MaxKeys(1000)
		if !recursive {
			options = oss.Delimiter("/")
		}

		marker := ""
		for {
			lsRes, err := bucket.ListObjects(oss.Marker(marker), oss.Prefix(prefix), options)
			if err != nil {
				// Just break on error
				break
			}

			for _, object := range lsRes.Objects {
				resultCh <- ObjectInfo{
					Bucket:       bucketName,
					Name:         object.Key,
					ETag:         strings.Trim(object.ETag, "\""),
					Size:         object.Size,
					LastModified: object.LastModified,
				}
			}

			if !lsRes.IsTruncated {
				break
			}
			marker = lsRes.NextMarker
		}
	}()

	return resultCh
}

// PresignedGetObject generates a presigned URL for GET operation
func (p *AliyunOSSProvider) PresignedGetObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return "", err
	}

	// Generate URL
	signedURL, err := bucket.SignURL(objectName, oss.HTTPGet, int64(expires.Seconds()))
	if err != nil {
		return "", err
	}

	return signedURL, nil
}

// PresignedPutObject generates a presigned URL for PUT operation
func (p *AliyunOSSProvider) PresignedPutObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	// Get bucket
	bucket, err := p.client.Bucket(bucketName)
	if err != nil {
		return "", err
	}

	// Generate URL
	signedURL, err := bucket.SignURL(objectName, oss.HTTPPut, int64(expires.Seconds()))
	if err != nil {
		return "", err
	}

	return signedURL, nil
}

// Helper functions

// extractMetadata extracts user metadata from OSS headers
func extractMetadata(header map[string][]string) map[string]string {
	metadata := make(map[string]string)
	for k, v := range header {
		if strings.HasPrefix(k, "X-Oss-Meta-") {
			key := strings.TrimPrefix(k, "X-Oss-Meta-")
			if len(v) > 0 {
				metadata[key] = v[0]
			}
		}
	}
	return metadata
}

// parseContentLength parses content length from header
func parseContentLength(s string) int64 {
	var size int64
	_, err := fmt.Sscanf(s, "%d", &size)
	if err != nil {
		return 0
	}
	return size
}

// parseRangeHeader parses range header format "bytes=start-end" and returns start, end
func parseRangeHeader(rangeHeader string) (int64, int64) {
	// Default values
	var start, end int64 = 0, -1

	// Remove "bytes=" prefix if present
	if after, ok := strings.CutPrefix(rangeHeader, "bytes="); ok {
		rangeHeader = after
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
