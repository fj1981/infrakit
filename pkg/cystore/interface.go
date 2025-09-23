package cystore

import (
	"context"
	"io"
	"time"
)

// Provider defines the interface for cloud storage operations
type Provider interface {
	// Bucket operations
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	CreateBucket(ctx context.Context, bucketName string) error
	RemoveBucket(ctx context.Context, bucketName string) error
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	// Object operations
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts PutObjectOptions) (ObjectInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, error)
	StatObject(ctx context.Context, bucketName, objectName string) (ObjectInfo, error)
	RemoveObject(ctx context.Context, bucketName, objectName string) error
	ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) <-chan ObjectInfo

	// Presigned URL operations
	PresignedGetObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error)
	PresignedPutObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error)
}

// BucketInfo contains information about a bucket
type BucketInfo struct {
	Name         string    // Name of the bucket
	CreationDate time.Time // Creation date of the bucket
}

// ObjectInfo contains information about an object
type ObjectInfo struct {
	Bucket       string            // Bucket name
	Name         string            // Object name
	ETag         string            // ETag of the object
	Size         int64             // Size of the object
	LastModified time.Time         // Last modified time of the object
	ContentType  string            // Content type of the object
	Metadata     map[string]string // User-defined metadata
}

// PutObjectOptions specifies options for PutObject operation
type PutObjectOptions struct {
	ContentType string            // Content type of the object
	Metadata    map[string]string // User-defined metadata
}

// GetObjectOptions specifies options for GetObject operation
type GetObjectOptions struct {
	Range        string // Range of bytes to download
	MatchETag    string // Download object if ETag matches
	NotMatchETag string // Download object if ETag doesn't match
}
