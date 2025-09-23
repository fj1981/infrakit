package cystore

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Store provides a unified interface for cloud storage operations
type Store struct {
	provider      Provider
	config        *Config
	defaultBucket string // default bucket name
}

var (
	providers         sync.Map
	ErrBucketRequired = errors.New("bucket is required and no default bucket is set")
)

type FuncNewStore func(config *Config) (Provider, error)

func RegistProvider(name string, funcProvider FuncNewStore) {
	providers.Store(name, funcProvider)
}

type Option func(*Store)

// WithBucket sets the default bucket for all operations
func WithBucket(bucket string) Option {
	return func(s *Store) {
		s.defaultBucket = bucket
	}
}

// getBucket returns the bucket name to use, preferring the provided one over the default
func (s *Store) getBucket(bucket []string) (string, error) {
	if len(bucket) > 0 && bucket[0] != "" {
		return bucket[0], nil
	}
	if s.defaultBucket != "" {
		return s.defaultBucket, nil
	}
	return "", ErrBucketRequired
}

// NewStore creates a new storage client based on the provided configuration
func NewStore(config *Config, opts ...Option) (*Store, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	pFunc, ok := providers.Load(string(config.Provider))
	if ok {
		provider, ok := pFunc.(FuncNewStore)
		if ok {
			p, err := provider(config)
			if err != nil {
				return nil, err
			}
			store := &Store{
				provider: p,
				config:   config,
			}

			// Apply options
			for _, opt := range opts {
				opt(store)
			}

			return store, nil
		}
	}

	return nil, ErrProviderNotFound
}

// Provider returns the underlying provider
func (s *Store) Provider() Provider {
	return s.provider
}

// Config returns the store configuration
func (s *Store) Config() *Config {
	return s.config
}

// EnsureBucket ensures that a bucket exists, creating it if necessary
func (s *Store) EnsureBucket(ctx context.Context, bucketName string) error {
	exists, err := s.provider.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}

	if !exists {
		return s.provider.CreateBucket(ctx, bucketName)
	}

	return nil
}

// Upload uploads a file to the specified bucket and object path
// If bucket is nil, the default bucket will be used if set
func (s *Store) Upload(ctx context.Context, objectPath string, data io.Reader, size int64, contentType string, bucket ...string) (string, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return "", err
	}
	_, err = s.provider.PutObject(ctx, bucketName, objectPath, data, size, PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", err
	}
	return objectPath, nil
}

// Download downloads a file from the specified bucket and object path
// If bucket is nil, the default bucket will be used if set
func (s *Store) Download(ctx context.Context, objectPath string, bucket ...string) (io.ReadCloser, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	reader, _, err := s.provider.GetObject(ctx, bucketName, objectPath, GetObjectOptions{})
	return reader, err
}

// Delete deletes a file from the specified bucket and object path
// If bucket is nil, the default bucket will be used if set
func (s *Store) Delete(ctx context.Context, objectPath string, bucket ...string) error {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return err
	}
	return s.provider.RemoveObject(ctx, bucketName, objectPath)
}

// GeneratePresignedURL generates a presigned URL for the given object
// If bucket is nil, the default bucket will be used if set
func (s *Store) GeneratePresignedURL(ctx context.Context, objectPath string, expiry time.Duration, bucket ...string) (string, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return "", err
	}
	return s.provider.PresignedGetObject(ctx, bucketName, objectPath, expiry)
}

// PutURL generates a presigned URL for uploading an object
func (s *Store) PutURL(ctx context.Context, bucketName, objectPath string, expirySeconds int) (string, error) {
	return s.provider.PresignedPutObject(ctx, bucketName, objectPath,
		secondsToDuration(expirySeconds))
}

// ListObjects lists objects in a bucket with the given prefix
// If bucket is nil, the default bucket will be used if set
func (s *Store) ListObjects(ctx context.Context, prefix string, bucket ...string) ([]ObjectInfo, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	objectCh := s.provider.ListObjects(ctx, bucketName, prefix, false)
	var objects []ObjectInfo
	for obj := range objectCh {
		objects = append(objects, obj)
	}
	return objects, nil
}

// GetObjectInfo gets metadata for an object
// If bucket is nil, the default bucket will be used if set
func (s *Store) GetObjectInfo(ctx context.Context, objectPath string, bucket ...string) (ObjectInfo, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return ObjectInfo{}, err
	}
	return s.provider.StatObject(ctx, bucketName, objectPath)
}

// FileExists checks if a file exists
func (s *Store) FileExists(ctx context.Context, objectPath string, bucket ...string) (bool, error) {
	bucketName, err := s.getBucket(bucket)
	if err != nil {
		return false, err
	}
	_, err = s.provider.StatObject(ctx, bucketName, objectPath)
	if err != nil {
		if strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "not exist") ||
			strings.Contains(err.Error(), "NoSuchKey") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// BuildPath builds a path from components
func BuildPath(components ...string) string {
	return filepath.Join(components...)
}

// SplitPath splits a path into directory and filename
func SplitPath(path string) (dir, file string) {
	return filepath.Split(path)
}

// GetContentType determines the content type based on file extension
func GetContentType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	case ".html":
		return "text/html"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".zip":
		return "application/zip"
	case ".doc", ".docx":
		return "application/msword"
	case ".xls", ".xlsx":
		return "application/vnd.ms-excel"
	case ".ppt", ".pptx":
		return "application/vnd.ms-powerpoint"
	default:
		return "application/octet-stream"
	}
}

// secondsToDuration converts seconds to time.Duration
func secondsToDuration(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}
