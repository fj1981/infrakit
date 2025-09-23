package huawei

import (
	"context"
	"io"
	"time"

	. "github.com/fj1981/infrakit/pkg/cystore"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// HuaweiOBSProvider implements the Provider interface for Huawei Cloud OBS
type HuaweiOBSProvider struct {
	client *obs.ObsClient
	config *Config
}

func init() {
	RegistProvider(string(ProviderHuaweiOBS), NewHuaweiOBSProvider)
}

// NewHuaweiOBSProvider creates a new Huawei Cloud OBS provider
func NewHuaweiOBSProvider(config *Config) (Provider, error) {
	// Create OBS client
	client, err := obs.New(config.AccessKey, config.SecretKey, config.Endpoint,
		obs.WithSecurityToken(config.SessionToken))
	if err != nil {
		return nil, err
	}

	return &HuaweiOBSProvider{
		client: client,
		config: config,
	}, nil
}

// BucketExists checks if a bucket exists
func (p *HuaweiOBSProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	_, err := p.client.HeadBucket(bucketName)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok && obsError.StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CreateBucket creates a new bucket
func (p *HuaweiOBSProvider) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := p.client.CreateBucket(&obs.CreateBucketInput{
		Bucket: bucketName,
	})
	return err
}

// RemoveBucket removes a bucket
func (p *HuaweiOBSProvider) RemoveBucket(ctx context.Context, bucketName string) error {
	_, err := p.client.DeleteBucket(bucketName)
	return err
}

// ListBuckets lists all buckets
func (p *HuaweiOBSProvider) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	output, err := p.client.ListBuckets(nil)
	if err != nil {
		return nil, err
	}

	result := make([]BucketInfo, len(output.Buckets))
	for i, bucket := range output.Buckets {
		result[i] = BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		}
	}

	return result, nil
}

// PutObject uploads an object to a bucket
func (p *HuaweiOBSProvider) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts PutObjectOptions) (ObjectInfo, error) {
	// Create input struct
	input := obs.PutObjectInput{}
	input.Bucket = bucketName
	input.Key = objectName
	input.Body = reader

	// Upload object
	output, err := p.client.PutObject(&input)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         output.ETag,
		Size:         objectSize,
		LastModified: time.Now(), // OBS doesn't return LastModified in PutObjectOutput
		ContentType:  opts.ContentType,
		Metadata:     opts.Metadata,
	}, nil
}

// GetObject downloads an object from a bucket
func (p *HuaweiOBSProvider) GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, error) {
	// Create input struct
	input := obs.GetObjectInput{}
	input.Bucket = bucketName
	input.Key = objectName

	// Get object
	output, err := p.client.GetObject(&input)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	info := ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         output.ETag,
		Size:         output.ContentLength,
		LastModified: output.LastModified,
		ContentType:  output.ContentType,
		Metadata:     output.Metadata,
	}

	return output.Body, info, nil
}

// StatObject gets object metadata
func (p *HuaweiOBSProvider) StatObject(ctx context.Context, bucketName, objectName string) (ObjectInfo, error) {
	// Create input struct
	input := obs.GetObjectMetadataInput{}
	input.Bucket = bucketName
	input.Key = objectName

	// Get object metadata
	output, err := p.client.GetObjectMetadata(&input)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:       bucketName,
		Name:         objectName,
		ETag:         output.ETag,
		Size:         output.ContentLength,
		LastModified: output.LastModified,
		ContentType:  output.ContentType,
		Metadata:     output.Metadata,
	}, nil
}

// RemoveObject removes an object from a bucket
func (p *HuaweiOBSProvider) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	// Create input struct
	input := obs.DeleteObjectInput{}
	input.Bucket = bucketName
	input.Key = objectName

	// Delete object
	_, err := p.client.DeleteObject(&input)
	return err
}

// ListObjects lists objects in a bucket
func (p *HuaweiOBSProvider) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) <-chan ObjectInfo {
	resultCh := make(chan ObjectInfo)

	go func() {
		defer close(resultCh)

		marker := ""
		for {
			// Create input struct
			input := obs.ListObjectsInput{}
			input.Bucket = bucketName
			input.Prefix = prefix
			input.Marker = marker

			output, err := p.client.ListObjects(&input)
			if err != nil {
				// Just break on error
				break
			}

			for _, object := range output.Contents {
				resultCh <- ObjectInfo{
					Bucket:       bucketName,
					Name:         object.Key,
					ETag:         object.ETag,
					Size:         object.Size,
					LastModified: object.LastModified,
				}
			}

			if !output.IsTruncated {
				break
			}
			marker = output.NextMarker
		}
	}()

	return resultCh
}

// PresignedGetObject generates a presigned URL for GET operation
func (p *HuaweiOBSProvider) PresignedGetObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	expireSeconds := int(expires.Seconds())
	output, err := p.client.CreateSignedUrl(&obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodGet,
		Bucket:  bucketName,
		Key:     objectName,
		Expires: expireSeconds,
	})
	if err != nil {
		return "", err
	}
	return output.SignedUrl, nil
}

// PresignedPutObject generates a presigned URL for PUT operation
func (p *HuaweiOBSProvider) PresignedPutObject(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	expireSeconds := int(expires.Seconds())
	output, err := p.client.CreateSignedUrl(&obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodPut,
		Bucket:  bucketName,
		Key:     objectName,
		Expires: expireSeconds,
	})
	if err != nil {
		return "", err
	}
	return output.SignedUrl, nil
}

// Helper function to parse range header
func parseRange(rangeHeader string) (int64, int64) {
	// Simple implementation, assumes format "bytes=start-end"
	var start, end int64 = 0, -1

	// Parse range header
	// This is a simplified implementation
	// In a real implementation, you would need to handle more complex range formats

	return start, end
}
