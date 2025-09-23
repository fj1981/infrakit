package cystore

import (
	"errors"
	"time"
)

// Common errors
var (
	ErrInvalidConfig     = errors.New("invalid storage configuration")
	ErrProviderNotFound  = errors.New("storage provider not found")
	ErrBucketNotFound    = errors.New("bucket not found")
	ErrObjectNotFound    = errors.New("object not found")
	ErrInvalidObjectName = errors.New("invalid object name")
)

// ProviderType represents the type of storage provider
type ProviderType string

const (
	// ProviderMinio represents MinIO/S3 compatible storage
	ProviderMinio ProviderType = "minio"
	// ProviderLocal represents local file system storage
	ProviderLocal ProviderType = "local"
	// ProviderHuaweiOBS represents Huawei Cloud Object Storage Service
	ProviderHuaweiOBS ProviderType = "huawei_obs"
	// ProviderAliyunOSS represents Alibaba Cloud Object Storage Service
	ProviderAliyunOSS ProviderType = "aliyun_oss"
	// Add more providers as needed
)

// Config represents the unified configuration for all storage providers
type Config struct {
	// Provider type (minio, local, etc.)
	Provider ProviderType `json:"provider" yaml:"provider"`

	// Common settings
	Region  string        `json:"region" yaml:"region"`
	Secure  bool          `json:"secure" yaml:"secure"`
	Timeout time.Duration `json:"timeout" yaml:"timeout"`

	// Unified storage settings
	// These fields are used across different providers
	Endpoint     string `json:"endpoint" yaml:"endpoint"`
	AccessKey    string `json:"access_key" yaml:"access_key"`
	SecretKey    string `json:"secret_key" yaml:"secret_key"`
	SessionToken string `json:"session_token,omitempty" yaml:"session_token,omitempty"`
	UseSSL       bool   `json:"use_ssl" yaml:"use_ssl"`

	// Provider-specific settings that don't fit in the common fields
	// Local storage specific
	BasePath string `json:"base_path,omitempty" yaml:"base_path,omitempty"`
}

// Provider-specific config structs have been removed as part of the unified configuration

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Provider == "" {
		return ErrInvalidConfig
	}

	// Common validation for cloud providers
	switch c.Provider {
	case ProviderMinio, ProviderHuaweiOBS, ProviderAliyunOSS:
		if c.Endpoint == "" {
			return errors.New("endpoint is required")
		}
		if c.AccessKey == "" {
			return errors.New("access key is required")
		}
		if c.SecretKey == "" {
			return errors.New("secret key is required")
		}
	case ProviderLocal:
		if c.BasePath == "" {
			return errors.New("base path is required for local storage")
		}
	default:
		return ErrProviderNotFound
	}

	return nil
}
