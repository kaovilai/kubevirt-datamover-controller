/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// Compile-time check that S3ObjectStore implements velero.ObjectStore
var _ velero.ObjectStore = (*S3ObjectStore)(nil)

// S3ObjectStore implements velero.ObjectStore for AWS S3 and S3-compatible storage.
type S3ObjectStore struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string
}

// NewS3ObjectStore creates a new S3ObjectStore.
// For standalone use without calling Init().
func NewS3ObjectStore(bucket, prefix, region, credentialsFile string) (*S3ObjectStore, error) {
	store := &S3ObjectStore{
		bucket: bucket,
		prefix: prefix,
	}

	configMap := map[string]string{
		"bucket":          bucket,
		"prefix":          prefix,
		"region":          region,
		"credentialsFile": credentialsFile,
	}

	if err := store.Init(configMap); err != nil {
		return nil, err
	}

	return store, nil
}

// Init initializes the ObjectStore with the provided config.
// This implements velero.ObjectStore.Init().
// Expected config keys: bucket, prefix, region, credentialsFile
func (s *S3ObjectStore) Init(configMap map[string]string) error {
	bucket := configMap["bucket"]
	prefix := configMap["prefix"]
	region := configMap["region"]
	credentialsFile := configMap["credentialsFile"]

	if bucket == "" {
		return fmt.Errorf("bucket is required in config")
	}

	s.bucket = bucket
	s.prefix = prefix

	ctx := context.Background()

	// Build config options
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Load credentials from file if specified
	if credentialsFile != "" {
		creds, err := loadAWSCredentialsFromFile(credentialsFile)
		if err != nil {
			return fmt.Errorf("failed to load credentials: %w", err)
		}
		opts = append(opts, config.WithCredentialsProvider(creds))
	}

	// Add retry configuration for transient errors
	opts = append(opts, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 3
		})
	}))

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s.client = s3.NewFromConfig(cfg)
	s.uploader = manager.NewUploader(s.client)

	return nil
}

// fullKey returns the full object key including the prefix.
func (s *S3ObjectStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return strings.TrimSuffix(s.prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}

// PutObject uploads an object to S3.
// Implements velero.ObjectStore.PutObject().
func (s *S3ObjectStore) PutObject(bucket, key string, body io.Reader) error {
	_, err := s.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, err)
	}
	return nil
}

// GetObject retrieves an object from S3.
// Implements velero.ObjectStore.GetObject().
func (s *S3ObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	return output.Body, nil
}

// ObjectExists checks if an object exists in S3.
// Implements velero.ObjectStore.ObjectExists().
func (s *S3ObjectStore) ObjectExists(bucket, key string) (bool, error) {
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		// Check if it's a not found error using AWS SDK error types
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		// Also check for NoSuchKey which can be returned for missing objects
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence %s: %w", key, err)
	}
	return true, nil
}

// DeleteObject removes an object from S3.
// Implements velero.ObjectStore.DeleteObject().
func (s *S3ObjectStore) DeleteObject(bucket, key string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}
	return nil
}

// ListCommonPrefixes gets a list of all object key prefixes that start with
// the specified prefix and stop at the next instance of the provided delimiter.
// Implements velero.ObjectStore.ListCommonPrefixes().
func (s *S3ObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)

	var prefixes []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(fullPrefix),
		Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list common prefixes: %w", err)
		}
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				prefixes = append(prefixes, *p.Prefix)
			}
		}
	}

	return prefixes, nil
}

// ListObjects gets a list of all keys in the specified bucket that have the given prefix.
// Implements velero.ObjectStore.ListObjects().
func (s *S3ObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}

	return keys, nil
}

// CreateSignedURL creates a pre-signed URL for the given bucket and key that expires after ttl.
// Implements velero.ObjectStore.CreateSignedURL().
func (s *S3ObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(s.client)
	req, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("failed to create signed URL: %w", err)
	}
	return req.URL, nil
}

// Convenience methods for our uploader use case

// PutObjectWithBucket uploads an object using the configured bucket.
func (s *S3ObjectStore) PutObjectWithBucket(key string, body io.Reader) error {
	return s.PutObject(s.bucket, key, body)
}

// GetObjectWithBucket retrieves an object using the configured bucket.
func (s *S3ObjectStore) GetObjectWithBucket(key string) (io.ReadCloser, error) {
	return s.GetObject(s.bucket, key)
}

// PutObjectBytes uploads bytes using the configured bucket.
func (s *S3ObjectStore) PutObjectBytes(key string, data []byte) error {
	return s.PutObject(s.bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes using the configured bucket.
func (s *S3ObjectStore) GetObjectBytes(key string) ([]byte, error) {
	reader, err := s.GetObject(s.bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	return io.ReadAll(reader)
}

// loadAWSCredentialsFromFile loads AWS credentials from a Velero-style credentials file.
// The file format is INI-style with [default] profile containing aws_access_key_id and aws_secret_access_key.
func loadAWSCredentialsFromFile(path string) (aws.CredentialsProvider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read credentials file: %w", err)
	}

	var accessKeyID, secretAccessKey string

	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "aws_access_key_id") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				accessKeyID = strings.TrimSpace(parts[1])
			}
		} else if strings.HasPrefix(line, "aws_secret_access_key") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				secretAccessKey = strings.TrimSpace(parts[1])
			}
		}
	}

	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("credentials file missing aws_access_key_id or aws_secret_access_key")
	}

	return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""), nil
}

// InitObjectStore creates an ObjectStore based on the provider type.
// Returns a velero.ObjectStore implementation.
func InitObjectStore(cfg *UploaderConfig) (velero.ObjectStore, error) {
	switch strings.ToLower(cfg.BSLProvider) {
	case "aws", "":
		return NewS3ObjectStore(
			cfg.BSLBucket,
			cfg.BSLPrefix,
			cfg.BSLRegion,
			cfg.CredentialsFile,
		)
	case "gcp":
		return nil, fmt.Errorf("gcp object store not yet implemented")
	case "azure":
		return nil, fmt.Errorf("azure object store not yet implemented")
	default:
		// Try S3-compatible for unknown providers
		return NewS3ObjectStore(
			cfg.BSLBucket,
			cfg.BSLPrefix,
			cfg.BSLRegion,
			cfg.CredentialsFile,
		)
	}
}
