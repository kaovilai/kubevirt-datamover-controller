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
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// MockObjectStore is an in-memory implementation of ObjectStore for testing.
type MockObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte // key -> data
	bucket  string
	prefix  string
}

// NewMockObjectStore creates a new mock object store.
func NewMockObjectStore(bucket, prefix string) *MockObjectStore {
	return &MockObjectStore{
		objects: make(map[string][]byte),
		bucket:  bucket,
		prefix:  prefix,
	}
}

func (m *MockObjectStore) fullKey(key string) string {
	if m.prefix == "" {
		return key
	}
	return strings.TrimSuffix(m.prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}

// Init implements velero.ObjectStore.Init
func (m *MockObjectStore) Init(config map[string]string) error {
	if config["bucket"] == "" {
		return fmt.Errorf("bucket is required")
	}
	m.bucket = config["bucket"]
	m.prefix = config["prefix"]
	return nil
}

// PutObject implements velero.ObjectStore.PutObject
func (m *MockObjectStore) PutObject(bucket, key string, body io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	fullKey := m.fullKey(key)
	m.objects[fullKey] = data
	return nil
}

// GetObject implements velero.ObjectStore.GetObject
func (m *MockObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullKey := m.fullKey(key)
	data, ok := m.objects[fullKey]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", fullKey)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// ObjectExists implements velero.ObjectStore.ObjectExists
func (m *MockObjectStore) ObjectExists(bucket, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullKey := m.fullKey(key)
	_, ok := m.objects[fullKey]
	return ok, nil
}

// DeleteObject implements velero.ObjectStore.DeleteObject
func (m *MockObjectStore) DeleteObject(bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullKey := m.fullKey(key)
	delete(m.objects, fullKey)
	return nil
}

// ListCommonPrefixes implements velero.ObjectStore.ListCommonPrefixes
func (m *MockObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullPrefix := m.fullKey(prefix)
	prefixSet := make(map[string]bool)

	for key := range m.objects {
		if strings.HasPrefix(key, fullPrefix) {
			remainder := strings.TrimPrefix(key, fullPrefix)
			if idx := strings.Index(remainder, delimiter); idx >= 0 {
				prefixSet[fullPrefix+remainder[:idx+1]] = true
			}
		}
	}

	var prefixes []string
	for p := range prefixSet {
		prefixes = append(prefixes, p)
	}
	return prefixes, nil
}

// ListObjects implements velero.ObjectStore.ListObjects
func (m *MockObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullPrefix := m.fullKey(prefix)
	var keys []string

	for key := range m.objects {
		if strings.HasPrefix(key, fullPrefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// CreateSignedURL implements velero.ObjectStore.CreateSignedURL
func (m *MockObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	fullKey := m.fullKey(key)
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s?signed=true", bucket, fullKey), nil
}

// Convenience methods matching S3ObjectStore

// PutObjectWithBucket uploads using the configured bucket.
func (m *MockObjectStore) PutObjectWithBucket(key string, body io.Reader) error {
	return m.PutObject(m.bucket, key, body)
}

// GetObjectWithBucket retrieves using the configured bucket.
func (m *MockObjectStore) GetObjectWithBucket(key string) (io.ReadCloser, error) {
	return m.GetObject(m.bucket, key)
}

// PutObjectBytes uploads bytes using the configured bucket.
func (m *MockObjectStore) PutObjectBytes(key string, data []byte) error {
	return m.PutObject(m.bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes.
func (m *MockObjectStore) GetObjectBytes(key string) ([]byte, error) {
	reader, err := m.GetObject(m.bucket, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// GetAllObjects returns all stored objects (for test verification).
func (m *MockObjectStore) GetAllObjects() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]byte)
	for k, v := range m.objects {
		result[k] = v
	}
	return result
}
