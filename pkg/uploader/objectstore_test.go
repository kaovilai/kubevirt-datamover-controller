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
	"os"
	"path/filepath"
	"testing"
)

func TestS3ObjectStoreFullKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		key      string
		expected string
	}{
		{
			name:     "no prefix",
			prefix:   "",
			key:      "some/path/file.json",
			expected: "some/path/file.json",
		},
		{
			name:     "with prefix no trailing slash",
			prefix:   "backups",
			key:      "checkpoints/ns/vm/index.json",
			expected: "backups/checkpoints/ns/vm/index.json",
		},
		{
			name:     "with prefix with trailing slash",
			prefix:   "backups/",
			key:      "checkpoints/ns/vm/index.json",
			expected: "backups/checkpoints/ns/vm/index.json",
		},
		{
			name:     "key with leading slash",
			prefix:   "backups",
			key:      "/checkpoints/ns/vm/index.json",
			expected: "backups/checkpoints/ns/vm/index.json",
		},
		{
			name:     "both with slashes",
			prefix:   "backups/",
			key:      "/checkpoints/ns/vm/index.json",
			expected: "backups/checkpoints/ns/vm/index.json",
		},
		{
			name:     "nested prefix",
			prefix:   "velero/backups",
			key:      "file.json",
			expected: "velero/backups/file.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{prefix: tt.prefix}
			result := store.fullKey(tt.key)
			if result != tt.expected {
				t.Errorf("fullKey(%q) = %q, want %q", tt.key, result, tt.expected)
			}
		})
	}
}

func TestParseAWSCredentials(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name: "valid credentials with section header",
			data: []byte("[default]\n" +
				"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"),
			expectError: false,
		},
		{
			name: "valid credentials without section header",
			data: []byte("aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"),
			expectError: false,
		},
		{
			name: "valid credentials with extra whitespace",
			data: []byte("\n" +
				"  aws_access_key_id   =   AKIAIOSFODNN7EXAMPLE\n" +
				"  aws_secret_access_key   =   wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n\n"),
			expectError: false,
		},
		{
			name:        "missing access key id",
			data:        []byte("aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
			expectError: true,
		},
		{
			name:        "missing secret access key",
			data:        []byte("aws_access_key_id = AKIAIOSFODNN7EXAMPLE"),
			expectError: true,
		},
		{
			name:        "empty data",
			data:        []byte(""),
			expectError: true,
		},
		{
			name:        "nil data",
			data:        nil,
			expectError: true,
		},
		{
			name:        "empty values",
			data:        []byte("aws_access_key_id =\naws_secret_access_key ="),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, err := ParseAWSCredentials(tt.data)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if creds == nil {
					t.Error("expected credentials provider but got nil")
				}
			}
		})
	}
}

func TestLoadAWSCredentialsFromFile(t *testing.T) {
	// Test that file-based loading still works (used by datamover pod)
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "credentials")
	content := "[default]\naws_access_key_id = AKID\naws_secret_access_key = SECRET\n"
	if err := os.WriteFile(tmpFile, []byte(content), 0600); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	creds, err := loadAWSCredentialsFromFile(tmpFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds == nil {
		t.Error("expected credentials provider but got nil")
	}
}

func TestLoadAWSCredentialsFromFileNotExists(t *testing.T) {
	_, err := loadAWSCredentialsFromFile("/nonexistent/path/credentials")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestS3ObjectStoreInit(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "missing bucket",
			config: map[string]string{
				"prefix": "backups",
				"region": "us-east-1",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
		{
			name: "empty bucket",
			config: map[string]string{
				"bucket": "",
				"prefix": "backups",
				"region": "us-east-1",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestInitObjectStore(t *testing.T) {
	tests := []struct {
		name        string
		provider    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "gcp not implemented",
			provider:    "gcp",
			expectError: true,
			errorMsg:    "GCP object store not yet implemented",
		},
		{
			name:        "azure not implemented",
			provider:    "azure",
			expectError: true,
			errorMsg:    "Azure object store not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &UploaderConfig{
				BSLProvider: tt.provider,
				BSLBucket:   "test-bucket",
			}

			_, err := InitObjectStore(config)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
