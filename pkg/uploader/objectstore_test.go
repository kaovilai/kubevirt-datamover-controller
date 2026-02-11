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

func TestLoadAWSCredentialsFromFile(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid credentials",
			content: `[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
`,
			expectError: false,
		},
		{
			name: "valid credentials without section header",
			content: `aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
`,
			expectError: false,
		},
		{
			name: "valid credentials with extra whitespace",
			content: `
  aws_access_key_id   =   AKIAIOSFODNN7EXAMPLE
  aws_secret_access_key   =   wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

`,
			expectError: false,
		},
		{
			name:        "missing access key id",
			content:     `aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`,
			expectError: true,
			errorMsg:    "missing aws_access_key_id or aws_secret_access_key",
		},
		{
			name:        "missing secret access key",
			content:     `aws_access_key_id = AKIAIOSFODNN7EXAMPLE`,
			expectError: true,
			errorMsg:    "missing aws_access_key_id or aws_secret_access_key",
		},
		{
			name:        "empty file",
			content:     "",
			expectError: true,
			errorMsg:    "missing aws_access_key_id or aws_secret_access_key",
		},
		{
			name:        "empty values",
			content:     "aws_access_key_id =\naws_secret_access_key =",
			expectError: true,
			errorMsg:    "missing aws_access_key_id or aws_secret_access_key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "credentials")
			if err := os.WriteFile(tmpFile, []byte(tt.content), 0600); err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}

			creds, err := loadAWSCredentialsFromFile(tmpFile)

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
