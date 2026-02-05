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
	"testing"
)

func TestExtractDiskName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected string
	}{
		{
			name:     "standard vmb filename",
			filename: "vmb-test-du-volume0.qcow2",
			expected: "volume0",
		},
		{
			name:     "filename with timestamp",
			filename: "vmb-test-2026-02-05_19-12-01-disk1.qcow2",
			expected: "disk1",
		},
		{
			name:     "simple filename",
			filename: "backup-rootdisk.qcow2",
			expected: "rootdisk",
		},
		{
			name:     "uppercase extension",
			filename: "vmb-test-volume0.QCOW2",
			expected: "volume0",
		},
		{
			name:     "no dash in filename",
			filename: "backup.qcow2",
			expected: "backup",
		},
		{
			name:     "single part",
			filename: "disk.qcow2",
			expected: "disk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractDiskName(tt.filename)
			if result != tt.expected {
				t.Errorf("extractDiskName(%q) = %q, want %q", tt.filename, result, tt.expected)
			}
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Save original env and restore after test
	originalEnv := map[string]string{
		EnvBSLBucket:    os.Getenv(EnvBSLBucket),
		EnvVMName:       os.Getenv(EnvVMName),
		EnvVMNamespace:  os.Getenv(EnvVMNamespace),
		EnvBSLProvider:  os.Getenv(EnvBSLProvider),
		EnvBSLPrefix:    os.Getenv(EnvBSLPrefix),
		EnvBSLRegion:    os.Getenv(EnvBSLRegion),
		EnvBackupType:   os.Getenv(EnvBackupType),
		EnvSourcePVCPath: os.Getenv(EnvSourcePVCPath),
	}
	defer func() {
		for k, v := range originalEnv {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	tests := []struct {
		name        string
		envVars     map[string]string
		expectError bool
		validate    func(*testing.T, *UploaderConfig)
	}{
		{
			name: "valid config with all required fields",
			envVars: map[string]string{
				EnvBSLBucket:   "test-bucket",
				EnvVMName:      "test-vm",
				EnvVMNamespace: "test-ns",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BSLBucket != "test-bucket" {
					t.Errorf("BSLBucket = %q, want %q", cfg.BSLBucket, "test-bucket")
				}
				if cfg.VMName != "test-vm" {
					t.Errorf("VMName = %q, want %q", cfg.VMName, "test-vm")
				}
				if cfg.VMNamespace != "test-ns" {
					t.Errorf("VMNamespace = %q, want %q", cfg.VMNamespace, "test-ns")
				}
				// Check defaults
				if cfg.SourcePVCPath != DefaultSourcePVCPath {
					t.Errorf("SourcePVCPath = %q, want %q", cfg.SourcePVCPath, DefaultSourcePVCPath)
				}
				if cfg.CredentialsFile != DefaultCredentialsPath {
					t.Errorf("CredentialsFile = %q, want %q", cfg.CredentialsFile, DefaultCredentialsPath)
				}
				if cfg.BackupType != "full" {
					t.Errorf("BackupType = %q, want %q", cfg.BackupType, "full")
				}
			},
		},
		{
			name: "missing bucket returns error",
			envVars: map[string]string{
				EnvVMName:      "test-vm",
				EnvVMNamespace: "test-ns",
			},
			expectError: true,
		},
		{
			name: "missing vm name returns error",
			envVars: map[string]string{
				EnvBSLBucket:   "test-bucket",
				EnvVMNamespace: "test-ns",
			},
			expectError: true,
		},
		{
			name: "missing vm namespace returns error",
			envVars: map[string]string{
				EnvBSLBucket: "test-bucket",
				EnvVMName:    "test-vm",
			},
			expectError: true,
		},
		{
			name: "custom source path is used",
			envVars: map[string]string{
				EnvBSLBucket:     "test-bucket",
				EnvVMName:        "test-vm",
				EnvVMNamespace:   "test-ns",
				EnvSourcePVCPath: "/custom/path",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.SourcePVCPath != "/custom/path" {
					t.Errorf("SourcePVCPath = %q, want %q", cfg.SourcePVCPath, "/custom/path")
				}
			},
		},
		{
			name: "backup type incremental is preserved",
			envVars: map[string]string{
				EnvBSLBucket:   "test-bucket",
				EnvVMName:      "test-vm",
				EnvVMNamespace: "test-ns",
				EnvBackupType:  "incremental",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BackupType != "incremental" {
					t.Errorf("BackupType = %q, want %q", cfg.BackupType, "incremental")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars first
			for k := range originalEnv {
				os.Unsetenv(k)
			}
			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg, err := LoadConfigFromEnv()

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestBuildCheckpointChain(t *testing.T) {
	tests := []struct {
		name        string
		checkpoints []CheckpointEntry
		targetID    string
		expectLen   int
		expectFirst string
		expectLast  string
	}{
		{
			name: "single checkpoint (full backup)",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
			},
			targetID:    "cp-001",
			expectLen:   1,
			expectFirst: "cp-001",
			expectLast:  "cp-001",
		},
		{
			name: "chain of three checkpoints",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-003",
			expectLen:   3,
			expectFirst: "cp-001",
			expectLast:  "cp-003",
		},
		{
			name: "target in middle of chain",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-002",
			expectLen:   2,
			expectFirst: "cp-001",
			expectLast:  "cp-002",
		},
		{
			name: "target not found",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
			},
			targetID:  "nonexistent",
			expectLen: 0,
		},
		{
			name:        "empty checkpoints",
			checkpoints: []CheckpointEntry{},
			targetID:    "cp-001",
			expectLen:   0,
		},
		{
			name: "broken chain (missing parent)",
			checkpoints: []CheckpointEntry{
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"}, // cp-001 doesn't exist
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-003",
			expectLen:   2, // Only cp-002 and cp-003, stops when cp-001 not found
			expectFirst: "cp-002",
			expectLast:  "cp-003",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := buildCheckpointChain(tt.checkpoints, tt.targetID)

			if len(chain) != tt.expectLen {
				t.Errorf("chain length = %d, want %d", len(chain), tt.expectLen)
			}

			if tt.expectLen > 0 {
				if chain[0].ID != tt.expectFirst {
					t.Errorf("first checkpoint = %q, want %q", chain[0].ID, tt.expectFirst)
				}
				if chain[len(chain)-1].ID != tt.expectLast {
					t.Errorf("last checkpoint = %q, want %q", chain[len(chain)-1].ID, tt.expectLast)
				}
			}
		})
	}
}
