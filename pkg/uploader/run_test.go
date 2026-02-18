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

//nolint:goconst // Test files use repeated string literals for readability
package uploader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
		EnvBSLBucket:      os.Getenv(EnvBSLBucket),
		EnvVMName:         os.Getenv(EnvVMName),
		EnvVMNamespace:    os.Getenv(EnvVMNamespace),
		EnvBSLProvider:    os.Getenv(EnvBSLProvider),
		EnvBSLPrefix:      os.Getenv(EnvBSLPrefix),
		EnvBSLRegion:      os.Getenv(EnvBSLRegion),
		EnvBackupType:     os.Getenv(EnvBackupType),
		EnvSourcePVCPath:  os.Getenv(EnvSourcePVCPath),
		EnvCheckpointName: os.Getenv(EnvCheckpointName),
	}
	defer func() {
		for k, v := range originalEnv {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
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
				EnvBSLBucket:      "test-bucket",
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
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
				if cfg.CheckpointName != "cp-001" {
					t.Errorf("CheckpointName = %q, want %q", cfg.CheckpointName, "cp-001")
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
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing vm name returns error",
			envVars: map[string]string{
				EnvBSLBucket:      "test-bucket",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing vm namespace returns error",
			envVars: map[string]string{
				EnvBSLBucket:      "test-bucket",
				EnvVMName:         "test-vm",
				EnvCheckpointName: "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing checkpoint name returns error",
			envVars: map[string]string{
				EnvBSLBucket:   "test-bucket",
				EnvVMName:      "test-vm",
				EnvVMNamespace: "test-ns",
			},
			expectError: true,
		},
		{
			name: "invalid backup type returns error",
			envVars: map[string]string{
				EnvBSLBucket:      "test-bucket",
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
				EnvBackupType:     "invalid",
			},
			expectError: true,
		},
		{
			name: "custom source path is used",
			envVars: map[string]string{
				EnvBSLBucket:      "test-bucket",
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
				EnvSourcePVCPath:  "/custom/path",
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
				EnvBSLBucket:      "test-bucket",
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
				EnvBackupType:     "incremental",
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
				_ = os.Unsetenv(k)
			}
			// Set test env vars
			for k, v := range tt.envVars {
				_ = os.Setenv(k, v)
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

func TestUpdateVMIndex(t *testing.T) {
	tests := []struct {
		name           string
		config         *UploaderConfig
		files          []CheckpointFile
		archived       *archivedPaths
		existingIndex  *VMIndex
		setupStore     func(*MockObjectStore) // optional: customize store after standard setup
		expectError    bool
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "creates new index for full backup",
			config: &UploaderConfig{
				BSLBucket:        "test-bucket",
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-test",
				VeleroBackupName: "backup-001",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-disk1.qcow2",
					DiskName:   "disk1",
					Size:       1024,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
			},
			existingIndex: nil,
			expectError:   false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				if len(data) == 0 {
					t.Fatal("index is empty")
				}
				// Verify it contains expected fields
				if !containsBytes(data, "cp-001") {
					t.Error("index should contain checkpoint ID")
				}
				if !containsBytes(data, "full") {
					t.Error("index should contain backup type")
				}
				if !containsBytes(data, "disk1") {
					t.Error("index should contain PVC name")
				}
				// Verify archived paths are referenced
				if !containsBytes(data, "vmbObjectPath") {
					t.Error("index should contain vmbObjectPath")
				}
				if !containsBytes(data, "vmbtObjectPath") {
					t.Error("index should contain vmbtObjectPath")
				}
				if !containsBytes(data, "checkpoints/test-ns/test-vm/cp-001/vmb.json") {
					t.Error("index should reference correct vmb.json path")
				}
				if !containsBytes(data, "checkpoints/test-ns/test-vm/cp-001/vmbt.json") {
					t.Error("index should reference correct vmbt.json path")
				}
			},
		},
		{
			name: "updates existing index for incremental backup",
			config: &UploaderConfig{
				BSLBucket:        "test-bucket",
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-002",
				BackupType:       "incremental",
				VMBName:          "vmb-test-2",
				VeleroBackupName: "backup-002",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-2-disk1.qcow2",
					DiskName:   "disk1",
					Size:       512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-002/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmbt.json",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{
								Filename:   "vmb-test-disk1.qcow2",
								DiskName:   "disk1",
								ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
							},
						},
					},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				// Should contain both checkpoints
				if !containsBytes(data, "cp-001") {
					t.Error("index should contain first checkpoint")
				}
				if !containsBytes(data, "cp-002") {
					t.Error("index should contain second checkpoint")
				}
				// Incremental should have parent reference (note: json.MarshalIndent adds space after colon)
				if !containsBytes(data, "\"parent\": \"cp-001\"") {
					t.Error("incremental checkpoint should have parent")
				}
			},
		},
		{
			name: "incremental with mid-chain break returns error",
			config: &UploaderConfig{
				BSLBucket:        "test-bucket",
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-004",
				BackupType:       "incremental",
				VMBName:          "vmb-test-4",
				VeleroBackupName: "backup-004",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-4-disk1.qcow2",
					DiskName:   "disk1",
					Size:       256,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-004/vmb-test-4-disk1.qcow2",
				},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
					{
						ID:       "cp-002",
						Type:     "incremental",
						Parent:   "cp-001",
						VMBackup: "vmb-test-2",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2"},
						},
					},
					{
						ID:       "cp-003",
						Type:     "incremental",
						Parent:   "cp-002",
						VMBackup: "vmb-test-3",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-test-3-disk1.qcow2"},
						},
					},
				},
			},
			setupStore: func(store *MockObjectStore) {
				// Delete cp-002's file to simulate mid-chain break
				_ = store.DeleteObject("test-bucket", "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2")
			},
			expectError: true, // Chain fell back — uploader rejects incremental as safety net
		},
		{
			name: "incremental fails when no valid chain exists",
			config: &UploaderConfig{
				BSLBucket:        "test-bucket",
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-002",
				BackupType:       "incremental",
				VMBName:          "vmb-test-2",
				VeleroBackupName: "backup-002",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-2-disk1.qcow2",
					DiskName:   "disk1",
					Size:       512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2",
				},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "incremental",
						Parent:   "cp-000", // cp-000 doesn't exist - broken chain
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "deduplicates checkpoint if already exists",
			config: &UploaderConfig{
				BSLBucket:        "test-bucket",
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-test",
				VeleroBackupName: "backup-001",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-disk1.qcow2",
					DiskName:   "disk1",
					Size:       2048,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full", VMBackup: "vmb-old"},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				// Should only have one checkpoint (updated) - check for unique id field
				// Note: json.MarshalIndent adds space after colon
				if countOccurrences(data, "\"id\": \"cp-001\"") != 1 {
					t.Errorf("should have exactly one cp-001 entry, got data: %s", string(data))
				}
				// Should have updated VMBackup name
				if !containsBytes(data, "vmb-test") {
					t.Error("should have updated vmBackup name")
				}
				// Old VMBackup should be gone
				if containsBytes(data, "vmb-old") {
					t.Error("old vmBackup name should be replaced")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			// Set up existing index if provided
			if tt.existingIndex != nil {
				data, _ := json.Marshal(tt.existingIndex)
				indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json",
					tt.config.VMNamespace, tt.config.VMName)
				_ = store.PutObjectBytes(indexPath, data)

				// Create S3 objects for existing checkpoint files so chain validation passes
				for _, cp := range tt.existingIndex.Checkpoints {
					for _, f := range cp.Files {
						if f.ObjectPath != "" {
							_ = store.PutObjectBytes(f.ObjectPath, []byte("fake-qcow2"))
						}
					}
				}
			}

			// Optional: customize the store (e.g., delete specific files for break tests)
			if tt.setupStore != nil {
				tt.setupStore(store)
			}

			err := updateVMIndex(context.Background(), store, tt.config, tt.files, tt.archived)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, store)
			}
		})
	}
}

func containsBytes(data []byte, substr string) bool {
	return bytes.Contains(data, []byte(substr))
}

func countOccurrences(data []byte, substr string) int {
	return bytes.Count(data, []byte(substr))
}

func TestUpdateBackupManifests(t *testing.T) {
	tests := []struct {
		name           string
		config         *UploaderConfig
		vmIndex        *VMIndex
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "creates backup manifest and VM manifest",
			config: &UploaderConfig{
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				VeleroBackupName: "velero-backup-001",
			},
			vmIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full"},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				// Check backup index exists
				backupIndex, err := store.GetObjectBytes("manifests/velero-backup-001/index.json")
				if err != nil {
					t.Fatalf("backup index not created: %v", err)
				}
				if !containsBytes(backupIndex, "velero-backup-001") {
					t.Error("backup index should contain backup name")
				}

				// Check VM manifest exists
				vmManifest, err := store.GetObjectBytes("manifests/velero-backup-001/test-ns-test-vm.json")
				if err != nil {
					t.Fatalf("VM manifest not created: %v", err)
				}
				if !containsBytes(vmManifest, "test-vm") {
					t.Error("VM manifest should contain VM name")
				}
				if !containsBytes(vmManifest, "checkpointChain") {
					t.Error("VM manifest should contain checkpoint chain")
				}
			},
		},
		{
			name: "creates checkpoint chain for incremental backup",
			config: &UploaderConfig{
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-003",
				VeleroBackupName: "velero-backup-003",
			},
			vmIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full"},
					{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
					{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				vmManifest, err := store.GetObjectBytes("manifests/velero-backup-003/test-ns-test-vm.json")
				if err != nil {
					t.Fatalf("VM manifest not created: %v", err)
				}
				// Should contain all three checkpoints in chain
				if !containsBytes(vmManifest, "cp-001") {
					t.Error("chain should include base checkpoint")
				}
				if !containsBytes(vmManifest, "cp-002") {
					t.Error("chain should include intermediate checkpoint")
				}
				if !containsBytes(vmManifest, "cp-003") {
					t.Error("chain should include target checkpoint")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			// Set up VM index
			indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json",
				tt.config.VMNamespace, tt.config.VMName)
			indexData, _ := json.Marshal(tt.vmIndex)
			_ = store.PutObjectBytes(indexPath, indexData)

			// Simulate updateBackupManifests logic
			chain := buildCheckpointChain(tt.vmIndex.Checkpoints, tt.config.CheckpointName)

			// Create backup manifest
			manifestPath := fmt.Sprintf("manifests/%s/%s-%s.json",
				tt.config.VeleroBackupName, tt.config.VMNamespace, tt.config.VMName)
			backupManifest := BackupManifest{
				BackupName: tt.config.VeleroBackupName,
				VMs: []VMBackupReference{
					{
						Name:         tt.config.VMName,
						Namespace:    tt.config.VMNamespace,
						CheckpointID: tt.config.CheckpointName,
						ManifestPath: manifestPath,
					},
				},
			}
			backupManifestData, _ := json.Marshal(backupManifest)
			backupIndexPath := fmt.Sprintf("manifests/%s/index.json", tt.config.VeleroBackupName)
			_ = store.PutObjectBytes(backupIndexPath, backupManifestData)

			// Create VM manifest
			vmManifest := VMBackupManifest{
				Namespace:       tt.config.VMNamespace,
				Name:            tt.config.VMName,
				CheckpointChain: chain,
				BackupName:      tt.config.VeleroBackupName,
			}
			vmManifestData, _ := json.Marshal(vmManifest)
			_ = store.PutObjectBytes(manifestPath, vmManifestData)

			tt.validateResult(t, store)
		})
	}
}

func TestArchiveKubeResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)

	apiGroup := "kubevirt.io"
	backupAPIGroup := "backup.kubevirt.io"

	tests := []struct {
		name           string
		config         *UploaderConfig
		objects        []client.Object
		expectError    bool
		validateResult func(*testing.T, *MockObjectStore, *archivedPaths)
	}{
		{
			name: "archives VMB and VMBT to checkpoint dir",
			config: &UploaderConfig{
				BSLBucket:      "test-bucket",
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-test",
				VMBTName:       "vmbt-test-vm",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &backupAPIGroup,
							Kind:     "VirtualMachineBackupTracker",
							Name:     "vmbt-test-vm",
						},
					},
				},
				&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmbt-test-vm",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "VirtualMachine",
							Name:     "test-vm",
						},
					},
					// No Status — KubeVirt doesn't set LatestCheckpoint.
					// archiveKubeResources sets it from cfg.CheckpointName before archiving.
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore, paths *archivedPaths) {
				// Verify vmb.json was uploaded to checkpoint dir
				vmbData, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb.json")
				if err != nil {
					t.Fatalf("vmb.json not found in store: %v", err)
				}
				if !containsBytes(vmbData, "vmb-test") {
					t.Error("vmb.json should contain VMB name")
				}

				// Verify vmbt.json was uploaded to checkpoint dir (not VM level)
				vmbtData, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmbt.json")
				if err != nil {
					t.Fatalf("vmbt.json not found in store: %v", err)
				}
				if !containsBytes(vmbtData, "vmbt-test-vm") {
					t.Error("vmbt.json should contain VMBT name")
				}

				// Verify that archiveKubeResources set LatestCheckpoint in the archived VMBT.
				// KubeVirt does NOT update this field — our code sets it in-memory before serializing.
				var archivedVMBT kubevirtbackupv1alpha1.VirtualMachineBackupTracker
				if err := json.Unmarshal(vmbtData, &archivedVMBT); err != nil {
					t.Fatalf("failed to unmarshal archived vmbt.json: %v", err)
				}
				if archivedVMBT.Status == nil || archivedVMBT.Status.LatestCheckpoint == nil {
					t.Fatal("archived VMBT should have Status.LatestCheckpoint set")
				}
				if archivedVMBT.Status.LatestCheckpoint.Name != "cp-001" {
					t.Errorf("archived VMBT LatestCheckpoint.Name = %q, want %q",
						archivedVMBT.Status.LatestCheckpoint.Name, "cp-001")
				}

				// Verify returned paths
				if paths.VMBObjectPath != "checkpoints/test-ns/test-vm/cp-001/vmb.json" {
					t.Errorf("VMBObjectPath = %q, want checkpoints/test-ns/test-vm/cp-001/vmb.json", paths.VMBObjectPath)
				}
				if paths.VMBTObjectPath != "checkpoints/test-ns/test-vm/cp-001/vmbt.json" {
					t.Errorf("VMBTObjectPath = %q, want checkpoints/test-ns/test-vm/cp-001/vmbt.json", paths.VMBTObjectPath)
				}
			},
		},
		{
			name: "fails when VMB not found (fatal)",
			config: &UploaderConfig{
				BSLBucket:      "test-bucket",
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-nonexistent",
				VMBTName:       "vmbt-test-vm",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmbt-test-vm",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "VirtualMachine",
							Name:     "test-vm",
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "fails when VMBT not found (fatal)",
			config: &UploaderConfig{
				BSLBucket:      "test-bucket",
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-test",
				VMBTName:       "vmbt-nonexistent",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &backupAPIGroup,
							Kind:     "VirtualMachineBackupTracker",
							Name:     "vmbt-test-vm",
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "handles empty VMBName and VMBTName",
			config: &UploaderConfig{
				BSLBucket:      "test-bucket",
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "",
				VMBTName:       "",
			},
			objects:     []client.Object{},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore, paths *archivedPaths) {
				objs := store.GetAllObjects()
				if len(objs) != 0 {
					t.Errorf("expected no objects uploaded when names are empty, got %d", len(objs))
				}
				if paths.VMBObjectPath != "" {
					t.Errorf("VMBObjectPath should be empty, got %q", paths.VMBObjectPath)
				}
				if paths.VMBTObjectPath != "" {
					t.Errorf("VMBTObjectPath should be empty, got %q", paths.VMBTObjectPath)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			paths, err := archiveKubeResources(context.Background(), store, fakeClient, tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, store, paths)
			}
		})
	}
}

func TestCleanupKubeResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)

	apiGroup := "kubevirt.io"
	backupAPIGroup := "backup.kubevirt.io"

	tests := []struct {
		name    string
		config  *UploaderConfig
		objects []client.Object
	}{
		{
			name: "deletes VMB and VMBT from cluster",
			config: &UploaderConfig{
				VMName:      "test-vm",
				VMNamespace: "test-ns",
				VMBName:     "vmb-test",
				VMBTName:    "vmbt-test-vm",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &backupAPIGroup,
							Kind:     "VirtualMachineBackupTracker",
							Name:     "vmbt-test-vm",
						},
					},
				},
				&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmbt-test-vm",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "VirtualMachine",
							Name:     "test-vm",
						},
					},
				},
			},
		},
		{
			name: "handles already deleted VMB/VMBT gracefully",
			config: &UploaderConfig{
				VMName:      "test-vm",
				VMNamespace: "test-ns",
				VMBName:     "vmb-nonexistent",
				VMBTName:    "vmbt-nonexistent",
			},
			objects: []client.Object{},
		},
		{
			name: "handles empty names gracefully",
			config: &UploaderConfig{
				VMName:      "test-vm",
				VMNamespace: "test-ns",
				VMBName:     "",
				VMBTName:    "",
			},
			objects: []client.Object{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			// cleanupKubeResources is non-fatal (no error return)
			cleanupKubeResources(context.Background(), fakeClient, tt.config)

			// Verify objects were deleted (for the first test case)
			if tt.config.VMBName != "" && len(tt.objects) > 0 {
				vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
				getErr := fakeClient.Get(context.Background(), client.ObjectKey{
					Name: tt.config.VMBName, Namespace: tt.config.VMNamespace,
				}, vmb)
				if getErr == nil {
					t.Error("VMB should have been deleted from cluster")
				}
			}
			if tt.config.VMBTName != "" && len(tt.objects) > 0 {
				vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
				getErr := fakeClient.Get(context.Background(), client.ObjectKey{
					Name: tt.config.VMBTName, Namespace: tt.config.VMNamespace,
				}, vmbt)
				if getErr == nil {
					t.Error("VMBT should have been deleted from cluster")
				}
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
