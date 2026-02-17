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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLookupLatestCheckpoint_NoIndex(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false for missing index")
	}
	if result.LatestCheckpoint != "" {
		t.Errorf("expected empty checkpoint, got %q", result.LatestCheckpoint)
	}
	if !strings.Contains(result.Message, "no checkpoint index found") {
		t.Errorf("expected message about no index, got %q", result.Message)
	}
}

func TestLookupLatestCheckpoint_EmptyCheckpoints(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create an index with no checkpoints
	vmIndex := VMIndex{
		VMName:      "test-vm",
		Namespace:   "test-ns",
		Checkpoints: []CheckpointEntry{},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false for empty checkpoints")
	}
	if !strings.Contains(result.Message, "no checkpoints") {
		t.Errorf("expected message about no checkpoints, got %q", result.Message)
	}
}

func TestLookupLatestCheckpoint_CorruptedIndex(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create a corrupted index
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", []byte("not valid json"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false for corrupted index")
	}
	if !strings.Contains(result.Message, "falling back to full backup") {
		t.Errorf("expected fallback message, got %q", result.Message)
	}
}

func TestLookupLatestCheckpoint_SingleFullCheckpoint(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create a valid index with one full checkpoint
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						DiskName:   "disk0",
						Size:       1024,
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
				},
				Timestamp: time.Now(),
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Create the actual qcow2 file in the store
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("fake-qcow2"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Errorf("expected Found=true, got message: %s", result.Message)
	}
	if result.LatestCheckpoint != "cp-001" {
		t.Errorf("expected checkpoint cp-001, got %q", result.LatestCheckpoint)
	}
	if !result.IsChainValid {
		t.Error("expected valid chain")
	}
	if result.ChainLength != 1 {
		t.Errorf("expected chain length 1, got %d", result.ChainLength)
	}
}

func TestLookupLatestCheckpoint_IncrementalChain(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create a valid index with full + incremental checkpoints
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-002-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-003",
				Type:     "incremental",
				Parent:   "cp-002",
				VMBackup: "vmb-003",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-003-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-003-disk0.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Create all qcow2 files
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("full"))
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2", []byte("inc1"))
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-003/vmb-003-disk0.qcow2", []byte("inc2"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Errorf("expected Found=true, got message: %s", result.Message)
	}
	if result.LatestCheckpoint != "cp-003" {
		t.Errorf("expected checkpoint cp-003, got %q", result.LatestCheckpoint)
	}
	if result.ChainLength != 3 {
		t.Errorf("expected chain length 3, got %d", result.ChainLength)
	}
}

func TestLookupLatestCheckpoint_BrokenChain_LatestMissing(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create index with 3 checkpoints but latest has missing file
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-002-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-003",
				Type:     "incremental",
				Parent:   "cp-002",
				VMBackup: "vmb-003",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-003-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-003-disk0.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Only create files for cp-001 and cp-002 (cp-003 is missing)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("full"))
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2", []byte("inc1"))
	// cp-003 file NOT created - simulates missing file

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should fall back to cp-002 since cp-003 has missing files
	if !result.Found {
		t.Errorf("expected Found=true (fallback to cp-002), got message: %s", result.Message)
	}
	if result.LatestCheckpoint != "cp-002" {
		t.Errorf("expected fallback to checkpoint cp-002, got %q", result.LatestCheckpoint)
	}
	if result.ChainLength != 2 {
		t.Errorf("expected chain length 2, got %d", result.ChainLength)
	}
}

func TestLookupLatestCheckpoint_BrokenChain_FullMissing(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Create index where the full backup's files are missing
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-002-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Only create incremental file, NOT the full backup file
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2", []byte("inc1"))
	// cp-001 file NOT created

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should indicate no valid checkpoint found since full backup is missing
	if result.Found {
		t.Error("expected Found=false when full backup files are missing")
	}
}

func TestLookupLatestCheckpoint_MultipleDisks(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// VM with multiple disks
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
					{
						Filename:   "vmb-001-disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Create both disk files
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("disk0"))
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk1.qcow2", []byte("disk1"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Errorf("expected Found=true, got message: %s", result.Message)
	}
	if result.LatestCheckpoint != "cp-001" {
		t.Errorf("expected checkpoint cp-001, got %q", result.LatestCheckpoint)
	}
}

func TestLookupLatestCheckpoint_MultipleDisks_OneMissing(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// VM with multiple disks, one missing
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
					{
						Filename:   "vmb-001-disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Only create one disk file
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("disk0"))
	// disk1 NOT created

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false when one disk file is missing")
	}
}

func TestLookupLatestCheckpoint_ChainNotStartingWithFull(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	// Index where the chain starts with incremental (broken parent)
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001", // cp-001 doesn't exist
				VMBackup: "vmb-002",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-002-disk0.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmb-002-disk0.qcow2", []byte("inc"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false when chain doesn't start with full backup")
	}
}

func TestLookupLatestCheckpoint_CorruptedEmptyObjectPath(t *testing.T) {
	// End-to-end test: a checkpoint with an empty ObjectPath (corrupt index)
	// should cause the chain to be marked as broken, returning Found=false
	// with a message indicating the corruption reason.
	store := NewMockObjectStore("test-bucket", "")

	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						DiskName:   "root-disk",
						ObjectPath: "", // Corrupt: empty ObjectPath
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false when checkpoint has empty ObjectPath (corrupt index)")
	}

	// Verify the message contains the corruption reason for debugging
	if !strings.Contains(result.Message, "empty object path") {
		t.Errorf("expected message to contain 'empty object path' for debugging, got: %q",
			result.Message)
	}
}

func TestLookupLatestCheckpoint_CorruptedEmptyObjectPath_FallsBackToEarlier(t *testing.T) {
	// When the latest checkpoint has a corrupt file (empty ObjectPath),
	// the chain walker should fall back to the previous valid checkpoint.
	store := NewMockObjectStore("test-bucket", "")

	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-001-disk0.qcow2",
						DiskName:   "root-disk",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2",
					},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-002-disk0.qcow2",
						DiskName:   "root-disk",
						ObjectPath: "", // Corrupt: empty ObjectPath
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	// Only the full backup file exists (incremental is corrupt)
	_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb-001-disk0.qcow2", []byte("full"))

	result, err := LookupLatestCheckpoint(context.Background(), store, "test-bucket", "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should fall back to the full backup checkpoint (cp-001)
	if !result.Found {
		t.Fatalf("expected Found=true (fallback to cp-001), got Found=false: %s", result.Message)
	}
	if result.LatestCheckpoint != "cp-001" {
		t.Errorf("expected LatestCheckpoint=cp-001 (fallback), got %q", result.LatestCheckpoint)
	}
}

func TestValidateCheckpointFiles(t *testing.T) {
	tests := []struct {
		name        string
		files       []CheckpointFile
		storeFiles  map[string]string // path -> content
		expectError bool
	}{
		{
			name: "all files exist",
			files: []CheckpointFile{
				{Filename: "disk0.qcow2", ObjectPath: "path/to/disk0.qcow2"},
				{Filename: "disk1.qcow2", ObjectPath: "path/to/disk1.qcow2"},
			},
			storeFiles: map[string]string{
				"path/to/disk0.qcow2": "data0",
				"path/to/disk1.qcow2": "data1",
			},
			expectError: false,
		},
		{
			name: "one file missing",
			files: []CheckpointFile{
				{Filename: "disk0.qcow2", ObjectPath: "path/to/disk0.qcow2"},
				{Filename: "disk1.qcow2", ObjectPath: "path/to/disk1.qcow2"},
			},
			storeFiles: map[string]string{
				"path/to/disk0.qcow2": "data0",
				// disk1 missing
			},
			expectError: true,
		},
		{
			name:        "empty files list",
			files:       []CheckpointFile{},
			storeFiles:  map[string]string{},
			expectError: false,
		},
		{
			name: "file with empty object path returns error",
			files: []CheckpointFile{
				{Filename: "disk0.qcow2", DiskName: "root-disk", ObjectPath: ""},
				{Filename: "disk1.qcow2", ObjectPath: "path/to/disk1.qcow2"},
			},
			storeFiles: map[string]string{
				"path/to/disk1.qcow2": "data1",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")
			for path, content := range tt.storeFiles {
				_ = store.PutObjectBytes(path, []byte(content))
			}

			err := validateCheckpointFiles(context.Background(), store, "test-bucket", tt.files)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateCheckpointFiles_ParallelMultipleDisks(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")
	files := make([]CheckpointFile, 10)
	for i := range 10 {
		path := fmt.Sprintf("checkpoints/ns/vm/cp1/disk%d.qcow2", i)
		files[i] = CheckpointFile{
			Filename:   fmt.Sprintf("disk%d.qcow2", i),
			DiskName:   fmt.Sprintf("disk-%d", i),
			ObjectPath: path,
		}
		_ = store.PutObjectBytes(path, []byte("data"))
	}

	err := validateCheckpointFiles(context.Background(), store, "test-bucket", files)
	if err != nil {
		t.Errorf("unexpected error with 10 valid files: %v", err)
	}
}

func TestValidateCheckpointFiles_ParallelFirstErrorCancels(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")
	files := make([]CheckpointFile, 10)
	for i := range 10 {
		path := fmt.Sprintf("checkpoints/ns/vm/cp1/disk%d.qcow2", i)
		files[i] = CheckpointFile{
			Filename:   fmt.Sprintf("disk%d.qcow2", i),
			DiskName:   fmt.Sprintf("disk-%d", i),
			ObjectPath: path,
		}
		// Only add half the files to the store - the others will be "missing"
		if i%2 == 0 {
			_ = store.PutObjectBytes(path, []byte("data"))
		}
	}

	err := validateCheckpointFiles(context.Background(), store, "test-bucket", files)
	if err == nil {
		t.Error("expected error for missing files but got none")
	}
	if !strings.Contains(err.Error(), "not found in BSL") {
		t.Errorf("expected 'not found in BSL' error, got: %v", err)
	}
}

func TestValidateCheckpointChain(t *testing.T) {
	tests := []struct {
		name              string
		checkpoints       []CheckpointEntry
		targetID          string
		storeFiles        map[string]string
		expectFound       bool
		expectLatest      string
		expectChainLength int
		expectChainValid  bool
	}{
		{
			name: "valid single full checkpoint",
			checkpoints: []CheckpointEntry{
				{
					ID:   "cp-001",
					Type: "full",
					Files: []CheckpointFile{
						{ObjectPath: "checkpoints/ns/vm/cp-001/disk.qcow2"},
					},
				},
			},
			targetID: "cp-001",
			storeFiles: map[string]string{
				"checkpoints/ns/vm/cp-001/disk.qcow2": "data",
			},
			expectFound:       true,
			expectLatest:      "cp-001",
			expectChainLength: 1,
			expectChainValid:  true,
		},
		{
			name: "valid full + incremental chain",
			checkpoints: []CheckpointEntry{
				{
					ID:   "cp-001",
					Type: "full",
					Files: []CheckpointFile{
						{ObjectPath: "checkpoints/ns/vm/cp-001/disk.qcow2"},
					},
				},
				{
					ID:     "cp-002",
					Type:   "incremental",
					Parent: "cp-001",
					Files: []CheckpointFile{
						{ObjectPath: "checkpoints/ns/vm/cp-002/disk.qcow2"},
					},
				},
			},
			targetID: "cp-002",
			storeFiles: map[string]string{
				"checkpoints/ns/vm/cp-001/disk.qcow2": "full",
				"checkpoints/ns/vm/cp-002/disk.qcow2": "inc",
			},
			expectFound:       true,
			expectLatest:      "cp-002",
			expectChainLength: 2,
			expectChainValid:  true,
		},
		{
			name:        "target not found in checkpoints",
			checkpoints: []CheckpointEntry{},
			targetID:    "nonexistent",
			storeFiles:  map[string]string{},
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")
			for path, content := range tt.storeFiles {
				_ = store.PutObjectBytes(path, []byte(content))
			}

			result, err := validateCheckpointChain(context.Background(), store, "test-bucket", tt.checkpoints, tt.targetID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Found != tt.expectFound {
				t.Errorf("Found = %v, want %v (message: %s)", result.Found, tt.expectFound, result.Message)
			}
			if tt.expectFound {
				if result.LatestCheckpoint != tt.expectLatest {
					t.Errorf("LatestCheckpoint = %q, want %q", result.LatestCheckpoint, tt.expectLatest)
				}
				if result.ChainLength != tt.expectChainLength {
					t.Errorf("ChainLength = %d, want %d", result.ChainLength, tt.expectChainLength)
				}
				if result.IsChainValid != tt.expectChainValid {
					t.Errorf("IsChainValid = %v, want %v", result.IsChainValid, tt.expectChainValid)
				}
			}
		})
	}
}

func TestLoadVMIndex(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
		validate    func(*testing.T, *VMIndex)
	}{
		{
			name: "valid index",
			data: func() []byte {
				idx := VMIndex{
					VMName:    "test-vm",
					Namespace: "test-ns",
					Checkpoints: []CheckpointEntry{
						{ID: "cp-001", Type: "full"},
					},
				}
				d, _ := json.Marshal(idx)
				return d
			}(),
			expectError: false,
			validate: func(t *testing.T, idx *VMIndex) {
				if idx.VMName != "test-vm" {
					t.Errorf("VMName = %q, want %q", idx.VMName, "test-vm")
				}
				if len(idx.Checkpoints) != 1 {
					t.Errorf("got %d checkpoints, want 1", len(idx.Checkpoints))
				}
			},
		},
		{
			name:        "invalid json",
			data:        []byte("{invalid"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")
			_ = store.PutObjectBytes("index.json", tt.data)

			idx, err := loadVMIndex(store, "test-bucket", "index.json")

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, idx)
			}
		})
	}
}

func TestLoadVMIndex_NotFound(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	_, err := loadVMIndex(store, "test-bucket", "nonexistent/index.json")
	if err == nil {
		t.Error("expected error for missing index")
	}
}

func TestLookupLatestCheckpoint_CancelledContext(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := LookupLatestCheckpoint(ctx, store, "test-bucket", "test-ns", "test-vm")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	if !strings.Contains(err.Error(), "context cancelled") {
		t.Errorf("expected context cancellation error, got: %v", err)
	}
}

func TestValidateCheckpointChain_CancelledContext(t *testing.T) {
	store := NewMockObjectStore("test-bucket", "")

	checkpoints := []CheckpointEntry{
		{
			ID:   "cp-001",
			Type: "full",
			Files: []CheckpointFile{
				{ObjectPath: "checkpoints/ns/vm/cp-001/disk.qcow2"},
			},
		},
	}
	_ = store.PutObjectBytes("checkpoints/ns/vm/cp-001/disk.qcow2", []byte("data"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := validateCheckpointChain(ctx, store, "test-bucket", checkpoints, "cp-001")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	if !strings.Contains(err.Error(), "context cancelled") {
		t.Errorf("expected context cancellation error, got: %v", err)
	}
}
