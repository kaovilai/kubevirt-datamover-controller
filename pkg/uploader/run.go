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
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Run is the main entrypoint for the uploader.
func Run(ctx context.Context) error {
	fmt.Println("Starting kubevirt datamover uploader...")

	// Load configuration from environment
	config, err := LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	fmt.Printf("Config loaded: VM=%s/%s, checkpoint=%s, type=%s\n",
		config.VMNamespace, config.VMName, config.CheckpointName, config.BackupType)

	// Initialize object store
	storeInterface, err := InitObjectStore(config)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}

	// Type assert to get the concrete type with convenience methods
	store, ok := storeInterface.(*S3ObjectStore)
	if !ok {
		return fmt.Errorf("expected S3ObjectStore, got %T", storeInterface)
	}

	fmt.Printf("Object store initialized: bucket=%s, prefix=%s\n", config.BSLBucket, config.BSLPrefix)

	// Upload qcow2 files
	files, err := uploadQcow2Files(ctx, store, config)
	if err != nil {
		return fmt.Errorf("failed to upload qcow2 files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no qcow2 files found in %s", config.SourcePVCPath)
	}

	fmt.Printf("Uploaded %d qcow2 files\n", len(files))

	// Update VM index
	if err := updateVMIndex(ctx, store, config, files); err != nil {
		return fmt.Errorf("failed to update VM index: %w", err)
	}

	fmt.Println("VM index updated")

	// Update backup manifests
	if err := updateBackupManifests(ctx, store, config); err != nil {
		return fmt.Errorf("failed to update backup manifests: %w", err)
	}

	fmt.Println("Backup manifests updated")
	fmt.Println("Upload completed successfully")

	return nil
}

// LoadConfigFromEnv parses environment variables into UploaderConfig.
func LoadConfigFromEnv() (*UploaderConfig, error) {
	config := &UploaderConfig{
		BSLProvider:      os.Getenv(EnvBSLProvider),
		BSLBucket:        os.Getenv(EnvBSLBucket),
		BSLPrefix:        os.Getenv(EnvBSLPrefix),
		BSLRegion:        os.Getenv(EnvBSLRegion),
		CredentialsFile:  os.Getenv(EnvCredentialsFile),
		VMName:           os.Getenv(EnvVMName),
		VMNamespace:      os.Getenv(EnvVMNamespace),
		CheckpointName:   os.Getenv(EnvCheckpointName),
		BackupType:       os.Getenv(EnvBackupType),
		VeleroBackupName: os.Getenv(EnvVeleroBackupName),
		DataUploadName:   os.Getenv(EnvDataUploadName),
		DataUploadUID:    os.Getenv(EnvDataUploadUID),
		VMBName:          os.Getenv(EnvVMBName),
		SourcePVCPath:    os.Getenv(EnvSourcePVCPath),
	}

	// Apply defaults
	if config.SourcePVCPath == "" {
		config.SourcePVCPath = DefaultSourcePVCPath
	}
	if config.CredentialsFile == "" {
		config.CredentialsFile = DefaultCredentialsPath
	}
	if config.BackupType == "" {
		config.BackupType = "full"
	}

	// Validate required fields
	if config.BSLBucket == "" {
		return nil, fmt.Errorf("%s is required", EnvBSLBucket)
	}
	if config.VMName == "" {
		return nil, fmt.Errorf("%s is required", EnvVMName)
	}
	if config.VMNamespace == "" {
		return nil, fmt.Errorf("%s is required", EnvVMNamespace)
	}

	return config, nil
}

// uploadQcow2Files walks the source path and uploads all qcow2 files.
func uploadQcow2Files(_ context.Context, store *S3ObjectStore, config *UploaderConfig) ([]CheckpointFile, error) {
	var files []CheckpointFile

	err := filepath.WalkDir(config.SourcePVCPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Only process qcow2 files
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".qcow2") {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", path, err)
		}

		// Build object path: checkpoints/<ns>/<vm>/<checkpoint>/<filename>
		objectPath := fmt.Sprintf("checkpoints/%s/%s/%s/%s",
			config.VMNamespace, config.VMName, config.CheckpointName, d.Name())

		// Open file for upload
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer func() { _ = file.Close() }()

		fmt.Printf("Uploading %s (%d bytes) to %s\n", d.Name(), info.Size(), objectPath)

		// Upload file using convenience method
		if err := store.PutObjectWithBucket(objectPath, file); err != nil {
			return fmt.Errorf("failed to upload %s: %w", path, err)
		}

		// Extract disk name from filename (e.g., "vmb-xxx-disk1.qcow2" -> "disk1")
		diskName := extractDiskName(d.Name())

		files = append(files, CheckpointFile{
			Filename:   d.Name(),
			DiskName:   diskName,
			Size:       info.Size(),
			ObjectPath: objectPath,
		})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk source path: %w", err)
	}

	return files, nil
}

// extractDiskName extracts the disk name from a qcow2 filename.
// E.g., "vmb-xxx-disk1.qcow2" -> "disk1"
func extractDiskName(filename string) string {
	// Remove .qcow2 extension
	name := strings.TrimSuffix(filename, ".qcow2")
	name = strings.TrimSuffix(name, ".QCOW2")

	// Find the last dash and extract everything after it
	parts := strings.Split(name, "-")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return name
}

// updateVMIndex creates or updates the per-VM index.json file.
func updateVMIndex(_ context.Context, store *S3ObjectStore, config *UploaderConfig, files []CheckpointFile) error {
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", config.VMNamespace, config.VMName)

	// Try to load existing index
	var vmIndex VMIndex
	data, err := store.GetObjectBytes(indexPath)
	if err == nil {
		if err := json.Unmarshal(data, &vmIndex); err != nil {
			fmt.Printf("Warning: failed to parse existing index, creating new: %v\n", err)
			vmIndex = VMIndex{}
		}
	} else {
		// Index doesn't exist, create new
		vmIndex = VMIndex{
			VMName:      config.VMName,
			Namespace:   config.VMNamespace,
			Checkpoints: []CheckpointEntry{},
		}
	}

	// Create new checkpoint entry
	// Extract PVC/disk names from uploaded files
	var pvcNames []string
	for _, f := range files {
		if f.DiskName != "" {
			pvcNames = append(pvcNames, f.DiskName)
		}
	}

	// ReferencedBy tracks which Velero backups use this checkpoint
	var referencedBy []string
	if config.VeleroBackupName != "" {
		referencedBy = append(referencedBy, config.VeleroBackupName)
	}

	checkpoint := CheckpointEntry{
		ID:           config.CheckpointName,
		Type:         config.BackupType,
		Timestamp:    time.Now().UTC(),
		VMBackup:     config.VMBName,
		Files:        files,
		PVCs:         pvcNames,
		ReferencedBy: referencedBy,
	}

	// For incremental backups, set parent checkpoint
	if strings.ToLower(config.BackupType) == BackupTypeIncremental && len(vmIndex.Checkpoints) > 0 {
		// Use the most recent checkpoint as parent
		checkpoint.Parent = vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1].ID
	}

	// Append new checkpoint (avoid duplicates)
	found := false
	for i, cp := range vmIndex.Checkpoints {
		if cp.ID == checkpoint.ID {
			// Update existing entry
			vmIndex.Checkpoints[i] = checkpoint
			found = true
			break
		}
	}
	if !found {
		vmIndex.Checkpoints = append(vmIndex.Checkpoints, checkpoint)
	}

	vmIndex.LastUpdated = time.Now().UTC()

	// Write updated index
	indexData, err := json.MarshalIndent(vmIndex, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM index: %w", err)
	}

	if err := store.PutObjectBytes(indexPath, indexData); err != nil {
		return fmt.Errorf("failed to write VM index: %w", err)
	}

	return nil
}

// updateBackupManifests creates/updates the per-backup manifest files.
func updateBackupManifests(_ context.Context, store *S3ObjectStore, config *UploaderConfig) error {
	if config.VeleroBackupName == "" {
		fmt.Println("Warning: no Velero backup name provided, skipping manifest update")
		return nil
	}

	// Load the VM index to get the checkpoint chain
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", config.VMNamespace, config.VMName)
	data, err := store.GetObjectBytes(indexPath)
	if err != nil {
		return fmt.Errorf("failed to read VM index: %w", err)
	}

	var vmIndex VMIndex
	if err := json.Unmarshal(data, &vmIndex); err != nil {
		return fmt.Errorf("failed to parse VM index: %w", err)
	}

	// Build checkpoint chain (for restore)
	chain := buildCheckpointChain(vmIndex.Checkpoints, config.CheckpointName)

	// Create/update per-backup index.json
	backupIndexPath := fmt.Sprintf("manifests/%s/index.json", config.VeleroBackupName)

	var backupManifest BackupManifest
	data, err = store.GetObjectBytes(backupIndexPath)
	if err == nil {
		if err := json.Unmarshal(data, &backupManifest); err != nil {
			fmt.Printf("Warning: failed to parse existing backup manifest, creating new: %v\n", err)
			backupManifest = BackupManifest{}
		}
	} else {
		backupManifest = BackupManifest{
			BackupName: config.VeleroBackupName,
			Timestamp:  time.Now().UTC(),
			VMs:        []VMBackupReference{},
		}
	}

	// Add/update VM reference
	vmManifestPath := fmt.Sprintf("manifests/%s/%s-%s.json",
		config.VeleroBackupName, config.VMNamespace, config.VMName)

	vmRef := VMBackupReference{
		Name:         config.VMName,
		Namespace:    config.VMNamespace,
		CheckpointID: config.CheckpointName,
		ManifestPath: vmManifestPath,
	}

	// Update or add VM reference
	found := false
	for i, ref := range backupManifest.VMs {
		if ref.Name == config.VMName && ref.Namespace == config.VMNamespace {
			backupManifest.VMs[i] = vmRef
			found = true
			break
		}
	}
	if !found {
		backupManifest.VMs = append(backupManifest.VMs, vmRef)
	}

	// Write backup manifest
	manifestData, err := json.MarshalIndent(backupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup manifest: %w", err)
	}

	if err := store.PutObjectBytes(backupIndexPath, manifestData); err != nil {
		return fmt.Errorf("failed to write backup manifest: %w", err)
	}

	// Create per-VM backup manifest
	vmBackupManifest := VMBackupManifest{
		Namespace:       config.VMNamespace,
		Name:            config.VMName,
		CheckpointChain: chain,
		BackupName:      config.VeleroBackupName,
		Timestamp:       time.Now().UTC(),
	}

	vmManifestData, err := json.MarshalIndent(vmBackupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM backup manifest: %w", err)
	}

	if err := store.PutObjectBytes(vmManifestPath, vmManifestData); err != nil {
		return fmt.Errorf("failed to write VM backup manifest: %w", err)
	}

	return nil
}

// buildCheckpointChain builds the ordered list of checkpoints needed for restore.
// Starting from the target checkpoint, follows parent references back to the base full backup.
func buildCheckpointChain(checkpoints []CheckpointEntry, targetID string) []CheckpointEntry {
	// Build lookup map
	cpMap := make(map[string]CheckpointEntry)
	for _, cp := range checkpoints {
		cpMap[cp.ID] = cp
	}

	// Build chain by following parents
	var chain []CheckpointEntry
	currentID := targetID

	for currentID != "" {
		cp, ok := cpMap[currentID]
		if !ok {
			break
		}
		// Prepend to chain (oldest first)
		chain = append([]CheckpointEntry{cp}, chain...)
		currentID = cp.Parent
	}

	return chain
}
