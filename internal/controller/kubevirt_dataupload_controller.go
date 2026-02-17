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

// Package controller implements the KubeVirt DataMover controller
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// DefaultMaxConcurrentReconciles is the default number of concurrent reconciles
	DefaultMaxConcurrentReconciles = 3

	// DefaultTempPVCSize is the default size for temporary backup PVC
	DefaultTempPVCSize = "10Gi"

	// RequeueAfterShort is the short requeue duration for polling
	RequeueAfterShort = 5 * time.Second

	// RequeueAfterLong is the longer requeue duration
	RequeueAfterLong = 30 * time.Second

	// bslValidatedValue is the annotation value indicating BSL validation is complete
	bslValidatedValue = "true"

	// defaultCredentialKey is the default key in BSL credential secrets
	defaultCredentialKey = "cloud"
)

// KubeVirtDataUploadReconciler reconciles DataUpload objects where Spec.DataMover is "kubevirt"
type KubeVirtDataUploadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// OADPNamespace is the namespace where OADP and Velero resources are located
	OADPNamespace string

	// MaxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run
	MaxConcurrentReconciles int

	// DatamoverImage is the image to use for datamover pods
	DatamoverImage string

	// DatamoverImagePullPolicy is the pull policy for the datamover image
	DatamoverImagePullPolicy corev1.PullPolicy

	// ObjectStoreFactory creates an ObjectStore from an UploaderConfig.
	// Defaults to uploader.InitObjectStore if nil. Override in tests to inject mocks.
	ObjectStoreFactory func(cfg *uploader.UploaderConfig) (velero.ObjectStore, error)
}

// +kubebuilder:rbac:groups=velero.io,resources=datauploads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=datauploads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=backupstoragelocations,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups/status,verbs=get
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch

// Reconcile handles DataUpload resources where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the DataUpload
	dataUpload := &velerov2alpha1.DataUpload{}
	if err := r.Get(ctx, req.NamespacedName, dataUpload); err != nil {
		// Ignore not-found errors, as the object may have been deleted
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Skip if DataMover is not "kubevirt"
	if dataUpload.Spec.DataMover != common.DataMoverKubeVirt {
		logger.V(1).Info("Skipping DataUpload - DataMover is not kubevirt",
			"dataUpload", req.NamespacedName,
			"dataMover", dataUpload.Spec.DataMover)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling DataUpload with kubevirt datamover",
		"dataUpload", req.NamespacedName,
		"phase", dataUpload.Status.Phase)

	// Handle based on current phase
	switch dataUpload.Status.Phase {
	case "", velerov2alpha1.DataUploadPhaseNew:
		return r.handleNew(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseAccepted:
		return r.handleAccepted(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhasePrepared:
		return r.handlePrepared(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseInProgress:
		return r.handleInProgress(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCanceling:
		return r.handleCanceling(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCompleted,
		velerov2alpha1.DataUploadPhaseFailed,
		velerov2alpha1.DataUploadPhaseCanceled:
		// Terminal states - nothing to do
		logger.V(1).Info("DataUpload is in terminal state", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil

	default:
		logger.Info("Unknown DataUpload phase", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handleNew processes DataUploads in New phase
// Validates prerequisites and transitions to Accepted
func (r *KubeVirtDataUploadReconciler) handleNew(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling New phase DataUpload")

	// Step 1: Validate VM annotation exists
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference from DataUpload")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("Found VM reference", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Step 2: Fetch the VirtualMachine and validate prerequisites
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "VirtualMachine not found")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VirtualMachine %s/%s not found", vmRef.Namespace, vmRef.Name)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachine: %w", err)
	}

	// Step 3: Validate VM is running and CBT is enabled
	if err := common.ValidateVMForBackup(vm); err != nil {
		logger.Error(err, "VM validation failed")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("VM validation passed", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Transition to Accepted phase
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseAccepted, "DataUpload accepted by kubevirt datamover"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handleAccepted processes DataUploads in Accepted phase
// Creates VMBT/VMB and transitions to Prepared when ready
func (r *KubeVirtDataUploadReconciler) handleAccepted(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Accepted phase DataUpload")

	// Extract VirtualMachine reference from annotation
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Step 0: Verify BSL exists before creating any resources.
	// If BSL is unavailable, creating PVC/VMBT/VMB would be wasteful since
	// the DataUpload will fail in Prepared phase anyway.
	if du.Spec.BackupStorageLocation != "" {
		if _, err := r.getBackupStorageLocation(ctx, du); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(err, "BackupStorageLocation not found")
				if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("BackupStorageLocation not found: %v", err)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			// Transient error (network, API server issue) - requeue without creating resources
			logger.Info("BSL temporarily unavailable, will retry before creating resources",
				"reason", err.Error())
			return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
		}
	}

	// Step 1: Create or get temporary PVC for backup output
	pvc, err := r.ensureTempPVC(ctx, logger, du, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure temporary PVC")
		return ctrl.Result{}, err
	}
	logger.Info("Temporary PVC ready", "pvc", pvc.Name)

	// Step 2: Create or get VirtualMachineBackupTracker
	vmbt, err := r.ensureVMBackupTracker(ctx, logger, du, vmRef.Name, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure VirtualMachineBackupTracker")
		return ctrl.Result{}, err
	}
	logger.Info("VirtualMachineBackupTracker ready", "vmbt", vmbt.Name)

	// Step 3: Determine backup mode (full vs incremental) and update VMBT accordingly.
	forceFullBackup := r.resolveBackupMode(ctx, logger, du, vmbt, vmRef)

	// Step 4: Create VirtualMachineBackup if it doesn't exist
	vmb, created, err := r.ensureVMBackup(ctx, logger, du, vmbt, pvc.Name, vmRef.Namespace, forceFullBackup)
	if err != nil {
		// Check if the error is due to another VMB being in progress for the same VM.
		// KubeVirt's admission webhook only allows one active (non-terminal) VMB per VM.
		// Requeue with a longer delay instead of returning an error (which causes
		// exponential backoff retry storm).
		if strings.Contains(err.Error(), "in progress for source") {
			logger.Info("Another VirtualMachineBackup is in progress for this VM, will retry",
				"reason", err.Error())
			return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
		}
		logger.Error(err, "Failed to ensure VirtualMachineBackup")
		return ctrl.Result{}, err
	}

	if created {
		logger.Info("Created VirtualMachineBackup", "vmb", vmb.Name)
		// Requeue to check status
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Step 6: Check VMB status
	if vmb.Status == nil {
		logger.Info("VirtualMachineBackup status not yet available, requeuing")
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Simple logic: check the Done condition only
	for _, cond := range vmb.Status.Conditions {
		if cond.Type == kubevirtbackupv1alpha1.ConditionDone {
			if cond.Status == corev1.ConditionTrue {
				// Success
				logger.Info("VirtualMachineBackup completed",
					"vmb", vmb.Name,
					"type", vmb.Status.Type,
					"checkpoint", vmb.Status.CheckpointName)

				if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhasePrepared,
					fmt.Sprintf("VMBackup completed (type=%s)", vmb.Status.Type)); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
			}
			// Done: False means explicit failure
			logger.Error(nil, "VirtualMachineBackup failed", "reason", cond.Reason, "message", cond.Message)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VMBackup failed: %s", cond.Message)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// No Done condition yet - still in progress
	logger.Info("VirtualMachineBackup in progress, requeuing")
	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// resolveBackupMode determines whether to force a full backup or allow incremental.
// Returns true when ForceFullBackup should be set on the VMB.
// This covers two scenarios:
//  1. User explicitly requested force-full-backup via annotation on DataUpload.
//  2. BSL checkpoint validation found a broken chain (stale VMBT checkpoint with
//     no valid S3 data), requiring a forced full backup as defense-in-depth.
func (r *KubeVirtDataUploadReconciler) resolveBackupMode(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, vmRef *common.VMReference) bool {
	// Check if force full backup is requested via annotation.
	if du.Annotations[common.AnnotationForceFullBackup] == bslValidatedValue {
		logger.Info("Force full backup requested via annotation, skipping BSL checkpoint lookup")

		vmbtHasCheckpoint := vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil &&
			vmbt.Status.LatestCheckpoint.Name != ""
		if vmbtHasCheckpoint {
			if err := r.clearVMBTCheckpoint(ctx, logger, vmbt); err != nil {
				logger.Error(err, "Failed to clear VMBT checkpoint for forced full backup")
			} else {
				logger.Info("Cleared VMBT checkpoint for forced full backup")
			}
		}
		return true
	}

	// Validate checkpoint chain in BSL for incremental backup support.
	// This runs once per DataUpload (tracked via annotation) to avoid redundant
	// S3 queries on every reconcile, while still validating the BSL state for
	// each new backup.
	if du.Annotations[common.AnnotationBSLValidated] == bslValidatedValue {
		logger.V(1).Info("BSL validation already completed for this DataUpload")
		return false
	}

	return r.validateBSLCheckpoint(ctx, logger, du, vmbt, vmRef)
}

// validateBSLCheckpoint queries the BSL for a valid checkpoint chain and updates
// the VMBT accordingly. Returns true when a forced full backup is required because
// the checkpoint chain was found to be broken.
func (r *KubeVirtDataUploadReconciler) validateBSLCheckpoint(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, vmRef *common.VMReference) bool {
	forceFullBackup := false

	var checkpointLookup *uploader.CheckpointLookupResult
	bsl, bslErr := r.getBackupStorageLocation(ctx, du)
	if bslErr != nil {
		// BSL lookup failure is non-fatal. VMBT is left as-is: if it already has
		// a valid checkpoint, KubeVirt will use it for incremental backup.
		// Validation will be retried on the next reconcile.
		logger.Info("BSL not available for checkpoint lookup, skipping validation",
			"reason", bslErr.Error())
	} else {
		var err error
		checkpointLookup, err = r.lookupCheckpointFromBSL(ctx, bsl, vmRef.Namespace, vmRef.Name)
		if err != nil {
			// Checkpoint lookup failure is non-fatal. VMBT is left as-is
			// and validation will be retried on the next reconcile.
			logger.Info("Checkpoint lookup failed, skipping validation",
				"reason", err.Error())
		} else if checkpointLookup != nil {
			logger.Info("Checkpoint lookup completed",
				"found", checkpointLookup.Found,
				"message", checkpointLookup.Message)
		}
	}

	// Update VMBT based on BSL validation result.
	// Only act on definitive results (checkpointLookup != nil). If BSL was
	// unreachable or lookup errored, checkpointLookup is nil and we leave
	// the VMBT as-is (don't clear a potentially valid checkpoint due to
	// transient failures).
	vmbtHasCheckpoint := vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil &&
		vmbt.Status.LatestCheckpoint.Name != ""

	if checkpointLookup != nil && checkpointLookup.Found {
		// Valid checkpoint chain found in BSL - set it on VMBT for incremental backup
		if err := r.updateVMBTCheckpoint(ctx, logger, vmbt, checkpointLookup.LatestCheckpoint); err != nil {
			// Non-fatal: if we can't update the VMBT, KubeVirt will do a full backup
			logger.Info("Failed to update VMBT with checkpoint, will perform full backup",
				"checkpoint", checkpointLookup.LatestCheckpoint,
				"reason", err.Error())
		} else {
			logger.Info("Updated VMBT with latest checkpoint from BSL",
				"checkpoint", checkpointLookup.LatestCheckpoint,
				"chainLength", checkpointLookup.ChainLength)
		}
	} else if checkpointLookup != nil && !checkpointLookup.Found && vmbtHasCheckpoint {
		// BSL was reachable and explicitly returned no valid checkpoint chain,
		// but VMBT has a stale checkpoint from a previous backup.
		// Clear it and force full backup so KubeVirt doesn't use invalid data.
		staleCheckpoint := vmbt.Status.LatestCheckpoint.Name
		if err := r.clearVMBTCheckpoint(ctx, logger, vmbt); err != nil {
			logger.Error(err, "Failed to clear stale VMBT checkpoint, backup may fail",
				"staleCheckpoint", staleCheckpoint)
		} else {
			logger.Info("Cleared stale VMBT checkpoint, will force full backup",
				"staleCheckpoint", staleCheckpoint,
				"reason", checkpointLookup.Message)
		}
		// Also set ForceFullBackup on the VMB as defense-in-depth: even if a
		// checkpoint appears on the VMBT between now and VMB creation (race),
		// KubeVirt will still perform a full backup.
		forceFullBackup = true
	}

	// Mark BSL validation as done for this DataUpload to avoid redundant S3 queries.
	// Only set the annotation when we got a definitive result from BSL (checkpointLookup != nil).
	// If BSL was unreachable or the lookup failed due to transient errors (e.g., read-only
	// filesystem, network errors), don't set the annotation so validation will be retried
	// on the next reconcile.
	if checkpointLookup != nil {
		if du.Annotations == nil {
			du.Annotations = make(map[string]string)
		}
		du.Annotations[common.AnnotationBSLValidated] = bslValidatedValue
		if err := r.Update(ctx, du); err != nil {
			// Non-fatal: worst case we re-run BSL validation on next reconcile
			logger.Info("Failed to set BSL validated annotation, will retry",
				"reason", err.Error())
		}
	}

	return forceFullBackup
}

// handlePrepared processes DataUploads in Prepared phase
// Rebinds PV to OADP namespace, launches datamover pod, and transitions to InProgress
func (r *KubeVirtDataUploadReconciler) handlePrepared(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Prepared phase DataUpload")

	// Get VM reference for namespace context
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Datamover pod runs in OADP namespace (where credentials are accessible)
	podNamespace := r.getPodNamespace(du)

	// Check if datamover pod already exists (idempotency)
	podName := getDatamoverPodName(du)
	existingPod := &corev1.Pod{}
	err = r.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, existingPod)
	if err == nil {
		// Pod exists, transition to InProgress and monitor
		logger.Info("Datamover pod already exists, transitioning to InProgress", "pod", podName)
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod running"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}
	if !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing datamover pod: %w", err)
	}

	// Validate BSL and VMB exist BEFORE rebinding PV
	// This prevents leaving PV in a bad state if these checks fail

	// Get BackupStorageLocation
	bsl, err := r.getBackupStorageLocation(ctx, du)
	if err != nil {
		logger.Error(err, "Failed to get BackupStorageLocation")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to get BSL: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Get VMB to extract checkpoint info
	vmb, err := r.getVMBackup(ctx, du, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to get VirtualMachineBackup")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to get VMB: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check if PV has already been rebound (idempotency)
	reboundPVCName := fmt.Sprintf("%s%s", common.ReboundPVCNamePrefix, du.Name)
	reboundPVC := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: reboundPVCName, Namespace: podNamespace}, reboundPVC)
	pvAlreadyRebound := err == nil && reboundPVC.Status.Phase == corev1.ClaimBound

	if !pvAlreadyRebound {
		// Rebind PV from VM namespace to OADP namespace
		// This allows the datamover pod to access both the backup data AND credentials
		sourcePVCName := fmt.Sprintf("%s%s", common.TempPVCNamePrefix, du.Name)

		logger.Info("Rebinding PV from VM namespace to OADP namespace",
			"sourcePVC", sourcePVCName,
			"sourceNamespace", vmRef.Namespace,
			"targetNamespace", podNamespace)

		rebindResult, err := r.rebindPVToNamespace(ctx, logger, sourcePVCName, vmRef.Namespace, podNamespace, du.Name)
		if err != nil {
			// Fail without retry: PV rebind is a multi-step operation (delete PVC, patch PV, create new PVC).
			// If it fails partway through, automatic retries could leave resources in an inconsistent state.
			// Failing allows the user to investigate and take corrective action.
			logger.Error(err, "Failed to rebind PV to OADP namespace")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to rebind PV: %v", err)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		logger.Info("Successfully rebound PV to OADP namespace",
			"newPVC", rebindResult.NewPVCName,
			"pv", rebindResult.PVName)

		reboundPVCName = rebindResult.NewPVCName
	} else {
		logger.Info("PV already rebound to OADP namespace", "pvc", reboundPVCName)
	}

	// Get backup type from VMB status
	backupType := "full"
	if vmb.Status != nil && vmb.Status.Type != "" {
		backupType = string(vmb.Status.Type)
	}

	// Get checkpoint name from VMB status
	checkpointName := ""
	if vmb.Status != nil && vmb.Status.CheckpointName != nil {
		checkpointName = *vmb.Status.CheckpointName
	}

	// Build datamover pod config - now using OADP namespace and rebound PVC
	podConfig, err := r.buildDatamoverPodConfig(du, bsl, vmb, vmRef, backupType, checkpointName)
	if err != nil {
		logger.Error(err, "Failed to build datamover pod config")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to build pod config: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Override namespace and PVC name for the rebound resources
	podConfig.Namespace = podNamespace
	podConfig.SourcePVCName = reboundPVCName

	// Create the datamover pod
	pod := buildDatamoverPod(podConfig)

	// Set owner reference so pod is cleaned up when DataUpload is deleted
	// This works now because pod is in the same namespace as DataUpload
	if err := controllerutil.SetOwnerReference(du, pod, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set owner reference on pod: %w", err)
	}

	if err := r.Create(ctx, pod); err != nil {
		if errors.IsAlreadyExists(err) {
			// Race condition - pod was created between check and create
			logger.Info("Datamover pod already exists (race)", "pod", podName)
		} else {
			return ctrl.Result{}, fmt.Errorf("failed to create datamover pod: %w", err)
		}
	} else {
		logger.Info("Created datamover pod", "pod", podName, "namespace", podNamespace)
	}

	// Transition to InProgress
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod launched"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handleInProgress processes DataUploads in InProgress phase
// Monitors datamover pod and transitions to Completed/Failed
func (r *KubeVirtDataUploadReconciler) handleInProgress(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling InProgress phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Get the datamover pod
	podName := getDatamoverPodName(du)
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			// Pod not found - this is unexpected in InProgress phase
			logger.Error(err, "Datamover pod not found", "pod", podName, "namespace", podNamespace)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, "Datamover pod not found"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get datamover pod: %w", err)
	}

	// Check pod status
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		logger.Info("Datamover pod completed successfully", "pod", podName)

		// Cleanup resources
		r.cleanupDatamoverResources(ctx, logger, du, podNamespace)

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCompleted, "Data upload completed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodFailed:
		failureMessage := extractPodFailureMessage(pod)
		logger.Error(nil, "Datamover pod failed", "pod", podName, "message", failureMessage)

		// Skip cleanup on failure to preserve resources for debugging.
		// Resources (pod, rebound PVC/PV) can be manually cleaned up after investigation.

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Datamover pod failed: %s", failureMessage)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodPending, corev1.PodRunning:
		logger.V(1).Info("Datamover pod still running", "pod", podName, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil

	default:
		logger.Info("Datamover pod in unknown phase", "pod", podName, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}
}

// cleanupDatamoverResources cleans up resources created during the datamover process
func (r *KubeVirtDataUploadReconciler) cleanupDatamoverResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, podNamespace string) {
	// Delete the datamover pod
	podName := getDatamoverPodName(du)
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, pod); err == nil {
		if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete datamover pod", "pod", podName)
		} else {
			logger.Info("Deleted datamover pod", "pod", podName)
		}
	}

	// Delete the rebound PVC and PV
	reboundPVCName := fmt.Sprintf("%s%s", common.ReboundPVCNamePrefix, du.Name)
	if err := r.cleanupReboundPVCAndPV(ctx, logger, reboundPVCName, podNamespace, du.Name); err != nil {
		logger.Error(err, "Failed to cleanup rebound PVC and PV", "pvc", reboundPVCName)
		// Continue - don't block completion on cleanup failures
	}

	// TODO Phase 5: Also cleanup VMB in VM namespace if needed
}

// handleCanceling processes DataUploads in Canceling phase
// Cleans up resources and transitions to Canceled
func (r *KubeVirtDataUploadReconciler) handleCanceling(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Canceling phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Clean up datamover resources in OADP namespace
	r.cleanupDatamoverResources(ctx, logger, du, podNamespace)

	// TODO Phase 5: Cancel in-progress operations
	// - Delete VMB if exists in VM namespace
	// - Delete VMBT if exists in VM namespace

	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCanceled, "DataUpload canceled"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updatePhase updates the DataUpload phase and status message
// Uses Update instead of Status().Patch() to match Velero's approach,
// which works regardless of whether the CRD has status subresource enabled
func (r *KubeVirtDataUploadReconciler) updatePhase(ctx context.Context, du *velerov2alpha1.DataUpload, phase velerov2alpha1.DataUploadPhase, message string) error {
	logger := log.FromContext(ctx)

	// Skip update if already at target phase with same message (idempotency)
	if du.Status.Phase == phase && du.Status.Message == message {
		logger.V(1).Info("DataUpload already at target phase with same message, skipping update",
			"dataUpload", du.Name,
			"phase", phase)
		return nil
	}

	du.Status.Phase = phase
	du.Status.Message = message

	if err := r.Update(ctx, du); err != nil {
		logger.Error(err, "Failed to update DataUpload phase",
			"dataUpload", du.Name,
			"phase", phase)
		return fmt.Errorf("failed to update DataUpload phase to %s: %w", phase, err)
	}

	logger.Info("Updated DataUpload phase",
		"dataUpload", du.Name,
		"phase", phase,
		"message", message)

	return nil
}

// getPodNamespace returns the namespace where datamover pods should run.
// Uses OADPNamespace if configured, otherwise falls back to the DataUpload's namespace.
func (r *KubeVirtDataUploadReconciler) getPodNamespace(du *velerov2alpha1.DataUpload) string {
	if r.OADPNamespace != "" {
		return r.OADPNamespace
	}
	return du.Namespace
}

// SetupWithManager sets up the controller with the Manager
func (r *KubeVirtDataUploadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentReconciles
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1.DataUpload{}).
		WithEventFilter(r.filterKubeVirtDataMover()).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrent,
		}).
		Named("kubevirt-dataupload").
		Complete(r)
}

// filterKubeVirtDataMover returns a predicate that filters for DataUploads
// where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) filterKubeVirtDataMover() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		du, ok := obj.(*velerov2alpha1.DataUpload)
		if !ok {
			return false
		}
		return du.Spec.DataMover == common.DataMoverKubeVirt
	})
}

// ensureTempPVC creates or retrieves the temporary PVC for backup output.
// Note: We don't set an owner reference because the PVC is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// The PVC will be cleaned up during PV rebinding or explicit cleanup.
func (r *KubeVirtDataUploadReconciler) ensureTempPVC(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvcName := fmt.Sprintf("kubevirt-backup-%s", du.Name)

	// Check if PVC already exists
	existingPVC := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, existingPVC)
	if err == nil {
		logger.V(1).Info("Temporary PVC already exists", "pvc", pvcName)
		return existingPVC, nil
	}

	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to check for existing PVC: %w", err)
	}

	// Create new PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
				common.LabelDataUploadUID:  string(du.UID),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(DefaultTempPVCSize),
				},
			},
		},
	}

	if err := r.Create(ctx, pvc); err != nil {
		return nil, fmt.Errorf("failed to create temporary PVC: %w", err)
	}

	logger.Info("Created temporary PVC", "pvc", pvcName, "namespace", namespace)
	return pvc, nil
}

// ensureVMBackupTracker creates or retrieves the VirtualMachineBackupTracker for the VM
func (r *KubeVirtDataUploadReconciler) ensureVMBackupTracker(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmName, vmNamespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	// Use a consistent name based on VM name for tracking across backups
	vmbtName := fmt.Sprintf("vmbt-%s", vmName)

	// Check if VMBT already exists
	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmNamespace}, existingVMBT)
	if err == nil {
		logger.V(1).Info("VirtualMachineBackupTracker already exists", "vmbt", vmbtName)
		return existingVMBT, nil
	}

	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to check for existing VMBT: %w", err)
	}

	// Create new VMBT
	apiGroup := "kubevirt.io"
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmbtName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	if err := r.Create(ctx, vmbt); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachineBackupTracker: %w", err)
	}

	logger.Info("Created VirtualMachineBackupTracker", "vmbt", vmbtName, "namespace", vmNamespace)
	return vmbt, nil
}

// ensureVMBackup creates or retrieves the VirtualMachineBackup for this DataUpload.
// Returns the VMB, whether it was created (vs already existed), and any error.
// When forceFullBackup is true, the VMB is created with ForceFullBackup=true in its spec,
// which tells KubeVirt to perform a full backup regardless of any existing checkpoint.
// Note: We don't set an owner reference because VMB is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// VMB cleanup will be handled explicitly in Phase 5.
func (r *KubeVirtDataUploadReconciler) ensureVMBackup(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, pvcName, namespace string, forceFullBackup bool) (*kubevirtbackupv1alpha1.VirtualMachineBackup, bool, error) {
	// Use DataUpload name for VMB to ensure 1:1 mapping
	vmbName := fmt.Sprintf("vmb-%s", du.Name)

	// Check if VMB already exists
	existingVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	err := r.Get(ctx, types.NamespacedName{Name: vmbName, Namespace: namespace}, existingVMB)
	if err == nil {
		logger.V(1).Info("VirtualMachineBackup already exists", "vmb", vmbName)
		return existingVMB, false, nil
	}

	if !errors.IsNotFound(err) {
		return nil, false, fmt.Errorf("failed to check for existing VMB: %w", err)
	}

	// Create new VMB referencing the VMBT (enables incremental backups)
	apiGroup := "backup.kubevirt.io"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmbName,
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
				common.LabelDataUploadUID:  string(du.UID),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName:         &pvcName,
			ForceFullBackup: forceFullBackup,
		},
	}

	if forceFullBackup {
		logger.Info("Creating VirtualMachineBackup with ForceFullBackup=true", "vmb", vmbName)
	}

	if err := r.Create(ctx, vmb); err != nil {
		return nil, false, fmt.Errorf("failed to create VirtualMachineBackup: %w", err)
	}

	logger.Info("Created VirtualMachineBackup", "vmb", vmbName, "namespace", namespace, "tracker", vmbt.Name)
	return vmb, true, nil
}

// getDatamoverPodName returns the name for the datamover pod
func getDatamoverPodName(du *velerov2alpha1.DataUpload) string {
	return fmt.Sprintf("%s%s", common.DatamoverPodNamePrefix, du.Name)
}

// getBackupStorageLocation fetches the BSL referenced by the DataUpload
func (r *KubeVirtDataUploadReconciler) getBackupStorageLocation(ctx context.Context, du *velerov2alpha1.DataUpload) (*velerov1.BackupStorageLocation, error) {
	bslName := du.Spec.BackupStorageLocation
	if bslName == "" {
		return nil, fmt.Errorf("DataUpload %s has no BackupStorageLocation specified", du.Name)
	}

	// BSL is in the OADP namespace (where Velero runs)
	namespace := r.OADPNamespace
	if namespace == "" {
		// Fall back to the DataUpload's namespace
		namespace = du.Namespace
	}

	bsl := &velerov1.BackupStorageLocation{}
	if err := r.Get(ctx, types.NamespacedName{Name: bslName, Namespace: namespace}, bsl); err != nil {
		return nil, fmt.Errorf("failed to get BackupStorageLocation %s/%s: %w", namespace, bslName, err)
	}

	return bsl, nil
}

// getVMBackup fetches the VirtualMachineBackup for this DataUpload
func (r *KubeVirtDataUploadReconciler) getVMBackup(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackup, error) {
	vmbName := fmt.Sprintf("%s%s", common.VMBackupNamePrefix, du.Name)

	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmbName, Namespace: namespace}, vmb); err != nil {
		return nil, fmt.Errorf("failed to get VirtualMachineBackup %s/%s: %w", namespace, vmbName, err)
	}

	return vmb, nil
}

// getVeleroBackupName extracts the Velero backup name from DataUpload labels
func getVeleroBackupName(du *velerov2alpha1.DataUpload) string {
	if du.Labels == nil {
		return ""
	}
	return du.Labels[common.LabelVeleroBackupName]
}

// buildDatamoverPodConfig assembles the configuration for the datamover pod
func (r *KubeVirtDataUploadReconciler) buildDatamoverPodConfig(
	du *velerov2alpha1.DataUpload,
	bsl *velerov1.BackupStorageLocation,
	vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
	vmRef *common.VMReference,
	backupType string,
	checkpointName string,
) (*DatamoverPodConfig, error) {
	cfg, err := extractBSLConfig(bsl)
	if err != nil {
		return nil, err
	}

	if cfg.CredentialName == "" {
		return nil, fmt.Errorf("BSL %s has no credential secret configured", bsl.Name)
	}

	// Get PVC name
	pvcName := fmt.Sprintf("%s%s", common.TempPVCNamePrefix, du.Name)

	// Determine datamover image
	image := r.DatamoverImage
	if image == "" {
		return nil, fmt.Errorf("datamover image not configured")
	}

	pullPolicy := r.DatamoverImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}

	return &DatamoverPodConfig{
		Name:                 getDatamoverPodName(du),
		Namespace:            vmRef.Namespace,
		Image:                image,
		ImagePullPolicy:      pullPolicy,
		BSLProvider:          cfg.Provider,
		BSLBucket:            cfg.Bucket,
		BSLPrefix:            cfg.Prefix,
		BSLRegion:            cfg.Region,
		CredentialSecretName: cfg.CredentialName,
		CredentialSecretKey:  cfg.CredentialKey,
		VMName:               vmRef.Name,
		VMNamespace:          vmRef.Namespace,
		CheckpointName:       checkpointName,
		BackupType:           backupType,
		VeleroBackupName:     getVeleroBackupName(du),
		DataUploadName:       du.Name,
		DataUploadUID:        string(du.UID),
		VMBName:              vmb.Name,
		SourcePVCName:        pvcName,
		Labels:               make(map[string]string),
	}, nil
}

// extractPodFailureMessage extracts the failure message from a failed pod
func extractPodFailureMessage(pod *corev1.Pod) string {
	// Check container statuses for termination message
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}

	// Check init container statuses
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
	}

	// Fall back to pod conditions
	for _, cond := range pod.Status.Conditions {
		if cond.Status == corev1.ConditionFalse && cond.Message != "" {
			return cond.Message
		}
	}

	return "unknown error"
}

// bslConfig holds extracted BSL configuration used by both the datamover pod
// and the checkpoint lookup.
type bslConfig struct {
	Provider       string
	Bucket         string
	Prefix         string // Datamover prefix (e.g., "velero-kubevirt-datamover")
	Region         string
	CredentialName string
	CredentialKey  string
}

// extractBSLConfig extracts and validates common BSL configuration fields.
// The returned prefix includes the datamover suffix (e.g., "velero-kubevirt-datamover").
func extractBSLConfig(bsl *velerov1.BackupStorageLocation) (*bslConfig, error) {
	bucket := ""
	prefix := ""
	if bsl.Spec.ObjectStorage != nil {
		bucket = bsl.Spec.ObjectStorage.Bucket
		prefix = bsl.Spec.ObjectStorage.Prefix
	}
	if bucket == "" {
		return nil, fmt.Errorf("BSL %s has no bucket configured", bsl.Name)
	}

	// Add our datamover prefix to the BSL prefix
	if prefix != "" {
		prefix = prefix + "-" + common.DatamoverBSLPrefix
	} else {
		prefix = common.DatamoverBSLPrefix
	}

	region := ""
	if bsl.Spec.Config != nil {
		region = bsl.Spec.Config["region"]
	}

	credName := ""
	credKey := defaultCredentialKey
	if bsl.Spec.Credential != nil {
		credName = bsl.Spec.Credential.Name
		if bsl.Spec.Credential.Key != "" {
			credKey = bsl.Spec.Credential.Key
		}
	}

	return &bslConfig{
		Provider:       bsl.Spec.Provider,
		Bucket:         bucket,
		Prefix:         prefix,
		Region:         region,
		CredentialName: credName,
		CredentialKey:  credKey,
	}, nil
}

// lookupCheckpointFromBSL reads the VM's checkpoint index from the BSL and returns
// the latest valid checkpoint for incremental backup support.
// This method initializes an object store client, reads the BSL credentials from
// the secret, and queries the checkpoint index.
func (r *KubeVirtDataUploadReconciler) lookupCheckpointFromBSL(ctx context.Context, bsl *velerov1.BackupStorageLocation, vmNamespace, vmName string) (*uploader.CheckpointLookupResult, error) {
	cfg, err := extractBSLConfig(bsl)
	if err != nil {
		return nil, err
	}

	if cfg.CredentialName == "" {
		return nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	// Get credentials from BSL secret (in-memory, no temp file)
	credData, err := r.getCredentialsFromBSL(ctx, bsl)
	if err != nil {
		return nil, fmt.Errorf("failed to get BSL credentials: %w", err)
	}

	// Initialize object store using provider dispatch
	factory := r.ObjectStoreFactory
	if factory == nil {
		factory = func(c *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return uploader.InitObjectStore(c)
		}
	}
	// NOTE: The ObjectStore (and its underlying HTTP client) is not explicitly closed
	// after use. The velero.ObjectStore interface does not define a Close() method.
	// Go's garbage collector will reclaim idle connections. If performance becomes a
	// concern, consider caching the store instance across reconcile calls.
	store, err := factory(&uploader.UploaderConfig{
		BSLProvider:     cfg.Provider,
		BSLBucket:       cfg.Bucket,
		BSLPrefix:       cfg.Prefix,
		BSLRegion:       cfg.Region,
		CredentialsData: credData,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize object store: %w", err)
	}

	// Lookup the latest checkpoint
	result, err := uploader.LookupLatestCheckpoint(ctx, store, cfg.Bucket, vmNamespace, vmName)
	if err != nil {
		return nil, fmt.Errorf("checkpoint lookup failed: %w", err)
	}

	return result, nil
}

// getCredentialsFromBSL reads the BSL credential secret and returns the raw
// credential bytes. The credentials are kept in memory and never written to disk.
func (r *KubeVirtDataUploadReconciler) getCredentialsFromBSL(
	ctx context.Context, bsl *velerov1.BackupStorageLocation,
) ([]byte, error) {
	if bsl.Spec.Credential == nil {
		return nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	secretName := bsl.Spec.Credential.Name
	secretKey := bsl.Spec.Credential.Key
	if secretKey == "" {
		secretKey = defaultCredentialKey
	}

	// BSL credentials secret is in the same namespace as the BSL
	namespace := bsl.Namespace
	if namespace == "" {
		namespace = r.OADPNamespace
	}

	// Fetch the secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name: secretName, Namespace: namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf(
			"failed to get credential secret %s/%s: %w",
			namespace, secretName, err,
		)
	}

	credData, ok := secret.Data[secretKey]
	if !ok {
		return nil, fmt.Errorf(
			"credential secret %s/%s does not contain key %q",
			namespace, secretName, secretKey,
		)
	}

	return credData, nil
}

// clearVMBTCheckpoint removes the checkpoint from the VMBT status, forcing KubeVirt
// to perform a full backup. This is called when BSL validation determines the
// checkpoint chain is invalid (e.g., index.json or qcow2 files were deleted from S3).
func (r *KubeVirtDataUploadReconciler) clearVMBTCheckpoint(ctx context.Context, logger logr.Logger, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker) error {
	if vmbt.Status == nil || vmbt.Status.LatestCheckpoint == nil {
		// Nothing to clear
		return nil
	}

	vmbt.Status.LatestCheckpoint = nil

	if err := r.Status().Update(ctx, vmbt); err != nil {
		return fmt.Errorf("failed to clear VMBT %s/%s checkpoint: %w",
			vmbt.Namespace, vmbt.Name, err)
	}

	logger.Info("Cleared VMBT checkpoint", "vmbt", vmbt.Name)
	return nil
}

// updateVMBTCheckpoint updates the VirtualMachineBackupTracker status with the latest
// checkpoint from BSL. This enables KubeVirt to perform an incremental backup
// based on the given checkpoint.
func (r *KubeVirtDataUploadReconciler) updateVMBTCheckpoint(ctx context.Context, logger logr.Logger, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, checkpointName string) error {
	// Check if VMBT already has this checkpoint set
	if vmbt.Status != nil &&
		vmbt.Status.LatestCheckpoint != nil &&
		vmbt.Status.LatestCheckpoint.Name == checkpointName {
		logger.V(1).Info("VMBT already has the latest checkpoint", "checkpoint", checkpointName)
		return nil
	}

	// Set the latest checkpoint on the VMBT status
	now := metav1.Now()
	if vmbt.Status == nil {
		vmbt.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{}
	}
	vmbt.Status.LatestCheckpoint = &kubevirtbackupv1alpha1.BackupCheckpoint{
		Name:         checkpointName,
		CreationTime: &now,
	}

	if err := r.Status().Update(ctx, vmbt); err != nil {
		return fmt.Errorf("failed to update VMBT %s/%s status with checkpoint %s: %w",
			vmbt.Namespace, vmbt.Name, checkpointName, err)
	}

	logger.Info("Updated VMBT with checkpoint",
		"vmbt", vmbt.Name,
		"checkpoint", checkpointName)

	return nil
}
