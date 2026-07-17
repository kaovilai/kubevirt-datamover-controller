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
package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/downloader"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ddScheme returns a scheme with only the types the DataDownload controller
// needs. Deliberately does NOT register kubevirtcorev1 -- handleNew/handleAccepted
// must never fetch a live VirtualMachine object; if they tried, the fake client
// would fail with "no kind registered" and these tests would catch it.
func ddScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func TestDataDownloadReconcile(t *testing.T) {
	scheme := ddScheme()

	tests := []struct {
		name            string
		dataDownload    *velerov2alpha1.DataDownload
		expectedRequeue bool
		expectedPhase   velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "skip non-kubevirt datamover",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: "velero"},
			},
			expectedRequeue: false,
			expectedPhase:   "",
		},
		{
			name: "new phase without VM annotation fails",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "terminal phase is a no-op",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseCompleted},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCompleted,
		},
		{
			name: "Cancel requested while InProgress routes to Canceling instead of the normal handler",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCanceling,
		},
		{
			name: "Cancel requested while already terminal is ignored",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseFailed},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dataDownload).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (result.RequeueAfter > 0) != tt.expectedRequeue {
				t.Errorf("requeue = %v, want %v", result.RequeueAfter > 0, tt.expectedRequeue)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q", updated.Status.Phase, tt.expectedPhase)
			}
		})
	}

	t.Run("not found is ignored", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "ns"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}
	})

	t.Run("Cancel requested but restore already provisioned completes instead of canceling", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp", UID: types.UID("dd-uid-provisioned")},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				Cancel:    true,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-disk-1",
					Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
		}
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Name: "restored-disk-1", Namespace: "restore-ns"},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, reboundPV).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (Cancel must not override an already-provisioned restore)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted)
		}
	})
}

func TestDataDownloadUpdatePhase(t *testing.T) {
	scheme := ddScheme()
	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
		Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseNew},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dd.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
		t.Errorf("phase = %q, want %q", dd.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
	}

	var before velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &before); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}

	// Idempotent: same phase + message should skip the update (no error, no-op).
	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error on idempotent update: %v", err)
	}

	var after velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &after); err != nil {
		t.Fatalf("failed to re-get DataDownload: %v", err)
	}
	if before.ResourceVersion != after.ResourceVersion {
		t.Errorf("expected no write on idempotent update, resourceVersion changed %s -> %s",
			before.ResourceVersion, after.ResourceVersion)
	}
}

func TestHandleNewDataDownload(t *testing.T) {
	scheme := ddScheme()

	bslAvailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}
	bslUnavailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseUnavailable},
	}

	tests := []struct {
		name          string
		dd            *velerov2alpha1.DataDownload
		bsl           *velerov1.BackupStorageLocation
		expectedPhase velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "missing VM annotation fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "dd-1", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default"},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not found fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-2", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "missing-bsl",
					TargetVolume: velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not available fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-3", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					TargetVolume: velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslUnavailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "valid annotation and available BSL transitions to Accepted",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-4", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					TargetVolume: velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseAccepted,
		},
		{
			name: "missing TargetVolume fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-5", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default"},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dd, tt.bsl).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

			// This would panic/error if handleNew ever tried to Get a VirtualMachine,
			// since kubevirtcorev1 isn't registered in ddScheme().
			if _, err := r.handleNew(context.Background(), logr.Discard(), tt.dd); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dd.Name, Namespace: tt.dd.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, tt.expectedPhase, updated.Status.Message)
			}
		})
	}
}

func TestCalculateScratchPVCSize(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-1",
				PVCs:     []string{"pvc-1", "pvc-2"},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi"), resource.MustParse("5Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
					{DiskName: "disk-2"},
				},
			},
			{
				ID:       "cp-2",
				PVCs:     []string{"pvc-1"},
				PVCSizes: []resource.Quantity{resource.MustParse("15Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
				},
			},
		},
	}

	// Computed via the same addOverhead helper the production code uses, rather
	// than a hand-picked literal, so this pins the formula (max PVCSize + sum file
	// sizes + overhead) without risking a hand-computed value drifting from it.
	maxPVCSize := resource.MustParse("15Gi") // max(10Gi, 15Gi) across the chain
	expectedBase := maxPVCSize.DeepCopy()
	expectedBase.Add(resource.MustParse("2Gi")) // sum of file sizes (1Gi + 1Gi)
	expectedSized := addOverhead(expectedBase, sizeOverheadPercent)

	tests := []struct {
		name               string
		chain              []string
		targetVolume       string
		files              []uploader.CheckpointFile
		targetDiskCapacity resource.Quantity
		expectExact        *resource.Quantity
	}{
		{
			name:         "sized from max PVCSizes across chain plus file sizes",
			chain:        []string{"cp-1", "cp-2"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			expectExact: &expectedSized,
		},
		{
			name:         "no matching checkpoints or files falls back to default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files:        nil,
			expectExact:  new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			name:         "missing PVCSizes floored by target disk capacity before overhead",
			chain:        []string{"cp-99"}, // no chain match -> maxDiskSize from PVCSizes stays zero
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			targetDiskCapacity: resource.MustParse("20Gi"),
			// floor(20Gi) + 1Gi file = 21Gi, then overhead -- computed the same way
			// as expectedSized above, not hand-picked.
			expectExact: func() *resource.Quantity {
				base := resource.MustParse("20Gi")
				base.Add(resource.MustParse("1Gi"))
				q := addOverhead(base, sizeOverheadPercent)
				return &q
			}(),
		},
		{
			// No raw-disk-size floor at all (no PVCSizes match, no target capacity),
			// but the chain itself has files -- sizing off the chain size alone
			// (undoubled) would under-provision for the flattened raw output the
			// downloader writes alongside the still-present chain. Small chain size
			// stays under the default, so the plain default wins.
			name:         "maxDiskSize zero with small file chain returns plain default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi, well under the 10Gi default
			},
			expectExact: new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			// Same no-floor scenario, but the chain size itself exceeds the default --
			// must size off the chain doubled (chain + flattened raw coexist), not the
			// now-insufficient default.
			name:         "maxDiskSize zero with large file chain doubles chain size instead of default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 15 * 1024 * 1024 * 1024}, // 15Gi, exceeds the 10Gi default
			},
			expectExact: func() *resource.Quantity {
				q := addOverhead(*resource.NewQuantity(30*1024*1024*1024, resource.BinarySI), sizeOverheadPercent)
				return &q
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateScratchPVCSize(logr.Discard(), vmIndex, tt.chain, tt.targetVolume, tt.files, tt.targetDiskCapacity)
			if got.Cmp(*tt.expectExact) != 0 {
				t.Errorf("size = %s, want exactly %s", got.String(), tt.expectExact.String())
			}
		})
	}
}

// startFakeBinder mimics the real Kubernetes PV binder, which fake clients
// don't run: once handleInProgress's rebind sets the PV's claimRef to point at
// the target PVC (what the code under test actually writes), this completes
// the bind by setting the PVC's Spec.VolumeName and Status.Phase, polling
// rather than a fixed sleep that could race ahead of the patch. Call site must
// `defer` the returned function immediately, so the goroutine is waited on
// (bounded by timeout) and its status-update error is asserted before the
// test returns -- extracted out of the caller to keep its cyclomatic
// complexity down (gocyclo) as much as to avoid duplicating this boilerplate.
func startFakeBinder(t *testing.T, fakeClient client.Client, scratchPV *corev1.PersistentVolume, targetPVC *corev1.PersistentVolumeClaim, timeout time.Duration) func() {
	t.Helper()
	binderDone := make(chan struct{})
	var statusUpdateErr error
	go func() {
		defer close(binderDone)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			pv := &corev1.PersistentVolume{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: scratchPV.Name}, pv); err != nil {
				return
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == targetPVC.Name && pv.Spec.ClaimRef.Namespace == targetPVC.Namespace {
				pvc := &corev1.PersistentVolumeClaim{}
				if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, pvc); err != nil {
					return
				}
				pvc.Spec.VolumeName = scratchPV.Name
				if err := fakeClient.Update(context.Background(), pvc); err != nil {
					return
				}
				pvc.Status.Phase = corev1.ClaimBound
				statusUpdateErr = fakeClient.Status().Update(context.Background(), pvc)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return func() {
		select {
		case <-binderDone:
			if statusUpdateErr != nil {
				t.Errorf("fake-binder goroutine failed to update PVC status: %v", statusUpdateErr)
			}
		case <-time.After(timeout + time.Second):
			t.Log("timed out waiting for fake-binder goroutine to finish")
		}
	}
}

// ddTestFixture bundles the objects needed to exercise handleAccepted/handlePrepared/
// handleInProgress against a mock object store seeded with a manifest+index.
type ddTestFixture struct {
	dd        *velerov2alpha1.DataDownload
	bsl       *velerov1.BackupStorageLocation
	credSec   *corev1.Secret
	targetPVC *corev1.PersistentVolumeClaim
	mockStore *uploader.MockObjectStore
}

func newDDTestFixture(t *testing.T) *ddTestFixture {
	t.Helper()

	const (
		vmName      = "test-vm"
		vmNamespace = "vm-ns"
		oadpNS      = "openshift-adp"
		backupName  = "backup-001"
		targetPVC   = "restored-disk-1"
		diskName    = "disk1" // KubeVirt volume name -- deliberately different from targetPVC
		restoreNS   = "restore-ns"
	)

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dd",
			Namespace: oadpNS,
			UID:       types.UID("dd-uid-123"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
			Labels: map[string]string{
				common.LabelVeleroBackupName: backupName,
			},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
			TargetVolume: velerov2alpha1.TargetVolumeSpec{
				PVC:       targetPVC,
				Namespace: restoreNS,
			},
		},
		Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: oadpNS},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}

	credSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: oadpNS},
		Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n")},
	}

	targetPVCObj := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: targetPVC, Namespace: restoreNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	checkpointID := "cp-001"
	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       checkpointID,
				Type:     "full",
				PVCs:     []string{targetPVC},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi")},
				Files: []uploader.CheckpointFile{
					{
						Filename:   "vmb-" + checkpointID + "-" + diskName + ".qcow2",
						DiskName:   diskName,
						Size:       1024 * 1024 * 1024,
						ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + checkpointID + "/vmb-" + checkpointID + "-" + diskName + ".qcow2",
					},
				},
			},
		},
	}
	if err := uploader.PutVMIndex(mockStore, vmNamespace, vmName, "test-bucket", vmIndex); err != nil {
		t.Fatalf("failed to seed VM index: %v", err)
	}

	manifest := uploader.VMBackupManifest{
		Namespace:       vmNamespace,
		Name:            vmName,
		CheckpointChain: []string{checkpointID},
		BackupName:      backupName,
	}
	if err := uploader.PutVMBackupManifest(mockStore, vmNamespace, vmName, backupName, "test-bucket", manifest); err != nil {
		t.Fatalf("failed to seed VM backup manifest: %v", err)
	}

	return &ddTestFixture{dd: dd, bsl: bsl, credSec: credSec, targetPVC: targetPVCObj, mockStore: mockStore}
}

func TestHandleAcceptedDataDownload(t *testing.T) {
	t.Run("target PVC not found requeues without failing", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build() // no targetPVC
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue when target PVC is missing")
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
			t.Error("missing target PVC should requeue, not fail -- Velero may not have created it yet")
		}
	})

	t.Run("missing manifest fails", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		emptyStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return emptyStore, nil },
		}

		if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("happy path resolves chain, creates scratch PVC, transitions to Prepared", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue after transitioning to Prepared")
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
		}

		scratchPVC, err := r.findScratchPVC(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("failed to find scratch PVC: %v", err)
		}
		if scratchPVC == nil {
			t.Fatal("expected scratch PVC to be created")
		}
		if scratchPVC.Namespace != "openshift-adp" {
			t.Errorf("scratch PVC namespace = %q, want %q", scratchPVC.Namespace, "openshift-adp")
		}
		if scratchPVC.Spec.StorageClassName == nil || *scratchPVC.Spec.StorageClassName != "standard" {
			t.Errorf("scratch PVC storageClassName = %v, want %q", scratchPVC.Spec.StorageClassName, "standard")
		}
		if scratchPVC.Spec.VolumeMode == nil || *scratchPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
			t.Errorf("scratch PVC volumeMode = %v, want %q", scratchPVC.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
		}
		if got := updated.Annotations[AnnotationTargetDiskName]; got != "disk1" {
			t.Errorf("annotation %s = %q, want %q", AnnotationTargetDiskName, got, "disk1")
		}
	})

	t.Run("Block volumeMode target PVC fails without creating a scratch PVC", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		f.targetPVC.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
		}

		scratchPVC, err := r.findScratchPVC(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("failed to check for scratch PVC: %v", err)
		}
		if scratchPVC != nil {
			t.Error("expected no scratch PVC to be created for a Block-mode target")
		}
	})
}

func TestHandlePreparedDataDownload(t *testing.T) {
	t.Run("existing pod transitions to InProgress without creating a new one", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		existingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, existingPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 1 {
			t.Errorf("expected exactly 1 pod (no new one created), got %d", len(pods.Items))
		}
	})

	t.Run("happy path creates downloader pod and transitions to InProgress", func(t *testing.T) {
		f := newDDTestFixture(t)
		// handlePrepared reads the disk name resolved by handleAccepted in an
		// earlier reconcile; set it explicitly since this test calls handlePrepared
		// directly, bypassing Accepted.
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		scheme := ddScheme()
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, scratchPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress, updated.Status.Message)
		}

		pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
		if err != nil {
			t.Fatalf("failed to find pod: %v", err)
		}
		if pod == nil {
			t.Fatal("expected downloader pod to be created")
		}
		if pod.Labels[common.LabelDatamoverPod] != "download" {
			t.Errorf("pod label %s = %q, want %q", common.LabelDatamoverPod, pod.Labels[common.LabelDatamoverPod], "download")
		}
		if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != f.dd.Name {
			t.Errorf("expected owner reference to DataDownload %q, got %+v", f.dd.Name, pod.OwnerReferences)
		}

		// The pod's target-volume env var must carry the resolved KubeVirt disk
		// name ("disk1"), not the target PVC name ("restored-disk-1") -- these
		// deliberately differ in this fixture to prove resolveTargetDiskName's
		// translation is what actually reaches the pod, not the raw PVC name.
		var targetVolumeEnv string
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == downloader.EnvTargetVolume {
				targetVolumeEnv = env.Value
				break
			}
		}
		if targetVolumeEnv != "disk1" {
			t.Errorf("env %s = %q, want %q", downloader.EnvTargetVolume, targetVolumeEnv, "disk1")
		}
		if targetVolumeEnv == "restored-disk-1" {
			t.Error("env carries the target PVC name instead of the resolved disk name")
		}
	})
}

func TestHandleInProgressDataDownload(t *testing.T) {
	t.Run("already provisioned in a prior reconcile completes idempotently without a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// Simulates: a prior reconcile's rebindPVToNamespace succeeded (scratch PVC
		// deleted, PV's claimRef patched to point at the target PVC, PV labeled
		// with our UID) but the subsequent updatePhase(Completed) failed to
		// persist, so this reconcile starts from InProgress again with no pod and
		// no scratch PVC left. The target PVC's own Status.Phase is deliberately
		// left Pending (not Bound) to prove isRestoreAlreadyProvisioned detects
		// the committed rebind via the PV's claimRef, not the PVC's Bound status
		// (which the Kubernetes PV controller only sets asynchronously).
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-already-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Kind:      "PersistentVolumeClaim",
					Name:      f.targetPVC.Name,
					Namespace: f.targetPVC.Namespace,
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
		}
	})

	t.Run("claimRef UID mismatch does not count as already provisioned", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// The claimRef names the right PVC/namespace, but a different UID -- as if
		// the target PVC had been deleted and recreated after this PV's claimRef was
		// set. isRestoreAlreadyProvisioned must not treat this as the same restore.
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-already-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Kind:      "PersistentVolumeClaim",
					Name:      f.targetPVC.Name,
					Namespace: f.targetPVC.Namespace,
					UID:       types.UID("some-other-uid"),
				},
			},
		}
		f.targetPVC.UID = types.UID("actual-target-pvc-uid")

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		done, err := r.isRestoreAlreadyProvisioned(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if done {
			t.Error("expected isRestoreAlreadyProvisioned = false for a claimRef UID mismatch")
		}
	})

	t.Run("pod not found fails", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("pod failed transitions to Failed with message", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "qemu-img failed"}}},
				},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if updated.Status.Message == "" {
			t.Error("expected non-empty failure message")
		}
	})

	t.Run("pod running requeues", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue while pod is running")
		}
	})

	t.Run("pod succeeded rebinds scratch volume to target PVC and completes", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// Target PVC starts Pending/unbound (the realistic Velero-created placeholder
		// state) so this test exercises the real validateExistingPVCForBind mutation path
		// (retain, delete scratch PVC, patch claimRef, wait for bound) rather than
		// the already-bound idempotent short-circuit.
		origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
		pvRebindPollInterval = 10 * time.Millisecond
		pvRebindTimeout = 2 * time.Second
		defer func() {
			pvRebindPollInterval = origInterval
			pvRebindTimeout = origTimeout
		}()

		scratchPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				StorageClassName:              "standard",
				VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
			},
		}
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-scratch",
				StorageClassName: new("standard"),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC, pod).
			Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		// Fake clients don't run the real Kubernetes PV binder, so simulate it.
		defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
		}

		var pv corev1.PersistentVolume
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
			t.Fatalf("failed to get PV: %v", err)
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != f.targetPVC.Name || pv.Spec.ClaimRef.Namespace != f.targetPVC.Namespace {
			t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, f.targetPVC.Namespace, f.targetPVC.Name)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected downloader pod to be cleaned up after completion, found %d", len(pods.Items))
		}
	})
}

func TestHandleCancelingDataDownload(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, scratchPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
	}

	var pods corev1.PodList
	_ = fakeClient.List(context.Background(), &pods)
	if len(pods.Items) != 0 {
		t.Errorf("expected pod to be cleaned up, found %d", len(pods.Items))
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = fakeClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 0 {
		t.Errorf("expected scratch PVC to be cleaned up, found %d", len(pvcs.Items))
	}
}

func TestBuildDownloaderPodConfig(t *testing.T) {
	f := newDDTestFixture(t)
	f.bsl.Spec.Provider = "azure"
	f.bsl.Spec.Config = map[string]string{
		"resourceGroup":               "my-rg",
		"storageAccount":              "my-account",
		"storageAccountKeyEnvVar":     "AZURE_STORAGE_KEY",
		"storageAccountURI":           "https://my-account.blob.core.windows.net",
		"subscriptionId":              "sub-id",
		"useAAD":                      "true",
		"activeDirectoryAuthorityURI": "https://login.microsoftonline.com",
	}
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}
	vmRef := &common.VMReference{Name: "test-vm", Namespace: "vm-ns"}

	cfg, err := r.buildDownloaderPodConfig(f.dd, f.bsl, vmRef, "scratch-pvc-1", "disk1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ScratchPVCName != "scratch-pvc-1" {
		t.Errorf("ScratchPVCName = %q, want %q", cfg.ScratchPVCName, "scratch-pvc-1")
	}
	if cfg.TargetVolume != "disk1" {
		t.Errorf("TargetVolume = %q, want %q", cfg.TargetVolume, "disk1")
	}
	if cfg.BSLResourceGroup != "my-rg" {
		t.Errorf("BSLResourceGroup = %q, want %q", cfg.BSLResourceGroup, "my-rg")
	}
	if cfg.BSLStorageAccount != "my-account" {
		t.Errorf("BSLStorageAccount = %q, want %q", cfg.BSLStorageAccount, "my-account")
	}
	if cfg.BSLStorageAccountKeyEnvVar != "AZURE_STORAGE_KEY" {
		t.Errorf("BSLStorageAccountKeyEnvVar = %q, want %q", cfg.BSLStorageAccountKeyEnvVar, "AZURE_STORAGE_KEY")
	}
	if cfg.BSLStorageAccountURI != "https://my-account.blob.core.windows.net" {
		t.Errorf("BSLStorageAccountURI = %q, want %q", cfg.BSLStorageAccountURI, "https://my-account.blob.core.windows.net")
	}
	if cfg.BSLSubscriptionID != "sub-id" {
		t.Errorf("BSLSubscriptionID = %q, want %q", cfg.BSLSubscriptionID, "sub-id")
	}
	if cfg.BSLUseAAD != "true" {
		t.Errorf("BSLUseAAD = %q, want %q", cfg.BSLUseAAD, "true")
	}
	if cfg.BSLActiveDirectoryAuthorityURI != "https://login.microsoftonline.com" {
		t.Errorf("BSLActiveDirectoryAuthorityURI = %q, want %q", cfg.BSLActiveDirectoryAuthorityURI, "https://login.microsoftonline.com")
	}
}
