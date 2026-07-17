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

package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestOriginalReclaimPolicyOf(t *testing.T) {
	tests := []struct {
		name string
		pv   *corev1.PersistentVolume
		want corev1.PersistentVolumeReclaimPolicy
	}{
		{
			name: "not yet Retain -- spec value is authoritative regardless of annotation",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete},
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					pvOriginalReclaimPolicyAnnotation: string(corev1.PersistentVolumeReclaimRecycle),
				}},
			},
			want: corev1.PersistentVolumeReclaimDelete,
		},
		{
			name: "Retain with recorded annotation returns the recorded original",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain},
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					pvOriginalReclaimPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
				}},
			},
			want: corev1.PersistentVolumeReclaimDelete,
		},
		{
			name: "Retain with no annotation falls back to the (already-Retain) spec value",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain},
			},
			want: corev1.PersistentVolumeReclaimRetain,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := originalReclaimPolicyOf(tt.pv); got != tt.want {
				t.Errorf("originalReclaimPolicyOf() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateExistingPVCForBind(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	basePV := func(mutate func(*corev1.PersistentVolume)) *corev1.PersistentVolume {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: "standard",
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			},
		}
		if mutate != nil {
			mutate(pv)
		}
		return pv
	}

	basePVC := func(mutate func(*corev1.PersistentVolumeClaim)) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "target-pvc", Namespace: "restore-ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: new("standard"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimPending,
			},
		}
		if mutate != nil {
			mutate(pvc)
		}
		return pvc
	}

	tests := []struct {
		name          string
		pv            *corev1.PersistentVolume
		pvc           *corev1.PersistentVolumeClaim
		skipCreatePVC bool
		expectError   bool
		errorContains string
	}{
		{
			name:          "target PVC not found",
			pv:            basePV(nil),
			skipCreatePVC: true,
			expectError:   true,
			errorContains: "not found",
		},
		{
			name: "already bound to this PV short-circuits without spec validation",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimBound
				p.Spec.VolumeName = "pv-1"
				// Deliberately incompatible spec -- should not matter since already bound to pv-1.
				p.Spec.StorageClassName = new("mismatched")
			}),
			expectError: false,
		},
		{
			name: "already bound to a different PV fails",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimBound
				p.Spec.VolumeName = "some-other-pv"
			}),
			expectError:   true,
			errorContains: "already bound to PV",
		},
		{
			name: "pending PVC already requests a conflicting volume name",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimPending
				p.Spec.VolumeName = "some-other-pv"
			}),
			expectError:   true,
			errorContains: "already requests volume",
		},
		{
			name: "storageClassName mismatch",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.StorageClassName = new("other-class")
			}),
			expectError:   true,
			errorContains: "storageClassName",
		},
		{
			name: "requested capacity exceeds PV capacity",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
			}),
			expectError:   true,
			errorContains: "exceeds",
		},
		{
			name: "volumeMode mismatch",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
			}),
			expectError:   true,
			errorContains: "volumeMode",
		},
		{
			name: "access modes disjoint",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			}),
			expectError:   true,
			errorContains: "access mode",
		},
		{
			name: "target PVC being deleted fails",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				p.Finalizers = []string{"kubernetes.io/pvc-protection"}
			}),
			expectError:   true,
			errorContains: "being deleted",
		},
		{
			name:        "compatible PVC and PV",
			pv:          basePV(nil),
			pvc:         basePVC(nil),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if !tt.skipCreatePVC && tt.pvc != nil {
				builder = builder.WithObjects(tt.pvc)
			}
			fakeClient := builder.Build()

			result, err := validateExistingPVCForBind(context.Background(), fakeClient, logr.Discard(), tt.pv, "restore-ns", "target-pvc")

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("error = %q, want to contain %q", err.Error(), tt.errorContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil PVC result")
			}
			if result.Name != "target-pvc" {
				t.Errorf("result.Name = %q, want %q", result.Name, "target-pvc")
			}
		})
	}
}

func TestRebindPVToNamespace_EmptyExistingPVCName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := rebindPVToNamespace(
		context.Background(), fakeClient, logr.Discard(),
		"source-pvc", "oadp-ns", "restore-ns",
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, "",
	)
	if err == nil {
		t.Fatal("expected error for empty existingPVCName, got nil")
	}
	if !strings.Contains(err.Error(), "existingPVCName") {
		t.Errorf("error = %q, want to mention existingPVCName", err.Error())
	}
}

// TestRebindPVToNamespace_BindTargetExisting exercises the full rebindPVToNamespace
// flow in BindTargetExisting mode, including the real waitForPVCBound poll loop
// (not just the already-bound short-circuit tested above). A background goroutine
// flips the target PVC to Bound shortly after the patch, simulating what a real
// Kubernetes PV controller would do once claimRef is set -- fake clients don't run
// that controller, so the test drives it manually.
func TestRebindPVToNamespace_BindTargetExisting(t *testing.T) {
	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	// Snapshotted before the goroutine starts so the goroutine's deadline can't be
	// affected by the deferred restore above running concurrently with it.
	pvRebindTimeout := pvRebindTimeout
	binderDone := make(chan struct{})
	var statusUpdateErr error
	defer func() {
		// Wait for the fake-binder goroutine to finish before the vars-restore
		// defer above runs, so it never observes restored (non-test) values and
		// never outlives this test function.
		select {
		case <-binderDone:
			if statusUpdateErr != nil {
				t.Errorf("fake-binder goroutine failed to update PVC status: %v", statusUpdateErr)
			}
		case <-time.After(pvRebindTimeout + time.Second):
			t.Log("timed out waiting for fake-binder goroutine to finish")
		}
	}()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "scratch-pvc", Namespace: "oadp-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	targetPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk", Namespace: "restore-ns"},
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

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourcePV, sourcePVC, targetPVC).
		Build()

	go func() {
		// rebindPVToNamespace's patchPVBinding only sets the PV's claimRef -- a
		// real Kubernetes PV controller then completes the bind by setting the
		// PVC's Spec.VolumeName and Status.Phase, which the fake client won't do
		// on its own. Poll for the claimRef (what the code under test actually
		// writes) before simulating the rest of that binder's job, rather than a
		// fixed sleep that could race ahead of the patch.
		defer close(binderDone)
		deadline := time.Now().Add(pvRebindTimeout)
		for time.Now().Before(deadline) {
			pv := &corev1.PersistentVolume{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, pv); err != nil {
				statusUpdateErr = fmt.Errorf("get PV: %w", err)
				return
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == targetPVC.Name && pv.Spec.ClaimRef.Namespace == targetPVC.Namespace {
				pvc := &corev1.PersistentVolumeClaim{}
				if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, pvc); err != nil {
					statusUpdateErr = fmt.Errorf("get target PVC: %w", err)
					return
				}
				pvc.Spec.VolumeName = sourcePV.Name
				if err := fakeClient.Update(context.Background(), pvc); err != nil {
					statusUpdateErr = fmt.Errorf("update target PVC: %w", err)
					return
				}
				pvc.Status.Phase = corev1.ClaimBound
				statusUpdateErr = fakeClient.Status().Update(context.Background(), pvc)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		statusUpdateErr = fmt.Errorf("timed out waiting for PV %s claimRef to name target PVC %s/%s", sourcePV.Name, targetPVC.Namespace, targetPVC.Name)
	}()

	result, err := rebindPVToNamespace(
		context.Background(), fakeClient, logr.Discard(),
		sourcePVC.Name, sourcePVC.Namespace, targetPVC.Namespace,
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, targetPVC.Name,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewPVCName != targetPVC.Name {
		t.Errorf("NewPVCName = %q, want %q", result.NewPVCName, targetPVC.Name)
	}
	if result.NewPVCNamespace != targetPVC.Namespace {
		t.Errorf("NewPVCNamespace = %q, want %q", result.NewPVCNamespace, targetPVC.Namespace)
	}
	if result.PVName != sourcePV.Name {
		t.Errorf("PVName = %q, want %q", result.PVName, sourcePV.Name)
	}
	if result.OriginalReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("OriginalReclaimPolicy = %q, want %q", result.OriginalReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	}

	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("PV reclaim policy = %q, want %q", pv.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimRetain)
	}
	if got := pv.Annotations[pvOriginalReclaimPolicyAnnotation]; got != string(corev1.PersistentVolumeReclaimDelete) {
		t.Errorf("original-reclaim-policy annotation = %q, want %q", got, corev1.PersistentVolumeReclaimDelete)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != targetPVC.Name || pv.Spec.ClaimRef.Namespace != targetPVC.Namespace {
		t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, targetPVC.Namespace, targetPVC.Name)
	}

	var destPVC corev1.PersistentVolumeClaim
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, &destPVC); err != nil {
		t.Fatalf("failed to get destination PVC: %v", err)
	}
	if destPVC.Spec.VolumeName != sourcePV.Name {
		t.Errorf("destination PVC Spec.VolumeName = %q, want %q", destPVC.Spec.VolumeName, sourcePV.Name)
	}
}
