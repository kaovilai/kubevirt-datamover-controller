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
	"io"
	"time"

	"github.com/go-logr/logr"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
)

// DefaultOperationTimeout bounds how long a DataUpload/DataDownload may remain
// in a non-terminal phase after being Accepted when Spec.OperationTimeout is
// unset or zero. Matches Velero server's own default item-operation-timeout.
const DefaultOperationTimeout = 4 * time.Hour

// immediateRequeueDelay is used by capRequeueToOperationDeadline in place of a
// zero RequeueAfter: returning ctrl.Result{RequeueAfter: 0} with a nil error
// is treated by controller-runtime as "don't requeue," not "requeue now."
const immediateRequeueDelay = time.Second

// operationTimeoutExceeded reports whether the time elapsed since acceptedAt
// exceeds the effective operation timeout: specTimeout when positive, otherwise
// DefaultOperationTimeout. Returns exceeded=false if acceptedAt is nil (nothing
// to measure against yet).
func operationTimeoutExceeded(acceptedAt *metav1.Time, specTimeout time.Duration) (exceeded bool, elapsed, effective time.Duration) {
	if acceptedAt == nil {
		return false, 0, 0
	}
	effective = specTimeout
	if effective <= 0 {
		effective = DefaultOperationTimeout
	}
	elapsed = time.Since(acceptedAt.Time)
	return elapsed >= effective, elapsed, effective
}

// capRequeueToOperationDeadline caps result.RequeueAfter so a phase handler's
// own requeue delay (e.g. RequeueAfterLong) can never push the next reconcile
// past the operation's timeout deadline -- otherwise a short Spec.OperationTimeout
// could be overshot by however long the handler's own poll interval is before
// checkOperationTimeout gets a chance to re-evaluate it.
func capRequeueToOperationDeadline(result ctrl.Result, acceptedAt *metav1.Time, specTimeout time.Duration) ctrl.Result {
	if result.RequeueAfter <= 0 || acceptedAt == nil {
		return result
	}
	_, elapsed, effective := operationTimeoutExceeded(acceptedAt, specTimeout)
	remaining := effective - elapsed
	switch {
	case remaining <= 0:
		// The deadline has already passed -- e.g. the phase handler itself took
		// long enough to run that it crossed the deadline after checkOperationTimeout
		// last evaluated it. Requeue almost immediately instead of preserving the
		// handler's original (possibly long) delay, so the next reconcile can fail
		// it right away rather than waiting out a stale poll interval.
		result.RequeueAfter = immediateRequeueDelay
	case remaining < result.RequeueAfter:
		result.RequeueAfter = remaining
	}
	return result
}

// getBackupStorageLocation fetches the BSL by name from the OADP namespace,
// falling back to fallbackNamespace if oadpNamespace is empty.
func getBackupStorageLocation(ctx context.Context, k8sClient client.Client, bslName, oadpNamespace, fallbackNamespace string) (*velerov1.BackupStorageLocation, error) {
	if bslName == "" {
		return nil, fmt.Errorf("no BackupStorageLocation name specified")
	}

	namespace := oadpNamespace
	if namespace == "" {
		namespace = fallbackNamespace
	}

	bsl := &velerov1.BackupStorageLocation{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: bslName, Namespace: namespace}, bsl); err != nil {
		return nil, fmt.Errorf("failed to get BackupStorageLocation %s/%s: %w", namespace, bslName, err)
	}

	return bsl, nil
}

// findPodByUID finds the unique datamover pod associated with a resource UID.
func findPodByUID(ctx context.Context, k8sClient client.Client, uidLabelKey, uid, namespace string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		return nil, err
	}
	if len(podList.Items) == 0 {
		return nil, nil
	}
	if len(podList.Items) > 1 {
		return nil, fmt.Errorf("found multiple datamover pods in namespace %s with label %s=%s", namespace, uidLabelKey, uid)
	}
	return &podList.Items[0], nil
}

// cleanupPodsByUID deletes all pods matching a UID label in the given namespace.
func cleanupPodsByUID(ctx context.Context, k8sClient client.Client, uidLabelKey, uid, namespace string, logger logr.Logger) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		logger.Error(err, "Failed to list datamover pods for cleanup")
		return
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if err := k8sClient.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete datamover pod", "pod", pod.Name)
		} else {
			logger.Info("Deleted datamover pod", "pod", pod.Name)
		}
	}
}

// extractPodFailureMessage extracts the failure message from a failed pod.
func extractPodFailureMessage(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}

	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Status == corev1.ConditionFalse && cond.Message != "" {
			return cond.Message
		}
	}

	return "unknown error"
}

// safeGenerateNamePrefix truncates a GenerateName prefix so that the final
// name (prefix + 5 random chars) does not exceed maxNameLen.
func safeGenerateNamePrefix(prefix string, maxNameLen int) string {
	maxPrefix := max(maxNameLen-k8sGenerateNameRandomLen, 1)
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	return prefix
}

// addOverhead returns qty increased by the given percentage.
// For example, addOverhead(30Gi, 20) returns 36Gi.
func addOverhead(qty resource.Quantity, percent int64) resource.Quantity {
	base := qty.Value()
	overhead := base * percent / 100
	result := resource.NewQuantity(base+overhead, resource.BinarySI)
	return *result
}

// getVeleroBackupName extracts the Velero backup name from resource labels.
func getVeleroBackupName(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels[common.LabelVeleroBackupName]
}

// NewPodLogCollector returns a PodLogCollector function that reads the last
// tailLines of log output from a pod using the Kubernetes API.
func NewPodLogCollector(clientset kubernetes.Interface, tailLines int64) func(ctx context.Context, podName, podNamespace string) (string, error) {
	return func(ctx context.Context, podName, podNamespace string) (string, error) {
		opts := &corev1.PodLogOptions{
			TailLines: &tailLines,
		}
		stream, err := clientset.CoreV1().Pods(podNamespace).GetLogs(podName, opts).Stream(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to stream pod logs: %w", err)
		}
		defer func() { _ = stream.Close() }()
		data, err := io.ReadAll(stream)
		if err != nil {
			return "", fmt.Errorf("failed to read pod logs: %w", err)
		}
		return string(data), nil
	}
}
