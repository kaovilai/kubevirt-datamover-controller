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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestOperationTimeoutExceeded(t *testing.T) {
	tests := []struct {
		name          string
		acceptedAt    *metav1.Time
		specTimeout   time.Duration
		wantExceeded  bool
		wantEffective time.Duration
	}{
		{
			name:          "nil acceptedAt never exceeds",
			acceptedAt:    nil,
			specTimeout:   time.Second,
			wantExceeded:  false,
			wantEffective: 0,
		},
		{
			name:          "unset spec timeout falls back to default and has not exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-time.Minute)),
			specTimeout:   0,
			wantExceeded:  false,
			wantEffective: DefaultOperationTimeout,
		},
		{
			name:          "unset spec timeout falls back to default and has exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			specTimeout:   0,
			wantExceeded:  true,
			wantEffective: DefaultOperationTimeout,
		},
		{
			name:          "custom spec timeout not yet exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-time.Second)),
			specTimeout:   time.Hour,
			wantExceeded:  false,
			wantEffective: time.Hour,
		},
		{
			name:          "custom spec timeout exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-2 * time.Hour)),
			specTimeout:   time.Hour,
			wantExceeded:  true,
			wantEffective: time.Hour,
		},
		{
			name:          "negative spec timeout falls back to default",
			acceptedAt:    ptrTime(time.Now().Add(-time.Minute)),
			specTimeout:   -1,
			wantExceeded:  false,
			wantEffective: DefaultOperationTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exceeded, _, effective := operationTimeoutExceeded(tt.acceptedAt, tt.specTimeout)
			if exceeded != tt.wantExceeded {
				t.Errorf("exceeded = %v, want %v", exceeded, tt.wantExceeded)
			}
			if effective != tt.wantEffective {
				t.Errorf("effective = %v, want %v", effective, tt.wantEffective)
			}
		})
	}
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

func TestCapRequeueToOperationDeadline(t *testing.T) {
	tests := []struct {
		name           string
		result         ctrl.Result
		acceptedAt     *metav1.Time
		specTimeout    time.Duration
		wantRequeueMin time.Duration
		wantRequeueMax time.Duration
	}{
		{
			name:           "zero RequeueAfter is left alone",
			result:         ctrl.Result{},
			acceptedAt:     ptrTime(time.Now()),
			specTimeout:    time.Hour,
			wantRequeueMin: 0,
			wantRequeueMax: 0,
		},
		{
			name:           "nil acceptedAt is left alone",
			result:         ctrl.Result{RequeueAfter: 30 * time.Second},
			acceptedAt:     nil,
			specTimeout:    time.Hour,
			wantRequeueMin: 30 * time.Second,
			wantRequeueMax: 30 * time.Second,
		},
		{
			name:           "plenty of time left, delay unchanged",
			result:         ctrl.Result{RequeueAfter: 5 * time.Second},
			acceptedAt:     ptrTime(time.Now().Add(-time.Second)),
			specTimeout:    time.Hour,
			wantRequeueMin: 5 * time.Second,
			wantRequeueMax: 5 * time.Second,
		},
		{
			name:           "delay capped to the remaining deadline",
			result:         ctrl.Result{RequeueAfter: 30 * time.Second},
			acceptedAt:     ptrTime(time.Now().Add(-9 * time.Second)),
			specTimeout:    10 * time.Second,
			wantRequeueMin: 1,
			wantRequeueMax: time.Second,
		},
		{
			name:           "deadline already elapsed requeues almost immediately, not with the stale long delay",
			result:         ctrl.Result{RequeueAfter: 30 * time.Second},
			acceptedAt:     ptrTime(time.Now().Add(-2 * time.Hour)),
			specTimeout:    time.Hour,
			wantRequeueMin: 1,
			wantRequeueMax: immediateRequeueDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := capRequeueToOperationDeadline(tt.result, tt.acceptedAt, tt.specTimeout)
			if got.RequeueAfter < tt.wantRequeueMin || got.RequeueAfter > tt.wantRequeueMax {
				t.Errorf("RequeueAfter = %v, want in range [%v, %v]", got.RequeueAfter, tt.wantRequeueMin, tt.wantRequeueMax)
			}
		})
	}
}
