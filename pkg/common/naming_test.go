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

package common

import (
	"strings"
	"testing"
)

func TestSafeResourceName(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		input      string
		wantExact  string // if set, check exact match
		wantMaxLen int    // if set, check max length
		wantPrefix string // if set, check prefix
	}{
		{
			name:      "short name passes through unchanged",
			prefix:    "kubevirt-dm-",
			input:     "test-du",
			wantExact: "kubevirt-dm-test-du",
		},
		{
			name:      "typical DataUpload name passes through",
			prefix:    "vmb-",
			input:     "du-my-backup-my-vm-a1b2c3d4",
			wantExact: "vmb-du-my-backup-my-vm-a1b2c3d4",
		},
		{
			name:       "name at exactly 253 chars passes through",
			prefix:     "vmb-",
			input:      strings.Repeat("a", 249),
			wantMaxLen: 253,
		},
		{
			name:       "name exceeding 253 chars is truncated with hash",
			prefix:     "kubevirt-dm-pvc-",
			input:      strings.Repeat("a", 250),
			wantMaxLen: 253,
			wantPrefix: "kubevirt-dm-pvc-",
		},
		{
			name:      "empty name with prefix",
			prefix:    "vmb-",
			input:     "",
			wantExact: "vmb-",
		},
		{
			name:       "very long prefix still produces valid result",
			prefix:     strings.Repeat("p", 250) + "-",
			input:      "name",
			wantMaxLen: 253,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeResourceName(tt.prefix, tt.input)

			if tt.wantExact != "" && got != tt.wantExact {
				t.Errorf("SafeResourceName(%q, %q) = %q, want %q", tt.prefix, tt.input, got, tt.wantExact)
			}

			if tt.wantMaxLen > 0 && len(got) > tt.wantMaxLen {
				t.Errorf("SafeResourceName(%q, %q) length = %d, want <= %d, got %q",
					tt.prefix, tt.input, len(got), tt.wantMaxLen, got)
			}

			if tt.wantPrefix != "" && !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("SafeResourceName(%q, %q) = %q, want prefix %q", tt.prefix, tt.input, got, tt.wantPrefix)
			}
		})
	}
}

func TestSafeResourceNameDeterministic(t *testing.T) {
	prefix := "kubevirt-dm-"
	name := strings.Repeat("long-name-", 30) // 300 chars
	result1 := SafeResourceName(prefix, name)
	result2 := SafeResourceName(prefix, name)
	if result1 != result2 {
		t.Errorf("SafeResourceName is not deterministic: %q != %q", result1, result2)
	}
}

func TestSafeResourceNameDNSValid(t *testing.T) {
	// Name ending with dashes should have them trimmed
	prefix := "vmb-"
	name := strings.Repeat("a", 250) + "---"
	result := SafeResourceName(prefix, name)

	if len(result) > MaxResourceNameLen {
		t.Errorf("result length %d exceeds %d", len(result), MaxResourceNameLen)
	}

	// Should not end with '-' or '.'
	if strings.HasSuffix(result, "-") || strings.HasSuffix(result, ".") {
		t.Errorf("result %q ends with invalid DNS character", result)
	}
}

func TestSafeLabelValue(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantExact  string
		wantMaxLen int
	}{
		{
			name:      "short value passes through",
			input:     "test-value",
			wantExact: "test-value",
		},
		{
			name:      "UUID passes through (36 chars)",
			input:     "550e8400-e29b-41d4-a716-446655440000",
			wantExact: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:      "exactly 63 chars passes through",
			input:     strings.Repeat("a", 63),
			wantExact: strings.Repeat("a", 63),
		},
		{
			name:       "64 chars is truncated",
			input:      strings.Repeat("a", 64),
			wantMaxLen: 63,
		},
		{
			name:       "90-char DataUpload name is truncated",
			input:      "du-ultra-very-long-backup-name-that-is-over-sixty-three-characters-cirros-test-cont-2-a9ed8aa7",
			wantMaxLen: 63,
		},
		{
			name:       "very long value is truncated",
			input:      strings.Repeat("x", 200),
			wantMaxLen: 63,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeLabelValue(tt.input)

			if tt.wantExact != "" && got != tt.wantExact {
				t.Errorf("SafeLabelValue(%q) = %q, want %q", tt.input, got, tt.wantExact)
			}

			if tt.wantMaxLen > 0 && len(got) > tt.wantMaxLen {
				t.Errorf("SafeLabelValue(%q) length = %d, want <= %d, got %q", tt.input, len(got), tt.wantMaxLen, got)
			}
		})
	}
}

func TestSafeLabelValueDeterministic(t *testing.T) {
	value := "du-ultra-very-long-backup-name-that-is-over-sixty-three-characters-cirros-test-cont-2-a9ed8aa7"
	result1 := SafeLabelValue(value)
	result2 := SafeLabelValue(value)
	if result1 != result2 {
		t.Errorf("SafeLabelValue is not deterministic: %q != %q", result1, result2)
	}
}

func TestSafeLabelValueDifferentInputs(t *testing.T) {
	// Two different long values should produce different results
	value1 := strings.Repeat("a", 100)
	value2 := strings.Repeat("b", 100)
	result1 := SafeLabelValue(value1)
	result2 := SafeLabelValue(value2)
	if result1 == result2 {
		t.Errorf("different inputs produced same result: %q", result1)
	}
}

func TestHashForLabel(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "short VM name", input: "my-vm"},
		{name: "long VM name", input: strings.Repeat("long-vm-name-", 10)},
		{name: "empty", input: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HashForLabel(tt.input)
			if len(got) != 16 {
				t.Errorf("HashForLabel(%q) length = %d, want 16", tt.input, len(got))
			}
			// Deterministic
			if got2 := HashForLabel(tt.input); got != got2 {
				t.Errorf("HashForLabel is not deterministic: %q != %q", got, got2)
			}
		})
	}

	// Different inputs produce different hashes
	h1 := HashForLabel("vm-1")
	h2 := HashForLabel("vm-2")
	if h1 == h2 {
		t.Errorf("different inputs produced same hash: %q", h1)
	}
}
