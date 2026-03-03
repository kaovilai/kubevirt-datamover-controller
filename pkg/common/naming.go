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
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// MaxResourceNameLen is the maximum length for Kubernetes resource names
	// (DNS-1123 subdomain).
	MaxResourceNameLen = 253

	// MaxLabelValueLen is the maximum length for Kubernetes label values.
	MaxLabelValueLen = 63

	// hashLen is the length of the truncation hash suffix (8 hex chars).
	hashLen = 8
)

// SafeResourceName returns a Kubernetes-safe resource name by combining
// prefix and name. If the result exceeds 253 chars, the name portion is
// truncated and a deterministic 8-char hash suffix is appended.
//
// The hash is derived from the original (untruncated) name, so the same
// input always produces the same output (deterministic — safe for Get-by-name).
func SafeResourceName(prefix, name string) string {
	fullName := prefix + name
	if len(fullName) <= MaxResourceNameLen {
		return fullName
	}
	return truncateWithHash(prefix, name, MaxResourceNameLen)
}

// SafeLabelValue returns a label value that is ≤ 63 chars.
// If the value exceeds 63 chars, it is truncated with a deterministic
// 8-char hash suffix to preserve uniqueness for lookups.
func SafeLabelValue(value string) string {
	if len(value) <= MaxLabelValueLen {
		return value
	}
	return truncateWithHash("", value, MaxLabelValueLen)
}

// HashForLabel returns a fixed-length (16 hex char) hash of the input,
// suitable for use as a label value for grouping/lookup.
// The same input always produces the same output.
func HashForLabel(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:8]) // 8 bytes → 16 hex chars
}

// truncateWithHash truncates prefix+name to fit within maxLen,
// appending a deterministic hash suffix derived from the original name.
func truncateWithHash(prefix, name string, maxLen int) string {
	hash := sha256.Sum256([]byte(name))
	hashSuffix := hex.EncodeToString(hash[:hashLen/2]) // 4 bytes → 8 hex chars

	// Ensure we have room for at least the hash suffix
	minRequired := hashLen // just the hash if nothing else fits
	if len(prefix) > maxLen-minRequired {
		// Prefix itself is too long — truncate it to make room for hash
		prefix = prefix[:maxLen-minRequired]
		prefix = strings.TrimRight(prefix, "-.")
		return prefix + hashSuffix
	}

	// Available space for the name portion:
	// maxLen - len(prefix) - 1 (separator "-") - hashLen
	available := max(maxLen-len(prefix)-1-hashLen, 0)

	truncated := name
	if len(name) > available {
		truncated = name[:available]
	}
	// Trim trailing '-' or '.' to keep the name DNS-valid
	truncated = strings.TrimRight(truncated, "-.")

	if truncated == "" {
		return prefix + hashSuffix
	}
	return prefix + truncated + "-" + hashSuffix
}
