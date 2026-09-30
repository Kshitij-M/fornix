package contracts

import (
	"fmt"
	"strings"
)

// SandboxImagePlatform is the canonical platform expected from a pinned
// non-local sandbox image. It is part of the profile and signed runtime
// identity; callers must not infer it from the runner host.
type SandboxImagePlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

// Normalize canonicalizes and validates the supported OCI image platform
// tuple. The current sandbox profiles intentionally support Linux images
// only; architecture variants are accepted only where they have a defined
// meaning in the project's supported platform vocabulary.
func (p *SandboxImagePlatform) Normalize() error {
	if p == nil {
		return fmt.Errorf("sandbox image platform is nil")
	}
	p.OS = strings.ToLower(strings.TrimSpace(p.OS))
	p.Architecture = strings.ToLower(strings.TrimSpace(p.Architecture))
	p.Variant = strings.ToLower(strings.TrimSpace(p.Variant))
	if p.OS != "linux" {
		return fmt.Errorf("sandbox image platform OS must be linux")
	}
	switch p.Architecture {
	case "386", "amd64", "arm64", "ppc64le", "riscv64", "s390x":
		if p.Variant != "" && !(p.Architecture == "arm64" && p.Variant == "v8") {
			return fmt.Errorf("sandbox image platform variant is unsupported for architecture %q", p.Architecture)
		}
	case "arm":
		if p.Variant != "v5" && p.Variant != "v6" && p.Variant != "v7" {
			return fmt.Errorf("sandbox arm image platform requires variant v5, v6, or v7")
		}
	default:
		return fmt.Errorf("sandbox image platform architecture is unsupported")
	}
	return nil
}

// IsZero reports whether no platform has been supplied. It is used to keep
// local-process profiles free of container-only image identity.
func (p SandboxImagePlatform) IsZero() bool {
	return p.OS == "" && p.Architecture == "" && p.Variant == ""
}

// String returns the normalized OCI platform spelling for safe diagnostics
// and Engine option mapping; it contains no host path or credential material.
func (p SandboxImagePlatform) String() string {
	if p.IsZero() {
		return ""
	}
	value := p.OS + "/" + p.Architecture
	if p.Variant != "" {
		value += "/" + p.Variant
	}
	return value
}
