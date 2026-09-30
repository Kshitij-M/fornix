package contracts

import (
	"encoding/json"
	"testing"
)

func TestSandboxImagePlatformNormalizesAndRejectsAmbiguousPlatforms(t *testing.T) {
	tests := []struct {
		name    string
		input   SandboxImagePlatform
		want    string
		wantErr bool
	}{
		{name: "canonical amd64", input: SandboxImagePlatform{OS: "linux", Architecture: "amd64"}, want: "linux/amd64"},
		{name: "arm variant", input: SandboxImagePlatform{OS: "LINUX", Architecture: "ARM", Variant: "V7"}, want: "linux/arm/v7"},
		{name: "arm64 v8", input: SandboxImagePlatform{OS: "linux", Architecture: "arm64", Variant: "v8"}, want: "linux/arm64/v8"},
		{name: "unsupported operating system", input: SandboxImagePlatform{OS: "windows", Architecture: "amd64"}, wantErr: true},
		{name: "missing architecture", input: SandboxImagePlatform{OS: "linux"}, wantErr: true},
		{name: "unknown architecture", input: SandboxImagePlatform{OS: "linux", Architecture: "made-up"}, wantErr: true},
		{name: "arm requires a variant", input: SandboxImagePlatform{OS: "linux", Architecture: "arm"}, wantErr: true},
		{name: "invalid arm variant", input: SandboxImagePlatform{OS: "linux", Architecture: "arm", Variant: "v8"}, wantErr: true},
		{name: "variant on amd64", input: SandboxImagePlatform{OS: "linux", Architecture: "amd64", Variant: "v3"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			platform := test.input
			err := platform.Normalize()
			if (err != nil) != test.wantErr {
				t.Fatalf("Normalize() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && platform.String() != test.want {
				t.Fatalf("normalized platform = %q, want %q", platform.String(), test.want)
			}
		})
	}
}

func TestSandboxPlatformIsRequiredOnlyForNonLocalProfilesAndBindsHashes(t *testing.T) {
	local := DefaultSandboxProfile()
	encoded, err := json.Marshal(local)
	if err != nil {
		t.Fatal(err)
	}
	var localFields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &localFields); err != nil {
		t.Fatal(err)
	}
	if _, exists := localFields["image_platform"]; exists {
		t.Fatal("zero platform changed legacy local profile serialization")
	}
	local.ImagePlatform = SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	if err := local.Normalize(); err == nil {
		t.Fatal("local-process profile accepted container image platform identity")
	}

	base := SandboxProfile{
		Backend: string(SandboxBackendOCI), TimeoutMS: 2_000,
		MaxStdoutBytes: 1_024, MaxStderrBytes: 1_024, MaxArgCount: 8, MaxArgBytes: 128,
		MaxEnvEntries: 0, MaxEnvBytes: 0, CPUQuotaMilli: 500, MemoryBytes: 256 << 20,
		PIDsLimit: 32, ScratchBytes: 64 << 20, ImageDigest: "sha256:" + repeatHex("a", 64),
		ReadOnlyRootFS: true, ReadOnlyWorkdir: true,
	}
	missing := base
	if err := missing.Normalize(); err == nil {
		t.Fatal("non-local profile without a typed platform was accepted")
	}

	base.ImagePlatform = SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	if err := base.Normalize(); err != nil {
		t.Fatalf("valid OCI profile rejected: %v", err)
	}
	baseHash, err := base.Hash()
	if err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.ImagePlatform = SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
	if err := changed.Normalize(); err != nil {
		t.Fatal(err)
	}
	changedHash, err := changed.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if baseHash == changedHash {
		t.Fatal("changing image platform did not change the profile hash")
	}
}
