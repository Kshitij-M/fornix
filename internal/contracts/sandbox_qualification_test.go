package contracts

import (
	"testing"
	"time"
)

func TestSandboxQualificationEvidenceRequiresMeasuredBoundedAndCurrentProof(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	valid := SandboxQualificationEvidence{
		ID: "qualification-1", Backend: SandboxBackendOCI,
		Runtime: SandboxRuntimeIdentity{
			ProviderBuildHash: HashStrings("provider"), RuntimeBuildHash: HashStrings("runtime"),
			RuntimeConfigurationHash: HashStrings("configuration"), ImageDigest: "sha256:" + repeatHex("a", 64),
			ImagePlatform: SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
		},
		CapabilityHash: HashStrings("controls"), Limits: SandboxLimitEnvelope{
			TimeoutMS: int(MaxToolTimeout / time.Millisecond), MaxStdoutBytes: MaxToolOutputBytes,
			MaxStderrBytes: MaxToolOutputBytes, MaxArgCount: MaxToolArgCount, MaxArgBytes: MaxToolArgBytes,
			MaxEnvEntries: MaxToolEnvCount, MaxEnvBytes: MaxToolEnvBytes, CPUQuotaMilli: MaxToolCPUQuotaMilli,
			MemoryBytes: MaxToolMemoryBytes, PIDsLimit: MaxToolProcessCount, ScratchBytes: MaxToolScratchBytes,
		}, TestSuiteHash: HashStrings("suite"),
		Outcome: QualificationOutcomePassed, Measured: true,
		ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	if err := valid.NormalizeAt(now); err != nil {
		t.Fatalf("normalize valid measured proof: %v", err)
	}
	if valid.StableHash() == "" {
		t.Fatal("valid proof has no stable identity")
	}
	changedPlatform := valid
	changedPlatform.Runtime.ImagePlatform = SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
	if changedPlatform.StableHash() == valid.StableHash() {
		t.Fatal("changing the signed runtime platform did not change qualification evidence identity")
	}
	tests := []struct {
		name   string
		change func(*SandboxQualificationEvidence)
		now    time.Time
	}{
		{name: "unmeasured", change: func(e *SandboxQualificationEvidence) { e.Measured = false }, now: now},
		{name: "failed outcome", change: func(e *SandboxQualificationEvidence) { e.Outcome = QualificationOutcomeFailed }, now: now},
		{name: "expired", change: func(e *SandboxQualificationEvidence) { e.ExpiresAt = now }, now: now},
		{name: "future observation", change: func(e *SandboxQualificationEvidence) { e.ObservedAt = now.Add(time.Second) }, now: now},
		{name: "overlong validity", change: func(e *SandboxQualificationEvidence) { e.ExpiresAt = e.ObservedAt.Add(366 * 24 * time.Hour) }, now: now},
		{name: "mutable image tag", change: func(e *SandboxQualificationEvidence) { e.Runtime.ImageDigest = "latest" }, now: now},
		{name: "missing platform", change: func(e *SandboxQualificationEvidence) { e.Runtime.ImagePlatform = SandboxImagePlatform{} }, now: now},
		{name: "old evidence schema", change: func(e *SandboxQualificationEvidence) { e.SchemaVersion = 1 }, now: now},
		{name: "local backend", change: func(e *SandboxQualificationEvidence) { e.Backend = SandboxBackendLocalProcess }, now: now},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.change(&candidate)
			if err := candidate.NormalizeAt(test.now); err == nil {
				t.Fatal("invalid sandbox qualification evidence was accepted")
			}
		})
	}
}

func TestToolRequestIdentityIncludesSandboxQualification(t *testing.T) {
	request := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "sandbox-proof", ToolID: "test.tool", Argv: []string{"/bin/echo", "hello"}, Budget: DefaultSandboxProfile()}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	request.SandboxQualificationHash = HashStrings("qualified runtime")
	first, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.SandboxQualificationHash = HashStrings("different qualified runtime")
	second, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("changing sandbox qualification did not change the durable tool request identity")
	}
}

func TestSandboxLimitEnvelopeRejectsProfilesBeyondMeasuredBounds(t *testing.T) {
	profile := SandboxProfile{
		Backend: string(SandboxBackendOCI), TimeoutMS: 2_000,
		MaxStdoutBytes: 1_024, MaxStderrBytes: 1_024, MaxArgCount: 8, MaxArgBytes: 128,
		MaxEnvEntries: 4, MaxEnvBytes: 1_024, CPUQuotaMilli: 500, MemoryBytes: 256 << 20,
		PIDsLimit: 32, ScratchBytes: 64 << 20, ImageDigest: "sha256:" + repeatHex("a", 64),
		ImagePlatform:  SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
		ReadOnlyRootFS: true, ReadOnlyWorkdir: true,
	}
	envelope := SandboxLimitEnvelope{
		TimeoutMS: 4_000, MaxStdoutBytes: 2_048, MaxStderrBytes: 2_048, MaxArgCount: 16,
		MaxArgBytes: 256, MaxEnvEntries: 8, MaxEnvBytes: 2_048, CPUQuotaMilli: 1_000,
		MemoryBytes: 512 << 20, PIDsLimit: 64, ScratchBytes: 128 << 20,
	}
	if !envelope.Supports(profile) {
		t.Fatal("profile within the measured envelope was rejected")
	}
	envelope.MemoryBytes = profile.MemoryBytes - 1
	if envelope.Supports(profile) {
		t.Fatal("profile exceeding measured memory bounds was admitted")
	}
}

func repeatHex(value string, count int) string {
	result := make([]byte, count)
	for index := range result {
		result[index] = value[0]
	}
	return string(result)
}
