package qualification

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestVerifySandboxProofBindsSignerTargetWorkspaceRuntimeAndControls(t *testing.T) {
	proof, runtime, capabilities, trust := validSandboxProof(t)
	if err := VerifySandboxProof(proof, runtime, capabilities, trust); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}
	tests := []struct {
		name   string
		change func(*contracts.SandboxQualificationProof, *contracts.SandboxRuntimeIdentity, *contracts.SandboxBackendCapabilities, *SandboxTrust)
	}{
		{name: "wrong key id", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, trust *SandboxTrust) {
			trust.KeyID = "other-key"
		}},
		{name: "wrong public key", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, trust *SandboxTrust) {
			trust.PublicKey = testSandboxPublicKey(0x24)
		}},
		{name: "wrong target", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, trust *SandboxTrust) {
			trust.TargetHash = contracts.HashStrings("different deployment")
		}},
		{name: "wrong workspace", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, trust *SandboxTrust) {
			trust.WorkspaceID = "workspace-b"
		}},
		{name: "expired", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, trust *SandboxTrust) {
			trust.Now = func() time.Time { return time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC).Add(2 * time.Hour) }
		}},
		{name: "runtime drift", change: func(_ *contracts.SandboxQualificationProof, runtime *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, _ *SandboxTrust) {
			runtime.RuntimeBuildHash = contracts.HashStrings("different runtime build")
		}},
		{name: "platform drift", change: func(_ *contracts.SandboxQualificationProof, runtime *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, _ *SandboxTrust) {
			runtime.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
		}},
		{name: "capability drift", change: func(_ *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, capabilities *contracts.SandboxBackendCapabilities, _ *SandboxTrust) {
			capabilities.Limitations = append(capabilities.Limitations, "new limitation")
		}},
		{name: "tampered evidence", change: func(proof *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, _ *SandboxTrust) {
			proof.Evidence.TestSuiteHash = contracts.HashStrings("different suite")
		}},
		{name: "tampered signature subject", change: func(proof *contracts.SandboxQualificationProof, _ *contracts.SandboxRuntimeIdentity, _ *contracts.SandboxBackendCapabilities, _ *SandboxTrust) {
			proof.Bundle.Bundle.Manifest.RunnerVersion = "2"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, candidateRuntime, candidateCapabilities, candidateTrust := proof, runtime, capabilities, trust
			test.change(&candidate, &candidateRuntime, &candidateCapabilities, &candidateTrust)
			if err := VerifySandboxProof(candidate, candidateRuntime, candidateCapabilities, candidateTrust); err == nil {
				t.Fatal("invalid sandbox qualification was accepted")
			}
		})
	}
}

func TestVerifySandboxProofRejectsMissingSandboxCaseAndManifestEntry(t *testing.T) {
	proof, runtime, capabilities, trust := validSandboxProof(t)
	proof.Bundle.Bundle.Report.Cases = nil
	proof.Bundle.Bundle.Report.ReportHash = ""
	proof.Bundle.Bundle.Manifest.ReportHash = ""
	proof.Bundle.Bundle.Manifest.ManifestHash = ""
	proof = resignSandboxProof(t, proof, trust)
	if err := VerifySandboxProof(proof, runtime, capabilities, trust); err == nil {
		t.Fatal("signed bundle without a sandbox case was accepted")
	}

	proof, runtime, capabilities, trust = validSandboxProof(t)
	proof.Bundle.Bundle.Manifest.Checks = []contracts.QualificationCheckManifest{{
		Name: "unrelated-check", Version: "1", Category: contracts.QualificationCategoryAuthority,
		InputHash: contracts.HashStrings("unrelated input"), Outcome: contracts.QualificationOutcomePassed,
	}}
	proof.Bundle.Bundle.Manifest.ManifestHash = ""
	proof = resignSandboxProof(t, proof, trust)
	if err := VerifySandboxProof(proof, runtime, capabilities, trust); err == nil {
		t.Fatal("signed bundle without a sandbox manifest entry was accepted")
	}
}

func validSandboxProof(t *testing.T) (contracts.SandboxQualificationProof, contracts.SandboxRuntimeIdentity, contracts.SandboxBackendCapabilities, SandboxTrust) {
	t.Helper()
	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	targetHash := contracts.HashStrings("sandbox deployment target")
	trust := SandboxTrust{KeyID: "sandbox-deployment-key", PublicKey: testSandboxPublicKey(0x42), TargetHash: targetHash, WorkspaceID: "workspace-a", Now: func() time.Time { return now }}
	runtime := contracts.SandboxRuntimeIdentity{
		ProviderBuildHash: contracts.HashStrings("provider build"), RuntimeBuildHash: contracts.HashStrings("runtime build"),
		RuntimeConfigurationHash: contracts.HashStrings("runtime configuration"), ImageDigest: "sha256:" + string(bytes.Repeat([]byte{'b'}, 64)),
		ImagePlatform: contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
	}
	capabilities := contracts.SandboxBackendCapabilities{
		Backend: contracts.SandboxBackendOCI, Available: true,
		Enforced:    []contracts.SandboxCapability{contracts.SandboxCapabilityFilesystemIsolation, contracts.SandboxCapabilityNetworkIsolation, contracts.SandboxCapabilityCPULimit},
		Limitations: []string{"test target only"},
	}
	evidence := contracts.SandboxQualificationEvidence{
		ID: "sandbox-suite-v1", Backend: contracts.SandboxBackendOCI, Runtime: runtime,
		CapabilityHash: capabilities.ControlHash(), Limits: contracts.SandboxLimitEnvelope{
			TimeoutMS: int(contracts.MaxToolTimeout / time.Millisecond), MaxStdoutBytes: contracts.MaxToolOutputBytes,
			MaxStderrBytes: contracts.MaxToolOutputBytes, MaxArgCount: contracts.MaxToolArgCount, MaxArgBytes: contracts.MaxToolArgBytes,
			MaxEnvEntries: contracts.MaxToolEnvCount, MaxEnvBytes: contracts.MaxToolEnvBytes, CPUQuotaMilli: contracts.MaxToolCPUQuotaMilli,
			MemoryBytes: contracts.MaxToolMemoryBytes, PIDsLimit: contracts.MaxToolProcessCount, ScratchBytes: contracts.MaxToolScratchBytes,
		},
		TestSuiteHash: contracts.HashStrings("sandbox test suite"), Outcome: contracts.QualificationOutcomePassed,
		Measured: true, ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	evidenceHash := evidence.StableHash()
	report := contracts.QualificationReport{
		RunID: "sandbox-run-1", WorkspaceID: trust.WorkspaceID, TargetHash: targetHash,
		Outcome: contracts.QualificationOutcomePassed,
		Cases:   []contracts.QualificationCase{{Name: evidence.ID, Category: contracts.QualificationCategorySandbox, Outcome: contracts.QualificationOutcomePassed, EvidenceHash: evidenceHash}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	manifest := contracts.QualificationManifest{
		RunID: report.RunID, WorkspaceID: report.WorkspaceID, TargetHash: report.TargetHash,
		ReportHash: report.ReportHash, RunnerVersion: "1",
		Checks: []contracts.QualificationCheckManifest{{Name: evidence.ID, Version: "1", Category: contracts.QualificationCategorySandbox, InputHash: evidenceHash, Outcome: contracts.QualificationOutcomePassed, EvidenceHash: evidenceHash}},
	}
	bundle := contracts.QualificationBundle{Report: report, Manifest: manifest}
	if err := bundle.Normalize(); err != nil {
		t.Fatal(err)
	}
	proof := contracts.SandboxQualificationProof{Evidence: evidence, Bundle: signSandboxBundle(t, bundle, trust.KeyID, 0x42)}
	return proof, runtime, capabilities, trust
}

func resignSandboxProof(t *testing.T, proof contracts.SandboxQualificationProof, trust SandboxTrust) contracts.SandboxQualificationProof {
	t.Helper()
	proof.Bundle.Bundle.Report.ReportHash = ""
	if err := proof.Bundle.Bundle.Report.Normalize(); err != nil {
		t.Fatal(err)
	}
	proof.Bundle.Bundle.Manifest.ReportHash = proof.Bundle.Bundle.Report.ReportHash
	proof.Bundle.Bundle.Manifest.ManifestHash = ""
	if err := proof.Bundle.Bundle.Manifest.Normalize(); err != nil {
		t.Fatal(err)
	}
	proof.Bundle = signSandboxBundle(t, proof.Bundle.Bundle, trust.KeyID, 0x42)
	return proof
}

func signSandboxBundle(t *testing.T, bundle contracts.QualificationBundle, keyID string, seed byte) contracts.SignedQualificationBundle {
	t.Helper()
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(bundle, keyID, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func testSandboxPublicKey(seed byte) ed25519.PublicKey {
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	return privateKey.Public().(ed25519.PublicKey)
}
