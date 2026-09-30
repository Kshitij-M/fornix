package qualification

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// SandboxTrust selects the deployment-authorized signing key and target for
// one sandbox-provider qualification. PublicKey is copied by callers that
// retain this value; private key material is never accepted here.
type SandboxTrust struct {
	KeyID      string
	PublicKey  ed25519.PublicKey
	TargetHash string
	// WorkspaceID is optional for deployment-wide runtime qualification. When
	// set, the signed bundle must also name this qualification workspace.
	WorkspaceID string
	Now         func() time.Time
}

// ValidateSandboxTrust checks the public trust inputs before they are stored
// by a process-local sandbox registry.
func ValidateSandboxTrust(trust SandboxTrust) error {
	if len(trust.PublicKey) != ed25519.PublicKeySize || strings.TrimSpace(trust.KeyID) == "" || !validSHA256(strings.TrimSpace(trust.TargetHash)) {
		return fmt.Errorf("sandbox qualification trust is incomplete")
	}
	if trust.WorkspaceID != strings.TrimSpace(trust.WorkspaceID) || len(trust.WorkspaceID) > 128 || strings.ContainsAny(trust.WorkspaceID, "\x00\r\n\t") {
		return fmt.Errorf("sandbox qualification workspace is invalid")
	}
	return nil
}

// VerifySandboxProof proves that the supplied runtime identity and control
// report match measured, passing evidence signed by the configured key for
// the configured deployment target. The attestor is responsible for running
// the named conformance suite against the actual supported runtime.
func VerifySandboxProof(proof contracts.SandboxQualificationProof, runtime contracts.SandboxRuntimeIdentity, capabilities contracts.SandboxBackendCapabilities, trust SandboxTrust) error {
	if err := ValidateSandboxTrust(trust); err != nil {
		return err
	}
	if trust.Now == nil {
		trust.Now = func() time.Time { return time.Now().UTC() }
	}
	now := trust.Now().UTC()
	if now.IsZero() {
		return fmt.Errorf("sandbox qualification reference time is invalid")
	}
	evidence := proof.Evidence
	if err := evidence.NormalizeAt(now); err != nil {
		return fmt.Errorf("sandbox qualification evidence is invalid")
	}
	if err := runtime.Normalize(); err != nil {
		return fmt.Errorf("sandbox runtime identity is invalid")
	}
	if err := capabilities.Normalize(); err != nil {
		return fmt.Errorf("sandbox capability report is invalid")
	}
	if evidence.Backend != capabilities.Backend || evidence.CapabilityHash != capabilities.ControlHash() ||
		evidence.Runtime != runtime {
		return fmt.Errorf("sandbox qualification does not match the current provider runtime or controls")
	}
	if err := proof.Bundle.VerifyWithKey(trust.KeyID, trust.PublicKey); err != nil {
		return fmt.Errorf("sandbox qualification signature is not trusted")
	}
	report := proof.Bundle.Bundle.Report
	if report.TargetHash != strings.TrimSpace(trust.TargetHash) || report.Outcome != contracts.QualificationOutcomePassed ||
		(trust.WorkspaceID != "" && report.WorkspaceID != strings.TrimSpace(trust.WorkspaceID)) {
		return fmt.Errorf("sandbox qualification is outside the trusted target or workspace")
	}
	evidenceHash := evidence.StableHash()
	caseFound, manifestFound := false, false
	for _, item := range report.Cases {
		if item.Category != contracts.QualificationCategorySandbox || item.Name != evidence.ID {
			continue
		}
		if item.Outcome != contracts.QualificationOutcomePassed || item.EvidenceHash != evidenceHash {
			return fmt.Errorf("sandbox qualification case does not bind the evidence")
		}
		caseFound = true
	}
	for _, item := range proof.Bundle.Bundle.Manifest.Checks {
		if item.Category != contracts.QualificationCategorySandbox || item.Name != evidence.ID {
			continue
		}
		if item.Outcome != contracts.QualificationOutcomePassed || item.InputHash != evidenceHash || item.EvidenceHash != evidenceHash {
			return fmt.Errorf("sandbox qualification manifest does not bind the evidence")
		}
		manifestFound = true
	}
	if !caseFound || !manifestFound {
		return fmt.Errorf("sandbox qualification case or manifest entry is missing")
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
