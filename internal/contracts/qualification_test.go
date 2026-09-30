package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestQualificationReportNormalizesOrderAndStableHash(t *testing.T) {
	targetHash := HashStrings("deployment", "test")
	first := QualificationReport{
		RunID: "qualification-1", WorkspaceID: "workspace-a", TargetHash: targetHash, Outcome: "PASSED",
		Cases: []QualificationCase{
			{Name: "topology", Category: QualificationCategoryTopology, Outcome: QualificationOutcomePassed, Measurements: []QualificationMeasurement{{Name: "acquire_p95", Value: 4, Unit: "ms", Present: true}, {Name: "empty_acquires"}}},
			{Name: "adapter", Category: QualificationCategoryAdapter, Outcome: QualificationOutcomePassed, DurationMS: 42},
		},
		RecoveryDrills: []RecoveryDrill{{ID: "pitr-1", Kind: RecoveryDrillPITR, Outcome: QualificationOutcomePassed, TargetHash: targetHash, EvidenceHash: HashStrings("pitr-evidence"), SourceFingerprint: HashStrings("pitr-source"), RestoreFingerprint: HashStrings("pitr-restore"), ReplayHash: HashStrings("replay"), WALArchiveVerified: true, ObservedAt: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}},
	}
	second := first
	second.Cases = append([]QualificationCase(nil), first.Cases[0:1]...)
	second.Cases = append(second.Cases, first.Cases[1])
	second.Cases[0].Measurements = append([]QualificationMeasurement(nil), first.Cases[0].Measurements[1], first.Cases[0].Measurements[0])
	second.StartedAt = time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)
	second.FinishedAt = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.ReportHash != second.ReportHash {
		t.Fatalf("same evidence received different hashes: %s != %s", first.ReportHash, second.ReportHash)
	}
	if first.Cases[0].Category != QualificationCategoryAdapter || first.Cases[1].Measurements[0].Name != "acquire_p95" {
		t.Fatalf("report was not canonically ordered: %+v", first)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "secret") {
		t.Fatalf("qualification report contains forbidden secret text: %s", encoded)
	}
}

func TestQualificationReportRejectsUnboundedOrAmbiguousEvidence(t *testing.T) {
	targetHash := HashStrings("deployment", "test")
	base := QualificationReport{RunID: "qualification-1", TargetHash: targetHash, Outcome: QualificationOutcomePassed}
	base.Cases = []QualificationCase{{Name: "adapter", Category: QualificationCategoryAdapter, Outcome: QualificationOutcomePassed}}
	if err := base.Normalize(); err != nil {
		t.Fatal(err)
	}
	base.Cases = append(base.Cases, QualificationCase{Name: "adapter", Category: QualificationCategoryAdapter, Outcome: QualificationOutcomePassed})
	if err := base.Normalize(); err == nil {
		t.Fatal("duplicate qualification case was accepted")
	}
	invalid := QualificationReport{RunID: "qualification-1", TargetHash: targetHash, Outcome: QualificationOutcomePassed, Cases: []QualificationCase{{Name: "adapter", Category: QualificationCategoryAdapter, Outcome: QualificationOutcomePassed, ErrorCode: "provider secret leaked"}}}
	if err := invalid.Normalize(); err == nil {
		t.Fatal("arbitrary error text was accepted")
	}
	oversized := QualificationReport{RunID: "qualification-1", TargetHash: targetHash, Outcome: QualificationOutcomePassed}
	for index := 0; index < MaxQualificationCases+1; index++ {
		oversized.Cases = append(oversized.Cases, QualificationCase{Name: "case-" + string(rune('a'+index%26)) + "-" + HashStrings(string(rune(index))), Category: QualificationCategoryAdapter, Outcome: QualificationOutcomePassed})
	}
	if err := oversized.Normalize(); err == nil {
		t.Fatal("oversized qualification report was accepted")
	}
}

func TestRecoveryDrillRequiresEvidenceForPassedOutcome(t *testing.T) {
	drill := RecoveryDrill{ID: "failover-1", Kind: RecoveryDrillFailover, Outcome: QualificationOutcomePassed, TargetHash: HashStrings("deployment", "test"), ObservedAt: time.Now()}
	report := QualificationReport{RunID: "qualification-1", TargetHash: drill.TargetHash, Outcome: QualificationOutcomePassed, RecoveryDrills: []RecoveryDrill{drill}}
	if err := report.Normalize(); err == nil {
		t.Fatal("passed recovery drill without evidence was accepted")
	}
}

func TestBoundaryQualificationEvidenceIsDeterministicAndBounded(t *testing.T) {
	boundary := ExternalBoundaryAuthority{
		EgressPolicyHash:      HashStrings("egress"),
		DestinationPolicyHash: HashStrings("destination"),
		NetworkBoundary:       NetworkBoundaryControlledTransport,
		NetworkBoundaryHash:   HashStrings("network"),
	}.StableHash()
	observed := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	makeEvidence := func(id, kind string) BoundaryQualificationEvidence {
		return BoundaryQualificationEvidence{
			ID: id, Kind: kind, Outcome: QualificationOutcomePassed, BoundaryHash: boundary,
			CredentialSourceHash: HashStrings("credential-source", id), IdentityHash: HashStrings("identity", id),
			ProviderRequestHash: HashStrings("request", id), ProviderResponseHash: HashStrings("response", id),
			SourceFingerprint: HashStrings("source", id), Measured: true, ObservedAt: observed, ExpiresAt: observed.Add(time.Hour),
		}
	}
	first := QualificationReport{
		RunID: "qualification-boundary-1", TargetHash: HashStrings("deployment", "boundary"), Outcome: QualificationOutcomePassed,
		Cases: []QualificationCase{{Name: "boundary", Category: QualificationCategoryExternalBoundary, Outcome: QualificationOutcomePassed}},
		BoundaryEvidence: []BoundaryQualificationEvidence{
			makeEvidence("provider", BoundaryQualificationProviderIdempotency),
			makeEvidence("identity", BoundaryQualificationWorkloadIdentity),
		},
	}
	second := first
	second.BoundaryEvidence = append([]BoundaryQualificationEvidence(nil), first.BoundaryEvidence[1], first.BoundaryEvidence[0])
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.ReportHash != second.ReportHash || first.BoundaryEvidenceHash() != second.BoundaryEvidenceHash() {
		t.Fatalf("boundary evidence ordering changed identity: report=%s/%s evidence=%s/%s", first.ReportHash, second.ReportHash, first.BoundaryEvidenceHash(), second.BoundaryEvidenceHash())
	}
	if first.ExternalBoundaryEvidenceHash() != "" {
		t.Fatal("a multi-observation report exposed an ambiguous external boundary hash")
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "secret") {
		t.Fatalf("boundary evidence contains forbidden secret text: %s", encoded)
	}
}

func TestBoundaryQualificationEvidenceFailsClosed(t *testing.T) {
	boundary := HashStrings("boundary")
	base := QualificationReport{
		RunID: "qualification-boundary-invalid", TargetHash: HashStrings("deployment", "invalid"), Outcome: QualificationOutcomePassed,
		Cases:            []QualificationCase{{Name: "boundary", Category: QualificationCategoryExternalBoundary, Outcome: QualificationOutcomePassed}},
		BoundaryEvidence: []BoundaryQualificationEvidence{{ID: "provider", Kind: "unknown", Outcome: QualificationOutcomePassed, BoundaryHash: boundary, Measured: true, ObservedAt: time.Now().UTC()}},
	}
	if err := base.Normalize(); err == nil {
		t.Fatal("unknown boundary evidence kind was accepted")
	}
	base.BoundaryEvidence[0].Kind = BoundaryQualificationProviderIdempotency
	base.BoundaryEvidence[0].Measured = false
	if err := base.Normalize(); err == nil {
		t.Fatal("unmeasured passed boundary evidence was accepted")
	}
}
