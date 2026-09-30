package qualification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestRunnerRegistrationOrderAndTimingDoNotChangeHashes(t *testing.T) {
	target := contracts.HashStrings("qualification-target")
	checks := []Check{
		{Name: "z-check", Version: "1", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("z")}, nil
		}},
		{Name: "a-check", Version: "1", Category: contracts.QualificationCategoryAdapter, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("a")}, nil
		}},
	}
	first, err := Run(context.Background(), "runner-order", "workspace", target, checks, RunnerOptions{RunnerVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	checks[0].Run = func(context.Context) (contracts.QualificationCase, error) {
		time.Sleep(time.Millisecond)
		return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("z")}, nil
	}
	second, err := Run(context.Background(), "runner-order", "workspace", target, []Check{checks[1], checks[0]}, RunnerOptions{RunnerVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Bundle.Report.ReportHash != second.Bundle.Report.ReportHash || first.Bundle.Manifest.ManifestHash != second.Bundle.Manifest.ManifestHash {
		t.Fatalf("timing/order changed hashes: first=%+v second=%+v", first.Bundle, second.Bundle)
	}
}

func TestRunnerRejectsDuplicateCheckNames(t *testing.T) {
	check := Check{Name: "duplicate", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
		return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed}, nil
	}}
	if _, err := Run(context.Background(), "duplicate-run", "", contracts.HashStrings("target"), []Check{check, check}, RunnerOptions{}); err == nil {
		t.Fatal("duplicate check name was accepted")
	}
}

func TestRunnerBlocksNonOfflineChecksWithoutInvokingThem(t *testing.T) {
	called := false
	result, err := Run(context.Background(), "offline-run", "", contracts.HashStrings("target"), []Check{{Name: "live", Category: contracts.QualificationCategoryTopology, Run: func(context.Context) (contracts.QualificationCase, error) {
		called = true
		return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed}, nil
	}}}, RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if called || result.Bundle.Report.Outcome != contracts.QualificationOutcomeBlocked || result.Bundle.Report.Cases[0].ErrorCode != "offline_only" {
		t.Fatalf("non-offline check was not blocked: called=%v report=%+v", called, result.Bundle.Report)
	}
}

func TestRunnerTimeoutProducesBoundedBlockedCase(t *testing.T) {
	started := time.Now()
	result, err := Run(context.Background(), "timeout-run", "", contracts.HashStrings("target"), []Check{{Name: "slow", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(ctx context.Context) (contracts.QualificationCase, error) {
		<-ctx.Done()
		return contracts.QualificationCase{}, ctx.Err()
	}}}, RunnerOptions{MaxDuration: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("runner exceeded bounded timeout")
	}
	if result.Bundle.Report.Cases[0].Outcome != contracts.QualificationOutcomeBlocked || result.Bundle.Report.Cases[0].ErrorCode != "runner_timeout" {
		t.Fatalf("timeout case=%+v", result.Bundle.Report.Cases[0])
	}
}

func TestRunnerSanitizesCheckErrors(t *testing.T) {
	result, err := Run(context.Background(), "error-run", "", contracts.HashStrings("target"), []Check{{Name: "error", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
		return contracts.QualificationCase{}, errors.New("secret-token-and-prompt")
	}}}, RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Bundle.Report.Cases[0].ErrorCode != "check_failed" || result.Bundle.Report.Cases[0].EvidenceHash != "" {
		t.Fatalf("error was not redacted: %+v", result.Bundle.Report.Cases[0])
	}
}

func TestMergeBundlesIsDeterministicAndWorkspaceScoped(t *testing.T) {
	target := contracts.HashStrings("merge-target")
	makeBundle := func(name string) contracts.QualificationBundle {
		result, err := Run(context.Background(), "source-"+name, "workspace", target, []Check{{Name: name, Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings(name)}, nil
		}}}, RunnerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return result.Bundle
	}
	left, right := makeBundle("left"), makeBundle("right")
	one, err := MergeBundles(context.Background(), "merged", "workspace", target, []contracts.QualificationBundle{left, right}, RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	two, err := MergeBundles(context.Background(), "merged", "workspace", target, []contracts.QualificationBundle{right, left}, RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if one.Bundle.Report.ReportHash != two.Bundle.Report.ReportHash || one.Bundle.Manifest.ManifestHash != two.Bundle.Manifest.ManifestHash {
		t.Fatalf("merge order changed hashes: one=%+v two=%+v", one.Bundle, two.Bundle)
	}
	foreign := right
	foreign.Report.WorkspaceID = "other-workspace"
	foreign.Report.ReportHash = ""
	if err := foreign.Report.Normalize(); err != nil {
		t.Fatal(err)
	}
	foreign.Manifest.WorkspaceID = foreign.Report.WorkspaceID
	foreign.Manifest.ReportHash = foreign.Report.ReportHash
	foreign.Manifest.ManifestHash = ""
	if err := foreign.Manifest.Normalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := MergeBundles(context.Background(), "merged", "workspace", target, []contracts.QualificationBundle{left, foreign}, RunnerOptions{}); err == nil {
		t.Fatal("cross-workspace bundle was accepted")
	}
}

func TestManifestRejectsEnvironmentValues(t *testing.T) {
	result, err := Run(context.Background(), "manifest-run", "", contracts.HashStrings("target"), OfflineChecks(), RunnerOptions{EnvironmentNames: []string{"FORNIX_OPENAI_API_KEY=secret"}})
	if err == nil || result.Bundle.Manifest.ManifestHash != "" {
		t.Fatal("environment value was accepted into manifest")
	}
}

func TestExternalEffectQualificationIsBlockedByDefaultAndExplicitlyOptIn(t *testing.T) {
	probe := func(context.Context) (contracts.EffectAuthorityObservation, error) {
		return contracts.EffectAuthorityObservation{
			WorkspaceID:              "effect-runner-workspace",
			OperationID:              "operation-1",
			EffectID:                 "effect-1",
			ReservationHash:          contracts.HashStrings("reservation"),
			DomainLinkID:             "link-1",
			DomainLinkHash:           contracts.HashStrings("link"),
			ResultHash:               contracts.HashStrings("result"),
			ReceiptHash:              contracts.HashStrings("receipt"),
			ReceiptLinkHash:          contracts.HashStrings("receipt-link"),
			ReplayHash:               contracts.HashStrings("replay"),
			SuccessReconciled:        true,
			ReceiptLinkVerified:      true,
			DuplicateSuppressed:      true,
			StaleFenceRejected:       true,
			WorkspaceIsolationProven: true,
			ReplayStable:             true,
			InvocationCount:          1,
		}, nil
	}
	check := NewEffectAuthorityCheck(probe)
	target := contracts.HashStrings("effect-target")
	blocked, err := Run(context.Background(), "effect-blocked", "effect-runner-workspace", target, []Check{check}, RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Bundle.Report.Cases[0].Outcome != contracts.QualificationOutcomeBlocked || blocked.Bundle.Manifest.Checks[0].ExecutionMode != "external" {
		t.Fatalf("external check was not blocked by default: %+v", blocked.Bundle)
	}
	allowed, err := Run(context.Background(), "effect-allowed", "effect-runner-workspace", target, []Check{check}, RunnerOptions{AllowExternalChecks: true})
	if err != nil {
		t.Fatal(err)
	}
	if allowed.Bundle.Report.Outcome != contracts.QualificationOutcomePassed || allowed.Bundle.Manifest.Checks[0].ExecutionMode != "external" {
		t.Fatalf("explicit external check did not run: %+v", allowed.Bundle)
	}
}
