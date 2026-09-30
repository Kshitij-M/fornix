// Package qualification assembles bounded, redacted deployment evidence from
// existing Fornix conformance seams. It does not execute external effects or
// make a deployment's database, provider, or identity authority its own.
package qualification

import (
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

// Builder incrementally assembles one qualification report in memory. The
// caller supplies a hash of the deployment target; raw endpoints and secrets
// are intentionally not representable.
type Builder struct {
	report contracts.QualificationReport
}

// NewBuilder creates a report builder for one deployment target. A workspace
// ID is optional for deployment-wide evidence and required when the evidence
// is workspace-scoped.
func NewBuilder(runID, workspaceID, targetHash string) (*Builder, error) {
	report := contracts.QualificationReport{
		RunID:          strings.TrimSpace(runID),
		WorkspaceID:    strings.TrimSpace(workspaceID),
		TargetHash:     strings.TrimSpace(targetHash),
		Outcome:        contracts.QualificationOutcomePassed,
		Cases:          make([]contracts.QualificationCase, 0, 8),
		RecoveryDrills: make([]contracts.RecoveryDrill, 0, 4),
	}
	if err := report.Normalize(); err != nil {
		return nil, err
	}
	return &Builder{report: report}, nil
}

// AddCase adds one redacted case and recomputes the report only at finalize.
func (b *Builder) AddCase(item contracts.QualificationCase) error {
	if b == nil {
		return fmt.Errorf("qualification builder is nil")
	}
	previous := b.report
	b.report.ReportHash = ""
	if qualificationOutcomeRank(item.Outcome) > qualificationOutcomeRank(b.report.Outcome) {
		b.report.Outcome = item.Outcome
	}
	b.report.Cases = append(b.report.Cases, item)
	if err := b.report.Normalize(); err != nil {
		b.report = previous
		return err
	}
	return nil
}

// AddRecoveryDrill adds explicit deployment-owned recovery evidence.
func (b *Builder) AddRecoveryDrill(drill contracts.RecoveryDrill) error {
	if b == nil {
		return fmt.Errorf("qualification builder is nil")
	}
	previous := b.report
	b.report.ReportHash = ""
	if qualificationOutcomeRank(drill.Outcome) > qualificationOutcomeRank(b.report.Outcome) {
		b.report.Outcome = drill.Outcome
	}
	b.report.RecoveryDrills = append(b.report.RecoveryDrills, drill)
	if err := b.report.Normalize(); err != nil {
		b.report = previous
		return err
	}
	return nil
}

// AddReport merges one already-validated report into the builder. The target
// and workspace must match; duplicate cases are rejected so an accidental
// merge cannot silently replace evidence.
func (b *Builder) AddReport(report contracts.QualificationReport) error {
	if b == nil {
		return fmt.Errorf("qualification builder is nil")
	}
	if err := report.Normalize(); err != nil {
		return err
	}
	if report.TargetHash != b.report.TargetHash || report.WorkspaceID != b.report.WorkspaceID {
		return fmt.Errorf("qualification report crosses target or workspace")
	}
	if qualificationOutcomeRank(report.Outcome) > qualificationOutcomeRank(b.report.Outcome) {
		b.report.ReportHash = ""
		b.report.Outcome = report.Outcome
	}
	for _, item := range report.Cases {
		if err := b.AddCase(item); err != nil {
			return err
		}
	}
	for _, drill := range report.RecoveryDrills {
		if err := b.AddRecoveryDrill(drill); err != nil {
			return err
		}
	}
	return nil
}

// AddConnectorReport adapts the existing connector report into the common
// evidence envelope. Raw adapter failures remain outside the report.
func (b *Builder) AddConnectorReport(report connector.ConformanceReport) error {
	return b.AddNamedConnectorReport("", report)
}

// AddNamedConnectorReport adapts a connector report and prefixes each case
// with a bounded matrix entry name when one is supplied.
func (b *Builder) AddNamedConnectorReport(entryName string, report connector.ConformanceReport) error {
	if b == nil {
		return fmt.Errorf("qualification builder is nil")
	}
	capabilityHash := report.Capability.StableHash()
	if capabilityHash == "" || report.ReportHash == "" {
		return fmt.Errorf("connector report has no stable capability or report hash")
	}
	if b.report.WorkspaceID != "" && report.WorkspaceID != b.report.WorkspaceID {
		return fmt.Errorf("connector report crosses qualification workspace")
	}
	entryName = strings.TrimSpace(entryName)
	if entryName != "" && (len(entryName) > 96 || strings.ContainsAny(entryName, "\x00\r\n\t")) {
		return fmt.Errorf("qualification matrix entry name is invalid")
	}
	if qualificationOutcomeRank(report.Outcome) > qualificationOutcomeRank(b.report.Outcome) {
		b.report.ReportHash = ""
		b.report.Outcome = report.Outcome
	}
	for _, item := range report.Cases {
		name := "connector-" + capabilityHash[:16] + "-" + item.Name
		if entryName != "" {
			name = "adapter-" + entryName + "-" + item.Name
		}
		qualificationCase := contracts.QualificationCase{
			Name:         name,
			Category:     contracts.QualificationCategoryAdapter,
			Outcome:      item.Outcome,
			ErrorCode:    item.ErrorCode,
			DurationMS:   item.DurationMS,
			EvidenceHash: report.ReportHash,
		}
		if err := b.AddCase(qualificationCase); err != nil {
			return err
		}
	}
	return nil
}

func qualificationOutcomeRank(outcome string) int {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case contracts.QualificationOutcomeFailed:
		return 3
	case contracts.QualificationOutcomeBlocked:
		return 2
	case contracts.QualificationOutcomeSkipped:
		return 1
	default:
		return 0
	}
}

// Finalize validates and returns a detached normalized report.
func (b *Builder) Finalize() (contracts.QualificationReport, error) {
	if b == nil {
		return contracts.QualificationReport{}, fmt.Errorf("qualification builder is nil")
	}
	result := b.report
	result.Cases = append([]contracts.QualificationCase(nil), b.report.Cases...)
	result.RecoveryDrills = append([]contracts.RecoveryDrill(nil), b.report.RecoveryDrills...)
	if err := result.Normalize(); err != nil {
		return contracts.QualificationReport{}, err
	}
	return result, nil
}
