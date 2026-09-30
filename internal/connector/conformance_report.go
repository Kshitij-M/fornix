package connector

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

const ConformanceReportSchemaVersion = 1

// ConformanceCase is a redacted observation of one shared adapter check.
// ErrorCode is a bounded classification; raw adapter errors never cross this
// report boundary.
type ConformanceCase struct {
	Name       string `json:"name"`
	Outcome    string `json:"outcome"`
	ErrorCode  string `json:"error_code,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// ConformanceReport is a process-local, artifact-ready qualification record.
// ReportHash excludes timing so the same recorded input and adapter result
// produce the same identity during replay.
type ConformanceReport struct {
	SchemaVersion int                     `json:"schema_version"`
	WorkspaceID   string                  `json:"workspace_id"`
	Capability    contracts.CapabilityRef `json:"capability"`
	Outcome       string                  `json:"outcome"`
	Cases         []ConformanceCase       `json:"cases"`
	ReportHash    string                  `json:"report_hash"`
}

// ConformanceReportOptions makes external execution an explicit operator
// decision. The default is safe for CI and replay: only read-only or recorded
// adapter behavior may be exercised.
type ConformanceReportOptions struct {
	Admission            AdmissionOptions
	AllowExternalEffects bool
}

// RunConformanceReport converts the existing conformance suite into a bounded
// redacted report. It may execute a controlled live read when the caller has
// explicitly supplied a live binding; it never treats that external call as
// exactly-once.
func RunConformanceReport(ctx context.Context, registry *Registry, request contracts.OperationRequest, options ConformanceReportOptions) ConformanceReport {
	report := ConformanceReport{SchemaVersion: ConformanceReportSchemaVersion, WorkspaceID: strings.TrimSpace(request.WorkspaceID), Outcome: "failed", Cases: make([]ConformanceCase, 0, 8)}
	if registry == nil {
		report.Cases = append(report.Cases, ConformanceCase{Name: "registry-configured", Outcome: "failed", ErrorCode: "registry_unavailable"})
		report.ReportHash = conformanceReportHash(report)
		return report
	}
	requestCopy := request
	if request.Metadata != nil {
		requestCopy.Metadata = make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			requestCopy.Metadata[key] = value
		}
	}
	if err := requestCopy.Normalize(); err != nil {
		report.Cases = append(report.Cases, ConformanceCase{Name: "request-normalization", Outcome: "failed", ErrorCode: "invalid_request"})
		report.ReportHash = conformanceReportHash(report)
		return report
	}
	report.WorkspaceID = requestCopy.WorkspaceID
	report.Capability = requestCopy.Capability
	capability, found := registry.Lookup(requestCopy.Capability)
	if !found {
		report.Cases = append(report.Cases, ConformanceCase{Name: "capability-lookup", Outcome: "failed", ErrorCode: "unknown_capability"})
		report.ReportHash = conformanceReportHash(report)
		return report
	}
	definition := capability.Definition()
	if isEffectful(definition.Effect) && !options.AllowExternalEffects {
		report.Cases = append(report.Cases, ConformanceCase{Name: "external-effects-opt-in", Outcome: "blocked", ErrorCode: "external_effect_opt_in"})
		report.Outcome = "blocked"
		report.ReportHash = conformanceReportHash(report)
		return report
	}
	started := time.Now()
	failures := RunConformanceSuite(ctx, registry, requestCopy, options.Admission)
	duration := time.Since(started).Milliseconds()
	if duration < 0 {
		duration = 0
	}
	if len(failures) == 0 {
		report.Cases = append(report.Cases, ConformanceCase{Name: "suite", Outcome: "passed", DurationMS: duration})
		report.Outcome = "passed"
	} else {
		for _, failure := range failures {
			report.Cases = append(report.Cases, ConformanceCase{Name: boundedCaseName(failure.Case), Outcome: "failed", ErrorCode: conformanceErrorCode(failure.Err)})
		}
		// The timing is attached to the first case only so it remains useful to
		// operators without becoming part of the stable report identity.
		if len(report.Cases) > 0 {
			report.Cases[0].DurationMS = duration
		}
	}
	sort.SliceStable(report.Cases, func(i, j int) bool {
		if report.Cases[i].Name != report.Cases[j].Name {
			return report.Cases[i].Name < report.Cases[j].Name
		}
		return report.Cases[i].ErrorCode < report.Cases[j].ErrorCode
	})
	report.ReportHash = conformanceReportHash(report)
	return report
}

func isEffectful(effect contracts.EffectClass) bool {
	return effect != contracts.EffectClassReadOnly && effect != contracts.EffectClassObservation
}

func boundedCaseName(value string) string {
	value = strings.TrimSpace(value)
	if !safeReportToken(value, 96) {
		return "unknown_case"
	}
	return value
}

func conformanceErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var failure *FailureError
	if errors.As(err, &failure) && failure != nil && strings.TrimSpace(failure.Code) != "" {
		return boundedErrorCode(failure.Code)
	}
	switch {
	case errors.Is(err, ErrCapabilityNotFound):
		return "unknown_capability"
	case errors.Is(err, ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrAuthorityExecution):
		return "authority_required"
	case errors.Is(err, ErrApprovalRequired):
		return "approval_required"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return "qualification_failed"
	}
}

func boundedErrorCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if !safeReportToken(value, 64) {
		return "qualification_failed"
	}
	return value
}

func safeReportToken(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func conformanceReportHash(report ConformanceReport) string {
	parts := []string{"connector-conformance", "v1", report.WorkspaceID, report.Capability.StableHash(), report.Outcome}
	for _, item := range report.Cases {
		parts = append(parts, item.Name, item.Outcome, item.ErrorCode)
	}
	return contracts.HashStrings(parts...)
}
