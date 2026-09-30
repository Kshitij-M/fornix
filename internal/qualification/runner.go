package qualification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	DefaultRunnerVersion = "1"
	DefaultMaxChecks     = 64
	DefaultMaxDuration   = 5 * time.Minute
)

// Check is an explicitly registered, bounded qualification probe. The
// portable runner executes only Offline checks. Deployment-owned checks must
// produce a validated bundle externally and be imported with MergeBundles.
type Check struct {
	Name      string
	Version   string
	Category  string
	InputHash string
	Offline   bool
	Run       func(context.Context) (contracts.QualificationCase, error)
}

// RunnerOptions are hard limits and redacted build metadata. EnvironmentNames
// must contain names only; callers must never pass environment values.
type RunnerOptions struct {
	MaxChecks           int
	MaxDuration         time.Duration
	MaxReportBytes      int
	RunnerVersion       string
	CommitHash          string
	EnvironmentNames    []string
	AllowExternalChecks bool
}

// Result is the portable report and its canonical manifest.
type Result struct {
	Bundle contracts.QualificationBundle
}

func (o RunnerOptions) normalized() (RunnerOptions, error) {
	if o.MaxChecks == 0 {
		o.MaxChecks = DefaultMaxChecks
	}
	if o.MaxChecks < 1 || o.MaxChecks > contracts.MaxQualificationManifestChecks {
		return RunnerOptions{}, fmt.Errorf("runner max checks must be between 1 and %d", contracts.MaxQualificationManifestChecks)
	}
	if o.MaxDuration == 0 {
		o.MaxDuration = DefaultMaxDuration
	}
	if o.MaxDuration <= 0 || o.MaxDuration > 24*time.Hour {
		return RunnerOptions{}, fmt.Errorf("runner max duration is outside bounds")
	}
	if o.MaxReportBytes == 0 {
		o.MaxReportBytes = contracts.MaxQualificationReportBytes
	}
	if o.MaxReportBytes < 1024 || o.MaxReportBytes > contracts.MaxQualificationReportBytes {
		return RunnerOptions{}, fmt.Errorf("runner max report bytes are outside bounds")
	}
	if strings.TrimSpace(o.RunnerVersion) == "" {
		o.RunnerVersion = DefaultRunnerVersion
	}
	if len(o.EnvironmentNames) > contracts.MaxQualificationEnvironmentNames {
		return RunnerOptions{}, fmt.Errorf("runner has too many environment names")
	}
	o.EnvironmentNames = append([]string(nil), o.EnvironmentNames...)
	sort.Strings(o.EnvironmentNames)
	return o, nil
}

func normalizeChecks(checks []Check, max int) ([]Check, error) {
	if len(checks) == 0 || len(checks) > max {
		return nil, fmt.Errorf("qualification checks must contain between 1 and %d entries", max)
	}
	ordered := append([]Check(nil), checks...)
	seen := make(map[string]struct{}, len(ordered))
	for index := range ordered {
		ordered[index].Name = strings.TrimSpace(ordered[index].Name)
		ordered[index].Version = strings.TrimSpace(ordered[index].Version)
		ordered[index].Category = strings.ToLower(strings.TrimSpace(ordered[index].Category))
		if ordered[index].Version == "" {
			ordered[index].Version = "1"
		}
		if ordered[index].Category == "" {
			ordered[index].Category = contracts.QualificationCategoryAuthority
		}
		if ordered[index].InputHash == "" {
			ordered[index].InputHash = contracts.HashStrings("qualification-check", ordered[index].Name, ordered[index].Version, ordered[index].Category)
		}
		if ordered[index].Name == "" || ordered[index].Run == nil {
			return nil, fmt.Errorf("qualification check %d requires name and runner", index)
		}
		key := ordered[index].Category + "\x00" + ordered[index].Name
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("qualification check %q is duplicated", ordered[index].Name)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Category != ordered[j].Category {
			return ordered[i].Category < ordered[j].Category
		}
		return ordered[i].Name < ordered[j].Name
	})
	return ordered, nil
}

// Run executes checks sequentially in deterministic order. It never retries a
// check and never performs an implicit external effect.
func Run(ctx context.Context, runID, workspaceID, targetHash string, checks []Check, options RunnerOptions) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("qualification runner context is nil")
	}
	var err error
	options, err = options.normalized()
	if err != nil {
		return Result{}, err
	}
	ordered, err := normalizeChecks(checks, options.MaxChecks)
	if err != nil {
		return Result{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, options.MaxDuration)
	defer cancel()
	builder, err := NewBuilder(runID, workspaceID, targetHash)
	if err != nil {
		return Result{}, err
	}
	started := time.Now().UTC()
	for _, check := range ordered {
		item := contracts.QualificationCase{Name: check.Name, Category: check.Category}
		if runCtx.Err() != nil {
			item.Outcome = contracts.QualificationOutcomeSkipped
			if errorsIsDeadline(runCtx.Err()) {
				item.ErrorCode = "runner_timeout"
			} else {
				item.ErrorCode = "runner_cancelled"
			}
			if err := builder.AddCase(item); err != nil {
				return Result{}, err
			}
			continue
		}
		if !check.Offline && !options.AllowExternalChecks {
			item.Outcome = contracts.QualificationOutcomeBlocked
			item.ErrorCode = "offline_only"
			if err := builder.AddCase(item); err != nil {
				return Result{}, err
			}
			continue
		}
		checkStarted := time.Now()
		value, checkErr := executeCheck(runCtx, check)
		item.DurationMS = time.Since(checkStarted).Milliseconds()
		item.EvidenceHash = value.EvidenceHash
		item.Measurements = value.Measurements
		if errors.Is(checkErr, context.DeadlineExceeded) {
			item.Outcome = contracts.QualificationOutcomeBlocked
			item.ErrorCode = "runner_timeout"
		} else if errors.Is(checkErr, context.Canceled) {
			item.Outcome = contracts.QualificationOutcomeSkipped
			item.ErrorCode = "runner_cancelled"
		} else if checkErr != nil {
			item.Outcome = contracts.QualificationOutcomeFailed
			item.ErrorCode = "check_failed"
		} else {
			item.Outcome = strings.ToLower(strings.TrimSpace(value.Outcome))
			if item.Outcome == "" {
				item.Outcome = contracts.QualificationOutcomePassed
			}
			if item.Outcome != contracts.QualificationOutcomePassed &&
				item.Outcome != contracts.QualificationOutcomeFailed &&
				item.Outcome != contracts.QualificationOutcomeBlocked &&
				item.Outcome != contracts.QualificationOutcomeSkipped {
				item.Outcome = contracts.QualificationOutcomeFailed
				item.ErrorCode = "invalid_check_result"
			}
		}
		if err := builder.AddCase(item); err != nil {
			return Result{}, err
		}
		if report, finalizeErr := builder.Finalize(); finalizeErr != nil {
			return Result{}, finalizeErr
		} else if encodedSize(report) > options.MaxReportBytes {
			return Result{}, fmt.Errorf("qualification report exceeds runner byte budget")
		}
	}
	report, err := builder.Finalize()
	if err != nil {
		return Result{}, err
	}
	report.StartedAt = started
	report.FinishedAt = time.Now().UTC()
	report.ReportHash = ""
	if err := report.Normalize(); err != nil {
		return Result{}, err
	}
	if encodedSize(report) > options.MaxReportBytes {
		return Result{}, fmt.Errorf("qualification report exceeds runner byte budget")
	}
	manifest := contracts.QualificationManifest{
		RunID: runID, WorkspaceID: workspaceID, TargetHash: targetHash,
		ReportHash: report.ReportHash, RunnerVersion: options.RunnerVersion,
		CommitHash: options.CommitHash, EnvironmentNames: options.EnvironmentNames,
		Checks: make([]contracts.QualificationCheckManifest, 0, len(ordered)),
	}
	for _, check := range ordered {
		for _, item := range report.Cases {
			if item.Name == check.Name && item.Category == check.Category {
				manifest.Checks = append(manifest.Checks, contracts.QualificationCheckManifest{
					Name: check.Name, Version: check.Version, Category: check.Category,
					ExecutionMode: qualificationExecutionMode(check),
					InputHash:     check.InputHash, Outcome: item.Outcome, EvidenceHash: item.EvidenceHash,
				})
				break
			}
		}
	}
	if len(manifest.Checks) != len(ordered) {
		return Result{}, fmt.Errorf("qualification manifest did not record every check")
	}
	bundle := contracts.QualificationBundle{Report: report, Manifest: manifest}
	if err := bundle.Normalize(); err != nil {
		return Result{}, err
	}
	if encodedSize(bundle) > options.MaxReportBytes {
		return Result{}, fmt.Errorf("qualification bundle exceeds runner byte budget")
	}
	return Result{Bundle: bundle}, nil
}

func qualificationExecutionMode(check Check) string {
	if check.Offline {
		return "offline"
	}
	return "external"
}

type checkExecution struct {
	value contracts.QualificationCase
	err   error
}

func executeCheck(ctx context.Context, check Check) (contracts.QualificationCase, error) {
	done := make(chan checkExecution, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- checkExecution{err: errors.New("qualification check panicked")}
			}
		}()
		value, err := check.Run(ctx)
		done <- checkExecution{value: value, err: err}
	}()
	select {
	case result := <-done:
		return result.value, result.err
	case <-ctx.Done():
		return contracts.QualificationCase{}, ctx.Err()
	}
}

func encodedSize(value any) int {
	raw, err := jsonMarshal(value)
	if err != nil {
		return contracts.MaxQualificationReportBytes + 1
	}
	return len(raw)
}

var jsonMarshal = func(value any) ([]byte, error) {
	return json.Marshal(value)
}

func errorsIsDeadline(err error) bool { return errors.Is(err, context.DeadlineExceeded) }

// OfflineChecks returns the built-in, side-effect-free contract probes used by
// the default CLI runner.
func OfflineChecks() []Check {
	return []Check{
		{Name: "qualification-contract", Version: "1", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("qualification-contract", "v1")}, nil
		}},
		{Name: "qualification-determinism", Version: "1", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("qualification-determinism", "v1")}, nil
		}},
		{Name: "qualification-redaction", Version: "1", Category: contracts.QualificationCategoryAuthority, Offline: true, Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("qualification-redaction", "v1")}, nil
		}},
	}
}

// MergeBundles deterministically combines validated bundle evidence without
// contacting any source system. Duplicate bundle hashes are ignored; distinct
// duplicate case identities fail closed.
func MergeBundles(ctx context.Context, runID, workspaceID, targetHash string, bundles []contracts.QualificationBundle, options RunnerOptions) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("qualification merge context is nil")
	}
	if len(bundles) == 0 || len(bundles) > contracts.MaxQualificationManifestChecks {
		return Result{}, fmt.Errorf("qualification merge requires between 1 and %d bundles", contracts.MaxQualificationManifestChecks)
	}
	options, err := options.normalized()
	if err != nil {
		return Result{}, err
	}
	ordered := append([]contracts.QualificationBundle(nil), bundles...)
	for index := range ordered {
		if err := ordered[index].Normalize(); err != nil {
			return Result{}, fmt.Errorf("bundle %d: %w", index, err)
		}
		if ordered[index].Report.WorkspaceID != workspaceID || ordered[index].Report.TargetHash != targetHash {
			return Result{}, fmt.Errorf("bundle %d crosses merge scope", index)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Manifest.ManifestHash < ordered[j].Manifest.ManifestHash })
	builder, err := NewBuilder(runID, workspaceID, targetHash)
	if err != nil {
		return Result{}, err
	}
	seenBundles := make(map[string]struct{}, len(ordered))
	seenCases := make(map[string]contracts.QualificationCase)
	seenDrills := make(map[string]contracts.RecoveryDrill)
	seenChecks := make(map[string]contracts.QualificationCheckManifest)
	for _, bundle := range ordered {
		if _, exists := seenBundles[bundle.Manifest.ManifestHash]; exists {
			continue
		}
		seenBundles[bundle.Manifest.ManifestHash] = struct{}{}
		for _, item := range bundle.Report.Cases {
			key := item.Category + "\x00" + item.Name
			if prior, exists := seenCases[key]; exists {
				if prior.Outcome != item.Outcome || prior.EvidenceHash != item.EvidenceHash {
					return Result{}, fmt.Errorf("conflicting qualification case %q", item.Name)
				}
				continue
			}
			seenCases[key] = item
			if err := builder.AddCase(item); err != nil {
				return Result{}, err
			}
		}
		for _, item := range bundle.Report.RecoveryDrills {
			if prior, exists := seenDrills[item.ID]; exists {
				if prior.Outcome != item.Outcome || prior.EvidenceHash != item.EvidenceHash || prior.ReplayHash != item.ReplayHash {
					return Result{}, fmt.Errorf("conflicting qualification recovery drill %q", item.ID)
				}
				continue
			}
			seenDrills[item.ID] = item
			if err := builder.AddRecoveryDrill(item); err != nil {
				return Result{}, err
			}
		}
		for _, item := range bundle.Manifest.Checks {
			key := item.Category + "\x00" + item.Name
			if prior, exists := seenChecks[key]; exists {
				if prior.InputHash != item.InputHash || prior.Version != item.Version || prior.ExecutionMode != item.ExecutionMode || prior.Outcome != item.Outcome || prior.EvidenceHash != item.EvidenceHash {
					return Result{}, fmt.Errorf("conflicting qualification check %q", item.Name)
				}
				continue
			}
			seenChecks[key] = item
		}
	}
	report, err := builder.Finalize()
	if err != nil {
		return Result{}, err
	}
	manifest := contracts.QualificationManifest{
		RunID: runID, WorkspaceID: workspaceID, TargetHash: targetHash,
		ReportHash: report.ReportHash, RunnerVersion: options.RunnerVersion,
		CommitHash: options.CommitHash, EnvironmentNames: options.EnvironmentNames,
		Checks: make([]contracts.QualificationCheckManifest, 0, len(seenChecks)),
	}
	for _, item := range seenChecks {
		manifest.Checks = append(manifest.Checks, item)
	}
	bundle := contracts.QualificationBundle{Report: report, Manifest: manifest}
	if err := bundle.Normalize(); err != nil {
		return Result{}, err
	}
	if encodedSize(bundle) > options.MaxReportBytes {
		return Result{}, fmt.Errorf("qualification bundle exceeds runner byte budget")
	}
	return Result{Bundle: bundle}, nil
}
