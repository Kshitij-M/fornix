package qualification

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const MaxMatrixEntries = 64

// MatrixEntry identifies one explicitly prepared capability request. The
// registry and request are caller-owned so live credential, egress, and
// authority configuration cannot be guessed by the qualification runner.
type MatrixEntry struct {
	Name     string
	Registry *connector.Registry
	Request  contracts.OperationRequest
}

// RunMatrix runs bounded connector conformance in deterministic entry order
// and returns one common redacted qualification report. It performs no
// retries and does not turn an external call into an exactly-once operation.
func RunMatrix(ctx context.Context, runID, workspaceID, targetHash string, entries []MatrixEntry, options connector.ConformanceReportOptions) (contracts.QualificationReport, error) {
	if ctx == nil {
		return contracts.QualificationReport{}, fmt.Errorf("qualification matrix context is nil")
	}
	if len(entries) == 0 || len(entries) > MaxMatrixEntries {
		return contracts.QualificationReport{}, fmt.Errorf("qualification matrix must contain between 1 and %d entries", MaxMatrixEntries)
	}
	ordered := append([]MatrixEntry(nil), entries...)
	seen := make(map[string]struct{}, len(ordered))
	for index := range ordered {
		ordered[index].Name = strings.TrimSpace(ordered[index].Name)
		if ordered[index].Name == "" || len(ordered[index].Name) > 96 || strings.ContainsAny(ordered[index].Name, "\x00\r\n\t") {
			return contracts.QualificationReport{}, fmt.Errorf("matrix entry %d has an invalid name", index)
		}
		if _, exists := seen[ordered[index].Name]; exists {
			return contracts.QualificationReport{}, fmt.Errorf("matrix repeats entry %q", ordered[index].Name)
		}
		seen[ordered[index].Name] = struct{}{}
		if ordered[index].Registry == nil {
			return contracts.QualificationReport{}, fmt.Errorf("matrix entry %q has no registry", ordered[index].Name)
		}
		if ordered[index].Request.WorkspaceID != workspaceID {
			return contracts.QualificationReport{}, fmt.Errorf("matrix entry %q crosses qualification workspace", ordered[index].Name)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	builder, err := NewBuilder(runID, workspaceID, targetHash)
	if err != nil {
		return contracts.QualificationReport{}, err
	}
	for _, entry := range ordered {
		report := connector.RunConformanceReport(ctx, entry.Registry, entry.Request, options)
		if err := builder.AddNamedConnectorReport(entry.Name, report); err != nil {
			return contracts.QualificationReport{}, fmt.Errorf("matrix entry %q: %w", entry.Name, err)
		}
	}
	return builder.Finalize()
}
