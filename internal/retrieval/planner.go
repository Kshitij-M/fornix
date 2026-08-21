// Package retrieval builds deterministic, workspace-scoped context for
// verifiable AI work from authoritative Postgres records. Expensive stages are
// explicit plan choices, not hidden fallback behavior, so a result can explain
// which evidence and budget produced its context.
package retrieval

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/omaveda/fornix/internal/contracts"
)

const policyVersion = "retrieval-v1"

// BuildPlan normalizes a request and returns the stable staged plan that will
// govern retrieval execution.
func BuildPlan(request contracts.RetrievalRequest) (contracts.RetrievalPlan, contracts.RetrievalRequest, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.RetrievalPlan{}, contracts.RetrievalRequest{}, err
	}
	requestHash, err := normalized.Hash()
	if err != nil {
		return contracts.RetrievalPlan{}, contracts.RetrievalRequest{}, err
	}
	limit := normalized.CandidateLimit
	stages := []contracts.RetrievalStagePlan{
		{Name: contracts.StageStructured, Enabled: hasStructuredConstraints(normalized), CandidateLimit: limit},
		{Name: contracts.StageLexical, Enabled: normalized.Query != "", CandidateLimit: limit},
		{Name: contracts.StageGraph, Enabled: normalized.EnableGraph != nil && *normalized.EnableGraph, CandidateLimit: max(1, limit/2)},
		{Name: contracts.StageVector, Enabled: normalized.EnableVector != nil && *normalized.EnableVector && len(normalized.QueryEmbedding) > 0, CandidateLimit: limit},
	}
	for i := range stages {
		if stages[i].Enabled {
			continue
		}
		switch stages[i].Name {
		case contracts.StageStructured:
			stages[i].Reason = "no_structured_constraints"
		case contracts.StageLexical:
			stages[i].Reason = "empty_query"
		case contracts.StageGraph:
			stages[i].Reason = "disabled_by_request"
		case contracts.StageVector:
			if normalized.EnableVector != nil && !*normalized.EnableVector {
				stages[i].Reason = "disabled_by_request"
			} else {
				stages[i].Reason = "query_embedding_not_supplied"
			}
		}
	}
	plan := contracts.RetrievalPlan{
		SchemaVersion: contracts.RetrievalSchemaVersion,
		Policy:        policyVersion,
		RequestHash:   requestHash,
		WorkspaceID:   normalized.WorkspaceID,
		Budget: contracts.RetrievalBudget{
			MaxItems:  normalized.MaxItems,
			MaxBytes:  normalized.MaxBytes,
			MaxTokens: normalized.MaxTokens,
		},
		Stages: stages,
	}
	return plan, normalized, nil
}

// PlanHash returns the deterministic identity of a retrieval plan.
func PlanHash(plan contracts.RetrievalPlan) string {
	b, err := json.Marshal(plan)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func hasStructuredConstraints(request contracts.RetrievalRequest) bool {
	return len(request.ExactSourceRefs) > 0 || request.TaskID != "" || request.MemoType != "" || request.Repo != ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
