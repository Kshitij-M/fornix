package contracts

import (
	"fmt"
	"strconv"
)

const MaxQualificationEffectInvocations = 2

// EffectAuthorityObservation is the redacted result of one dispatcher-backed
// qualification probe. It contains only identity hashes, bounded state facts,
// and counts; provider payloads and credentials are intentionally absent.
type EffectAuthorityObservation struct {
	ReceiptLinkHash          string `json:"receipt_link_hash,omitempty"`
	ReceiptLinkVerified      bool   `json:"receipt_link_verified"`
	WorkspaceID              string `json:"workspace_id"`
	OperationID              string `json:"operation_id"`
	EffectID                 string `json:"effect_id"`
	ReservationHash          string `json:"reservation_hash"`
	DomainLinkID             string `json:"domain_link_id"`
	DomainLinkHash           string `json:"domain_link_hash"`
	ResultHash               string `json:"result_hash,omitempty"`
	ReceiptHash              string `json:"receipt_hash,omitempty"`
	ReplayHash               string `json:"replay_hash"`
	SuccessReconciled        bool   `json:"success_reconciled"`
	UncertainRecovery        bool   `json:"uncertain_recovery"`
	DuplicateSuppressed      bool   `json:"duplicate_suppressed"`
	StaleFenceRejected       bool   `json:"stale_fence_rejected"`
	WorkspaceIsolationProven bool   `json:"workspace_isolation_proven"`
	ReplayStable             bool   `json:"replay_stable"`
	InvocationCount          int    `json:"invocation_count"`
}

// Normalize validates the minimum proof required for a portable dispatcher
// qualification. It does not assert that the referenced rows exist; a
// Postgres-backed probe supplies that authority evidence.
func (o *EffectAuthorityObservation) Normalize() error {
	if o == nil {
		return fmt.Errorf("effect authority observation is nil")
	}
	workspace, err := normalizeDomainWorkspace(o.WorkspaceID)
	if err != nil {
		return err
	}
	o.WorkspaceID = workspace
	for field, value := range map[string]*string{
		"operation_id":   &o.OperationID,
		"effect_id":      &o.EffectID,
		"domain_link_id": &o.DomainLinkID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "effect qualification "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"reservation_hash":  &o.ReservationHash,
		"domain_link_hash":  &o.DomainLinkHash,
		"result_hash":       &o.ResultHash,
		"receipt_hash":      &o.ReceiptHash,
		"receipt_link_hash": &o.ReceiptLinkHash,
		"replay_hash":       &o.ReplayHash,
	} {
		required := field != "result_hash" && field != "receipt_hash" && field != "receipt_link_hash"
		if *value, err = normalizeDomainHash(*value, "effect qualification "+field, required); err != nil {
			return err
		}
	}
	if o.InvocationCount < 1 || o.InvocationCount > MaxQualificationEffectInvocations {
		return fmt.Errorf("effect qualification invocation count is outside bounds")
	}
	if o.SuccessReconciled == o.UncertainRecovery {
		return fmt.Errorf("effect qualification must prove exactly one terminal outcome")
	}
	if o.SuccessReconciled {
		if o.ResultHash == "" || o.ReceiptHash == "" || o.ReceiptLinkHash == "" || !o.ReceiptLinkVerified || !o.DuplicateSuppressed || !o.StaleFenceRejected || !o.WorkspaceIsolationProven || !o.ReplayStable {
			return fmt.Errorf("successful effect qualification is incomplete")
		}
		if o.InvocationCount != 1 {
			return fmt.Errorf("successful effect qualification must invoke once")
		}
	} else {
		if o.ResultHash != "" || o.ReceiptHash != "" || o.ReceiptLinkHash != "" || o.ReceiptLinkVerified || !o.DuplicateSuppressed || !o.StaleFenceRejected || !o.WorkspaceIsolationProven || !o.ReplayStable {
			return fmt.Errorf("uncertain effect qualification is incomplete")
		}
		if o.InvocationCount != 1 {
			return fmt.Errorf("uncertain effect qualification must invoke once")
		}
	}
	return nil
}

// StableHash returns the redacted observation identity.
func (o EffectAuthorityObservation) StableHash() string {
	if err := o.Normalize(); err != nil {
		return ""
	}
	return HashStrings(
		"effect-authority-qualification", o.WorkspaceID, o.OperationID, o.EffectID,
		o.ReservationHash, o.DomainLinkID, o.DomainLinkHash, o.ResultHash,
		o.ReceiptHash, o.ReceiptLinkHash, o.ReplayHash, strconv.FormatBool(o.SuccessReconciled),
		strconv.FormatBool(o.UncertainRecovery), strconv.FormatBool(o.ReceiptLinkVerified),
		strconv.FormatBool(o.DuplicateSuppressed),
		strconv.FormatBool(o.StaleFenceRejected), strconv.FormatBool(o.WorkspaceIsolationProven),
		strconv.FormatBool(o.ReplayStable), strconv.Itoa(o.InvocationCount),
	)
}

// QualificationCase converts the observation into the common redacted
// report shape. Invalid observations fail closed rather than becoming a
// skipped or passing case.
func (o EffectAuthorityObservation) QualificationCase() (QualificationCase, error) {
	if err := o.Normalize(); err != nil {
		return QualificationCase{}, err
	}
	return QualificationCase{
		Name:         "effect-authority-dispatch",
		Category:     QualificationCategoryAuthority,
		Outcome:      QualificationOutcomePassed,
		EvidenceHash: o.StableHash(),
		Measurements: []QualificationMeasurement{{Name: "external_invocations", Value: int64(o.InvocationCount), Unit: "count", Present: true}},
	}, nil
}
