package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/qualification"
)

func integerArg(args []string, name string, fallback int) int {
	raw := valueArg(args, name, "")
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

// qualificationCommand validates or hashes a redacted qualification report
// without contacting Fornix, a provider, a database, or an external system.
// This is intentionally an offline operator command for deployment evidence.
func (c *operatorCLI) qualificationCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: fornix qualification ...|schedule-register|schedule-list|schedule-get|schedule-plan|schedule-claim|schedule-renew|schedule-release|schedule-complete|schedule-pause|schedule-resume|schedule-cancel|schedule-attempts")
	}
	switch args[0] {
	case "refresh", "refresh-plan":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		itemsFile := valueArg(args[1:], "file", "")
		asOfRaw := valueArg(args[1:], "as-of", "")
		if deployment == "" || releaseID == "" || itemsFile == "" || asOfRaw == "" {
			return errors.New("qualification refresh requires --deployment ID --release-id ID --file PATH --as-of RFC3339")
		}
		asOf, err := time.Parse(time.RFC3339Nano, asOfRaw)
		if err != nil {
			return errors.New("qualification refresh --as-of must be RFC3339")
		}
		items, err := readQualificationRefreshItems(itemsFile)
		if err != nil {
			return err
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-refresh:"+releaseID+":"+asOf.UTC().Format(time.RFC3339Nano))
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "items": items, "as_of": asOf.UTC(), "idempotency_key": idempotency, "dry_run": args[0] == "refresh-plan" || valueArg(args[1:], "dry-run", "false") == "true"}
		scheduleID, attemptID, ownerID, planHash := valueArg(args[1:], "schedule-id", ""), valueArg(args[1:], "attempt-id", ""), valueArg(args[1:], "owner-id", ""), valueArg(args[1:], "plan-hash", "")
		if scheduleID != "" || attemptID != "" || ownerID != "" || planHash != "" {
			fence := uint64Value(args[1:], "fence", 0)
			if scheduleID == "" || attemptID == "" || ownerID == "" || planHash == "" || fence == 0 {
				return errors.New("qualification refresh schedule authorization requires --schedule-id ID --attempt-id ID --owner-id ID --fence N --plan-hash HASH")
			}
			body["schedule_authorization"] = map[string]any{"schedule_id": scheduleID, "attempt_id": attemptID, "owner_id": ownerID, "fence": fence, "plan_hash": planHash}
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/refresh"
		if args[0] == "refresh-plan" {
			path += "/plan"
		}
		return c.requestPrintWithHeaders(http.MethodPost, path, body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "refresh-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification refresh-list requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/refreshes?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "refresh-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		refreshID := valueArg(args[1:], "id", "")
		if deployment == "" || releaseID == "" || refreshID == "" {
			return errors.New("qualification refresh-get requires --deployment ID --release-id ID --id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/refreshes/" + url.PathEscape(refreshID) + "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "schedule-register":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		file := valueArg(args[1:], "file", "")
		if file == "" {
			return errors.New("qualification schedule-register requires --file PATH")
		}
		request, err := readQualificationRefreshScheduleRequest(file)
		if err != nil {
			return err
		}
		request.WorkspaceID = workspace
		request.Actor = contracts.AuditActor{ID: "cli", Kind: "cli", WorkspaceID: workspace}
		if request.IdempotencyKey == "" {
			request.IdempotencyKey = valueArg(args[1:], "idempotency", "qualification-schedule:"+request.DeploymentID+":"+request.ReleaseID)
		}
		if err := request.Normalize(); err != nil {
			return err
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/refresh-schedules", request, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": request.IdempotencyKey})
	case "schedule-list", "schedule-plan":
		workspace, deployment := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification schedule-list requires --deployment ID")
		}
		path := "/v1/qualification/refresh-schedules"
		if args[0] == "schedule-plan" {
			path += "/plan"
		}
		path += "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		if release := valueArg(args[1:], "release-id", ""); release != "" {
			path += "&release_id=" + url.QueryEscape(release)
		}
		for _, name := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], name, ""); value != "" {
				path += "&" + name + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "schedule-get", "schedule-attempts":
		workspace, id := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "id", "")
		if id == "" {
			return errors.New("qualification schedule command requires --id ID")
		}
		path := "/v1/qualification/refresh-schedules/" + url.PathEscape(id)
		if args[0] == "schedule-attempts" {
			path += "/attempts"
		}
		path += "?workspace_id=" + url.QueryEscape(workspace)
		for _, name := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], name, ""); value != "" {
				path += "&" + name + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "schedule-claim":
		workspace, owner := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "owner-id", "")
		if owner == "" {
			return errors.New("qualification schedule-claim requires --owner-id ID")
		}
		body := map[string]any{"workspace_id": workspace, "owner_id": owner, "lease_ttl_ms": int64Value(args[1:], "lease-ttl-ms", 0)}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/refresh-schedules/claim", body, false, map[string]string{"X-Workspace-ID": workspace})
	case "schedule-renew", "schedule-release":
		workspace, id, owner := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "id", ""), valueArg(args[1:], "owner-id", "")
		fence := uint64Value(args[1:], "fence", 0)
		if id == "" || owner == "" || fence == 0 {
			return errors.New("qualification schedule lease command requires --id ID --owner-id ID --fence N")
		}
		body := map[string]any{"workspace_id": workspace, "owner_id": owner, "fence": fence}
		path := "/v1/qualification/refresh-schedules/" + url.PathEscape(id) + "/" + strings.TrimPrefix(args[0], "schedule-")
		if args[0] == "schedule-renew" {
			body["lease_ttl_ms"] = int64Value(args[1:], "lease-ttl-ms", 0)
		}
		return c.requestPrintWithHeaders(http.MethodPost, path, body, false, map[string]string{"X-Workspace-ID": workspace})
	case "schedule-complete":
		workspace, id, owner := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "id", ""), valueArg(args[1:], "owner-id", "")
		fence := uint64Value(args[1:], "fence", 0)
		attempt, plan, refreshID, refreshHash := valueArg(args[1:], "attempt-id", ""), valueArg(args[1:], "plan-hash", ""), valueArg(args[1:], "refresh-id", ""), valueArg(args[1:], "refresh-hash", "")
		if id == "" || owner == "" || fence == 0 || attempt == "" || plan == "" {
			return errors.New("qualification schedule-complete requires --id ID --owner-id ID --fence N --attempt-id ID --plan-hash HASH")
		}
		body := map[string]any{"workspace_id": workspace, "schedule_id": id, "attempt_id": attempt, "owner_id": owner, "fence": fence, "plan_hash": plan, "refresh_id": refreshID, "refresh_hash": refreshHash, "outcome": valueArg(args[1:], "outcome", contracts.QualificationRefreshScheduleOutcomeSucceeded), "retryable": valueArg(args[1:], "retryable", "false") == "true", "idempotency_key": valueArg(args[1:], "idempotency", "qualification-attempt:"+attempt+":"+strconv.FormatUint(fence, 10))}
		asOf := valueArg(args[1:], "as-of", "")
		if asOf == "" {
			return errors.New("qualification schedule-complete requires --as-of RFC3339")
		}
		parsed, err := time.Parse(time.RFC3339Nano, asOf)
		if err != nil {
			return errors.New("qualification schedule-complete --as-of must be RFC3339")
		}
		body["as_of"] = parsed.UTC()
		if code := valueArg(args[1:], "error-code", ""); code != "" {
			body["error_code"] = code
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/refresh-schedules/"+url.PathEscape(id)+"/complete", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": body["idempotency_key"].(string)})
	case "schedule-pause", "schedule-resume", "schedule-cancel":
		workspace, id := valueArg(args[1:], "workspace", c.workspace), valueArg(args[1:], "id", "")
		if id == "" {
			return errors.New("qualification schedule state command requires --id ID")
		}
		body := map[string]any{"workspace_id": workspace, "reason": valueArg(args[1:], "reason", "")}
		if args[0] != "schedule-resume" {
			owner := valueArg(args[1:], "owner-id", "")
			fence := uint64Value(args[1:], "fence", 0)
			if owner == "" || fence == 0 {
				return errors.New("qualification schedule state command requires --owner-id ID --fence N")
			}
			body["owner_id"], body["fence"] = owner, fence
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/refresh-schedules/"+url.PathEscape(id)+"/"+strings.TrimPrefix(args[0], "schedule-"), body, false, map[string]string{"X-Workspace-ID": workspace})
	case "adapter-conformance":
		registry, err := effectdispatch.BuiltinConformanceRegistry()
		if err != nil {
			return errors.New("adapter conformance manifest could not be built")
		}
		hash, err := registry.Validate()
		if err != nil {
			return errors.New("adapter conformance manifest failed validation")
		}
		return c.print(map[string]any{
			"schema_version": effectdispatch.AdapterConformanceSchemaVersion,
			"outcome":        "passed",
			"entries":        registry.Entries(),
			"manifest_hash":  hash,
		})
	case "external-boundary":
		registry, err := effectdispatch.BuiltinConformanceRegistry()
		if err != nil {
			return errors.New("external-boundary manifest could not be built")
		}
		hash, err := registry.Validate()
		if err != nil {
			return errors.New("external-boundary manifest failed validation")
		}
		entries := registry.Entries()
		external := make([]effectdispatch.AdapterConformance, 0, len(entries))
		for _, entry := range entries {
			if entry.Effect == contracts.EffectClassExternalCommunication {
				external = append(external, entry)
			}
		}
		return c.print(map[string]any{
			"schema_version":                  contracts.ExternalBoundarySchemaVersion,
			"outcome":                         "passed",
			"external_effect_entries":         external,
			"adapter_manifest_hash":           hash,
			"boundary_contract":               "hash_only",
			"network_boundary_modes":          []string{contracts.NetworkBoundaryControlledTransport, contracts.NetworkBoundaryDeploymentAttested},
			"remote_execution_guarantee":      "at_least_once",
			"hosted_boundary_proof":           false,
			"provider_behavior_qualification": false,
			"deployment_observation_kinds": []string{
				contracts.BoundaryQualificationCredentialResolution,
				contracts.BoundaryQualificationWorkloadIdentity,
				contracts.BoundaryQualificationMTLS,
				contracts.BoundaryQualificationDNSRebinding,
				contracts.BoundaryQualificationProxyFirewall,
				contracts.BoundaryQualificationProviderIdempotency,
				contracts.BoundaryQualificationExternalRecovery,
			},
			"signed_observation_required_for_external_effects": true,
		})
	case "signer-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification signer-list requires --deployment ID")
		}
		path := "/v1/qualification/signers?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		if cursor := valueArg(args[1:], "cursor", ""); cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		if limit := valueArg(args[1:], "limit", ""); limit != "" {
			path += "&limit=" + url.QueryEscape(limit)
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "signer-register", "signer-rotate":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		keyID := valueArg(args[1:], "key-id", "")
		publicKey := valueArg(args[1:], "public-key", "")
		validFrom := valueArg(args[1:], "valid-from", "")
		validUntil := valueArg(args[1:], "valid-until", "")
		if deployment == "" || keyID == "" || publicKey == "" || validFrom == "" || validUntil == "" {
			return errors.New("qualification signer registration requires --deployment ID --key-id ID --public-key HEX --valid-from RFC3339 --valid-until RFC3339")
		}
		from, err := time.Parse(time.RFC3339Nano, validFrom)
		if err != nil {
			return errors.New("qualification signer --valid-from must be RFC3339")
		}
		until, err := time.Parse(time.RFC3339Nano, validUntil)
		if err != nil {
			return errors.New("qualification signer --valid-until must be RFC3339")
		}
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "key_id": keyID, "public_key": publicKey, "valid_from": from.UTC(), "valid_until": until.UTC()}
		if supersedes := valueArg(args[1:], "supersedes", ""); supersedes != "" {
			body["supersedes_key_id"] = supersedes
		} else if args[0] == "signer-rotate" {
			return errors.New("qualification signer-rotate requires --supersedes KEY-ID")
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/signers", body, false, map[string]string{"X-Workspace-ID": workspace})
	case "signer-revoke":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		keyID := valueArg(args[1:], "key-id", "")
		if deployment == "" || keyID == "" {
			return errors.New("qualification signer-revoke requires --deployment ID --key-id ID")
		}
		path := "/v1/qualification/signers/" + url.PathEscape(keyID) + "/revoke?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodPost, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "snapshot-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification snapshot-list requires --deployment ID")
		}
		path := "/v1/qualification/snapshots?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "snapshot-sign":
		input := valueArg(args[1:], "file", "")
		output := valueArg(args[1:], "output", "")
		keyFile := valueArg(args[1:], "key-file", "")
		keyID := valueArg(args[1:], "key-id", "")
		if input == "" || output == "" || keyFile == "" || keyID == "" {
			return errors.New("qualification snapshot-sign requires --file PATH --output PATH --key-file PATH --key-id ID")
		}
		snapshot, _, err := readQualificationTrustSnapshot(input)
		if err != nil {
			return err
		}
		privateKey, err := readQualificationPrivateKey(keyFile)
		if err != nil {
			return err
		}
		signed, err := contracts.SignQualificationTrustSnapshot(snapshot, keyID, privateKey)
		if err != nil {
			return errors.New("qualification trust snapshot could not be signed")
		}
		if err := writeQualificationTrustSnapshot(output, signed); err != nil {
			return err
		}
		return c.print(map[string]any{"written": true, "file": output, "snapshot_hash": signed.SnapshotHash, "revision": signed.Revision, "signer_key_id": signed.SignerKeyID})
	case "snapshot-publish":
		input := valueArg(args[1:], "file", "")
		if input == "" {
			return errors.New("qualification snapshot-publish requires --file PATH")
		}
		snapshot, raw, err := readQualificationTrustSnapshot(input)
		if err != nil {
			return err
		}
		if snapshot.Signature == "" {
			return errors.New("qualification snapshot-publish requires a signed snapshot")
		}
		workspace := valueArg(args[1:], "workspace", snapshot.WorkspaceID)
		deployment := valueArg(args[1:], "deployment", snapshot.DeploymentID)
		idempotency := valueArg(args[1:], "idempotency", "qualification-snapshot:"+snapshot.SnapshotHash)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "idempotency_key": idempotency, "signed_snapshot": json.RawMessage(raw)}
		if source := valueArg(args[1:], "source", ""); source != "" {
			body["source_reference"] = source
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/snapshots", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "snapshot-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		snapshotID := valueArg(args[1:], "id", "")
		if deployment == "" || snapshotID == "" {
			return errors.New("qualification snapshot-get requires --deployment ID --id ID")
		}
		path := "/v1/qualification/snapshots/" + url.PathEscape(snapshotID) + "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		if valueArg(args[1:], "raw", "false") == "true" {
			path += "&raw=true"
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "snapshot-revoke":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		snapshotID := valueArg(args[1:], "id", "")
		if deployment == "" || snapshotID == "" {
			return errors.New("qualification snapshot-revoke requires --deployment ID --id ID")
		}
		path := "/v1/qualification/snapshots/" + url.PathEscape(snapshotID) + "/revoke?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodPost, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "import-authorized":
		path := valueArg(args[1:], "file", "")
		if path == "" {
			return errors.New("qualification import-authorized requires --file PATH")
		}
		signed, raw, err := readSignedQualificationBytes(path)
		if err != nil {
			return err
		}
		workspace := valueArg(args[1:], "workspace", signed.Bundle.Report.WorkspaceID)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification import-authorized requires --deployment ID")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-import:"+signed.Signature.SignedHash)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "target_hash": signed.Bundle.Report.TargetHash, "idempotency_key": idempotency, "signed_bundle": json.RawMessage(raw)}
		if source := valueArg(args[1:], "source", ""); source != "" {
			body["source_reference"] = source
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/import", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "imports-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification imports-list requires --deployment ID")
		}
		path := "/v1/qualification/imports?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "import-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		importID := valueArg(args[1:], "id", "")
		if deployment == "" || importID == "" {
			return errors.New("qualification import-get requires --deployment ID --id ID")
		}
		path := "/v1/qualification/imports/" + url.PathEscape(importID) + "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		if valueArg(args[1:], "raw", "false") == "true" {
			path += "&raw=true"
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "release-register":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseHash := valueArg(args[1:], "release-hash", "")
		targetHash := valueArg(args[1:], "target-hash", "")
		version := valueArg(args[1:], "version", "")
		if deployment == "" || releaseHash == "" || targetHash == "" || version == "" {
			return errors.New("qualification release-register requires --deployment ID --release-hash HASH --target-hash HASH --version VERSION")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-release:"+releaseHash)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_hash": releaseHash, "target_hash": targetHash, "version": version, "idempotency_key": idempotency}
		if commitHash := valueArg(args[1:], "commit-hash", ""); commitHash != "" {
			body["commit_hash"] = commitHash
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/releases", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "release-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification release-list requires --deployment ID")
		}
		path := "/v1/qualification/releases?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "release-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification release-get requires --deployment ID --id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "evidence-link", "evidence-replace":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		kind := valueArg(args[1:], "kind", "")
		importID := valueArg(args[1:], "import-id", "")
		if deployment == "" || releaseID == "" || kind == "" || importID == "" {
			return errors.New("qualification evidence-link requires --deployment ID --release-id ID --kind KIND --import-id ID")
		}
		supersedes := valueArg(args[1:], "supersedes-link-id", "")
		if args[0] == "evidence-replace" && supersedes == "" {
			return errors.New("qualification evidence-replace requires --supersedes-link-id ID")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-evidence:"+releaseID+":"+kind+":"+importID)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "kind": kind, "import_id": importID, "idempotency_key": idempotency}
		if supersedes != "" {
			body["supersedes_link_id"] = supersedes
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/releases/"+url.PathEscape(releaseID)+"/evidence", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "evidence-revoke":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		linkID := valueArg(args[1:], "link-id", "")
		kind := valueArg(args[1:], "kind", "")
		reason := valueArg(args[1:], "reason", "")
		if deployment == "" || releaseID == "" || linkID == "" || kind == "" || reason == "" {
			return errors.New("qualification evidence-revoke requires --deployment ID --release-id ID --link-id ID --kind KIND --reason REASON")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-evidence-revoke:"+linkID)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "link_id": linkID, "kind": kind, "reason": reason, "idempotency_key": idempotency}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/evidence/" + url.PathEscape(linkID) + "/revoke"
		return c.requestPrintWithHeaders(http.MethodPost, path, body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "evidence-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification evidence-list requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/evidence?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "release-gate":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification release-gate requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/gate?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		if kinds := valueArg(args[1:], "required-kinds", ""); kinds != "" {
			path += "&required_kinds=" + url.QueryEscape(kinds)
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "readiness-capture":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification readiness-capture requires --deployment ID --release-id ID")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-readiness:"+releaseID)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "idempotency_key": idempotency}
		if kinds := strings.TrimSpace(valueArg(args[1:], "required-kinds", "")); kinds != "" {
			values := make([]string, 0, len(strings.Split(kinds, ",")))
			for _, value := range strings.Split(kinds, ",") {
				if value = strings.TrimSpace(value); value != "" {
					values = append(values, value)
				}
			}
			body["required_kinds"] = values
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/snapshots", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "readiness-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification readiness-list requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/readiness/snapshots?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment) + "&release_id=" + url.QueryEscape(releaseID)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "readiness-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		snapshotID := valueArg(args[1:], "id", "")
		if deployment == "" || snapshotID == "" {
			return errors.New("qualification readiness-get requires --deployment ID --id ID")
		}
		path := "/v1/qualification/readiness/snapshots/" + url.PathEscape(snapshotID) + "?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "incident-annotate":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		snapshotID := valueArg(args[1:], "snapshot-id", "")
		code := valueArg(args[1:], "code", "")
		disposition := valueArg(args[1:], "disposition", "observed")
		if deployment == "" || releaseID == "" || snapshotID == "" || code == "" {
			return errors.New("qualification incident-annotate requires --deployment ID --release-id ID --snapshot-id ID --code CODE")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-incident:"+snapshotID+":"+code)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "snapshot_id": snapshotID, "code": code, "disposition": disposition, "idempotency_key": idempotency}
		if reference := valueArg(args[1:], "reference-hash", ""); reference != "" {
			body["reference_hash"] = reference
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		path := "/v1/qualification/readiness/snapshots/" + url.PathEscape(snapshotID) + "/annotations"
		return c.requestPrintWithHeaders(http.MethodPost, path, body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "incident-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		snapshotID := valueArg(args[1:], "snapshot-id", "")
		if deployment == "" || releaseID == "" || snapshotID == "" {
			return errors.New("qualification incident-list requires --deployment ID --release-id ID --snapshot-id ID")
		}
		path := "/v1/qualification/readiness/snapshots/" + url.PathEscape(snapshotID) + "/annotations?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment) + "&release_id=" + url.QueryEscape(releaseID)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "freshness-policy-set":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		ageRaw := valueArg(args[1:], "max-age-seconds", strconv.FormatInt(contracts.ReadinessFreshnessDefaultAge, 10))
		if deployment == "" {
			return errors.New("qualification freshness-policy-set requires --deployment ID")
		}
		age, err := strconv.ParseInt(ageRaw, 10, 64)
		if err != nil {
			return errors.New("qualification freshness-policy-set --max-age-seconds must be an integer")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-freshness-policy:"+deployment+":"+ageRaw)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "max_age_seconds": age, "require_ready": valueArg(args[1:], "require-ready", "false") == "true", "idempotency_key": idempotency}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/policies", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "freshness-policy-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification freshness-policy-get requires --deployment ID")
		}
		path := "/v1/qualification/readiness/policies/current?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "freshness-policy-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification freshness-policy-list requires --deployment ID")
		}
		path := "/v1/qualification/readiness/policies?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "readiness-review":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		leftID := valueArg(args[1:], "left-snapshot-id", "")
		rightID := valueArg(args[1:], "right-snapshot-id", "")
		if deployment == "" || releaseID == "" || leftID == "" || rightID == "" {
			return errors.New("qualification readiness-review requires --deployment ID --release-id ID --left-snapshot-id ID --right-snapshot-id ID")
		}
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "left_snapshot_id": leftID, "right_snapshot_id": rightID}
		if asOf := valueArg(args[1:], "as-of", ""); asOf != "" {
			parsed, err := time.Parse(time.RFC3339Nano, asOf)
			if err != nil {
				return errors.New("qualification readiness-review --as-of must be RFC3339")
			}
			body["as_of"] = parsed.UTC()
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/review", body, false, map[string]string{"X-Workspace-ID": workspace})
	case "retention-policy-set":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-policy-set requires --deployment ID")
		}
		defaultSeconds := int64(contracts.DefaultQualificationRetentionDays * 24 * 60 * 60)
		parseSeconds := func(flag, daysFlag string) (int64, error) {
			if raw := valueArg(args[1:], flag, ""); raw != "" {
				return strconv.ParseInt(raw, 10, 64)
			}
			if raw := valueArg(args[1:], daysFlag, ""); raw != "" {
				days, err := strconv.ParseInt(raw, 10, 64)
				return days * 24 * 60 * 60, err
			}
			return defaultSeconds, nil
		}
		snapshotSeconds, err := parseSeconds("snapshot-retention-seconds", "snapshot-retention-days")
		if err != nil {
			return errors.New("qualification retention-policy-set snapshot retention must be an integer")
		}
		incidentSeconds, err := parseSeconds("incident-retention-seconds", "incident-retention-days")
		if err != nil {
			return errors.New("qualification retention-policy-set incident retention must be an integer")
		}
		policySeconds, err := parseSeconds("policy-retention-seconds", "policy-retention-days")
		if err != nil {
			return errors.New("qualification retention-policy-set policy retention must be an integer")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-retention-policy:"+deployment+":"+strconv.FormatInt(snapshotSeconds, 10)+":"+strconv.FormatInt(incidentSeconds, 10)+":"+strconv.FormatInt(policySeconds, 10))
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "snapshot_retention_seconds": snapshotSeconds, "incident_retention_seconds": incidentSeconds, "policy_retention_seconds": policySeconds, "keep_latest_snapshots": integerArg(args[1:], "keep-latest-snapshots", 10), "keep_latest_incidents": integerArg(args[1:], "keep-latest-incidents", 10), "protect_incidents": valueArg(args[1:], "protect-incidents", "true") == "true", "idempotency_key": idempotency, "dry_run": valueArg(args[1:], "dry-run", "false") == "true"}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/retention/policies", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "retention-policy-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-policy-get requires --deployment ID")
		}
		path := "/v1/qualification/readiness/retention/policies/current?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "retention-policy-list":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-policy-list requires --deployment ID")
		}
		path := "/v1/qualification/readiness/retention/policies?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment)
		for _, flagName := range []string{"cursor", "limit"} {
			if value := valueArg(args[1:], flagName, ""); value != "" {
				path += "&" + flagName + "=" + url.QueryEscape(value)
			}
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "retention-sync":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-sync requires --deployment ID")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-retention-sync:"+deployment+":"+valueArg(args[1:], "cursor", "start"))
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "cursor": valueArg(args[1:], "cursor", ""), "batch_size": integerArg(args[1:], "batch", contracts.DefaultQualificationRetentionBatch), "dry_run": valueArg(args[1:], "dry-run", "false") == "true", "idempotency_key": idempotency}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/retention/metadata/sync", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "retention-plan":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-plan requires --deployment ID")
		}
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "cursor": valueArg(args[1:], "cursor", ""), "batch_size": integerArg(args[1:], "batch", contracts.DefaultQualificationRetentionBatch)}
		if asOf := valueArg(args[1:], "as-of", ""); asOf != "" {
			parsed, err := time.Parse(time.RFC3339Nano, asOf)
			if err != nil {
				return errors.New("qualification retention-plan --as-of must be RFC3339")
			}
			body["as_of"] = parsed.UTC()
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/retention/plan", body, false, map[string]string{"X-Workspace-ID": workspace})
	case "retention-recovery":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		if deployment == "" {
			return errors.New("qualification retention-recovery requires --deployment ID")
		}
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment}
		if asOf := valueArg(args[1:], "as-of", ""); asOf != "" {
			parsed, err := time.Parse(time.RFC3339Nano, asOf)
			if err != nil {
				return errors.New("qualification retention-recovery --as-of must be RFC3339")
			}
			body["as_of"] = parsed.UTC()
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/readiness/retention/recovery", body, false, map[string]string{"X-Workspace-ID": workspace})
	case "release-verify":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		releaseHash := valueArg(args[1:], "release-hash", "")
		targetHash := valueArg(args[1:], "target-hash", "")
		artifactKind := valueArg(args[1:], "artifact-kind", "release")
		artifactHash := valueArg(args[1:], "artifact-hash", "")
		attestationHash := valueArg(args[1:], "attestation-hash", "")
		gateHash := valueArg(args[1:], "gate-hash", "")
		expiresAt := valueArg(args[1:], "expires-at", "")
		if deployment == "" || releaseID == "" || releaseHash == "" || targetHash == "" || artifactHash == "" || attestationHash == "" || gateHash == "" || expiresAt == "" {
			return errors.New("qualification release-verify requires --deployment ID --release-id ID --release-hash HASH --target-hash HASH --artifact-hash HASH --attestation-hash HASH --gate-hash HASH --expires-at RFC3339")
		}
		expires, err := time.Parse(time.RFC3339Nano, expiresAt)
		if err != nil {
			return errors.New("qualification release-verify --expires-at must be RFC3339")
		}
		idempotency := valueArg(args[1:], "idempotency", "qualification-release-verification:"+releaseID+":"+artifactKind+":"+artifactHash)
		body := map[string]any{"workspace_id": workspace, "deployment_id": deployment, "release_id": releaseID, "release_hash": releaseHash, "target_hash": targetHash, "artifact_kind": artifactKind, "artifact_hash": artifactHash, "attestation_hash": attestationHash, "gate_hash": gateHash, "expires_at": expires.UTC(), "idempotency_key": idempotency}
		if source := valueArg(args[1:], "source", ""); source != "" {
			body["source_reference"] = source
		}
		if valueArg(args[1:], "dry-run", "false") == "true" {
			body["dry_run"] = true
		}
		return c.requestPrintWithHeaders(http.MethodPost, "/v1/qualification/releases/"+url.PathEscape(releaseID)+"/verification", body, false, map[string]string{"X-Workspace-ID": workspace, "Idempotency-Key": idempotency})
	case "verification-get":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		artifactKind := valueArg(args[1:], "artifact-kind", "release")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification verification-get requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/verification?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment) + "&artifact_kind=" + url.QueryEscape(artifactKind)
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "verification-revoke":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		artifactKind := valueArg(args[1:], "artifact-kind", "release")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification verification-revoke requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/verification/revoke?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment) + "&artifact_kind=" + url.QueryEscape(artifactKind)
		return c.requestPrintWithHeaders(http.MethodPost, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "release-admission":
		workspace := valueArg(args[1:], "workspace", c.workspace)
		deployment := valueArg(args[1:], "deployment", "")
		releaseID := valueArg(args[1:], "release-id", "")
		artifactKind := valueArg(args[1:], "artifact-kind", "release")
		if deployment == "" || releaseID == "" {
			return errors.New("qualification release-admission requires --deployment ID --release-id ID")
		}
		path := "/v1/qualification/releases/" + url.PathEscape(releaseID) + "/admission?workspace_id=" + url.QueryEscape(workspace) + "&deployment_id=" + url.QueryEscape(deployment) + "&artifact_kind=" + url.QueryEscape(artifactKind)
		if artifactHash := valueArg(args[1:], "artifact-hash", ""); artifactHash != "" {
			path += "&artifact_hash=" + url.QueryEscape(artifactHash)
		}
		return c.requestPrintWithHeaders(http.MethodGet, path, nil, false, map[string]string{"X-Workspace-ID": workspace})
	case "run":
		path := valueArg(args[1:], "file", "")
		if path == "" {
			return errors.New("qualification run requires --file PATH")
		}
		targetHash := valueArg(args[1:], "target-hash", "")
		if targetHash == "" {
			targetHash = contracts.HashStrings("fornix", "portable-qualification")
		}
		result, err := qualification.Run(context.Background(),
			valueArg(args[1:], "run-id", "portable-qualification"),
			valueArg(args[1:], "workspace", ""),
			targetHash,
			qualification.OfflineChecks(),
			qualification.RunnerOptions{
				RunnerVersion:    valueArg(args[1:], "runner-version", qualification.DefaultRunnerVersion),
				CommitHash:       valueArg(args[1:], "commit-hash", ""),
				EnvironmentNames: splitCSV(valueArg(args[1:], "env-names", "")),
			})
		if err != nil {
			return err
		}
		if err := writeQualificationBundle(path, result.Bundle); err != nil {
			return err
		}
		return c.print(map[string]any{"written": true, "file": path, "report_hash": result.Bundle.Report.ReportHash, "manifest_hash": result.Bundle.Manifest.ManifestHash, "outcome": result.Bundle.Report.Outcome})
	case "merge":
		path := valueArg(args[1:], "file", "")
		inputs := splitCSV(valueArg(args[1:], "inputs", ""))
		if path == "" || len(inputs) == 0 {
			return errors.New("qualification merge requires --file PATH --inputs PATH[,PATH...]")
		}
		bundles := make([]contracts.QualificationBundle, 0, len(inputs))
		for _, input := range inputs {
			bundle, err := readQualificationBundle(input)
			if err != nil {
				return err
			}
			bundles = append(bundles, bundle)
		}
		workspace := valueArg(args[1:], "workspace", bundles[0].Report.WorkspaceID)
		targetHash := valueArg(args[1:], "target-hash", bundles[0].Report.TargetHash)
		result, err := qualification.MergeBundles(context.Background(), valueArg(args[1:], "run-id", "portable-qualification-merge"), workspace, targetHash, bundles, qualification.RunnerOptions{
			RunnerVersion:    valueArg(args[1:], "runner-version", qualification.DefaultRunnerVersion),
			CommitHash:       valueArg(args[1:], "commit-hash", ""),
			EnvironmentNames: splitCSV(valueArg(args[1:], "env-names", "")),
		})
		if err != nil {
			return err
		}
		if err := writeQualificationBundle(path, result.Bundle); err != nil {
			return err
		}
		return c.print(map[string]any{"written": true, "file": path, "report_hash": result.Bundle.Report.ReportHash, "manifest_hash": result.Bundle.Manifest.ManifestHash, "outcome": result.Bundle.Report.Outcome})
	case "boundary-sign":
		input := valueArg(args[1:], "file", "")
		output := valueArg(args[1:], "output", "")
		keyFile := valueArg(args[1:], "key-file", "")
		keyID := valueArg(args[1:], "key-id", "")
		if input == "" || output == "" || keyFile == "" || keyID == "" {
			return errors.New("qualification boundary-sign requires --file PATH --output PATH --key-file PATH --key-id ID")
		}
		externalEffect := valueArg(args[1:], "external-effect", "false") == "true"
		asOf, err := qualification.ParseAsOf(valueArg(args[1:], "as-of", ""))
		if err != nil {
			return err
		}
		if externalEffect && asOf.IsZero() {
			asOf = time.Now().UTC()
		}
		if err := refuseExistingQualificationOutput(output, valueArg(args[1:], "force", "false") == "true"); err != nil {
			return err
		}
		bundle, err := readQualificationBundle(input)
		if err != nil {
			return err
		}
		privateKey, err := readQualificationPrivateKey(keyFile)
		if err != nil {
			return err
		}
		signed, err := qualification.PublishBoundaryBundle(bundle, keyID, privateKey, qualification.BoundaryPublisherOptions{RequireExternalEffect: externalEffect, AsOf: asOf})
		if err != nil {
			return errors.New("boundary qualification bundle could not be published")
		}
		if err := writeSignedQualificationBundle(output, signed); err != nil {
			return err
		}
		summary, err := qualification.Summary(signed, externalEffect)
		if err != nil {
			return errors.New("boundary qualification bundle could not be summarized")
		}
		return c.print(map[string]any{"written": true, "file": output, "signed_hash": summary.SignedHash, "observation_hash": summary.ObservationHash, "report_hash": summary.ReportHash, "manifest_hash": summary.ManifestHash, "evidence_count": summary.EvidenceCount, "external_effect": summary.ExternalEffect, "expires_at": summary.ExpiresAtRFC3339})
	case "boundary-validate":
		input := valueArg(args[1:], "file", "")
		if input == "" {
			return errors.New("qualification boundary-validate requires --file PATH")
		}
		externalEffect := valueArg(args[1:], "external-effect", "false") == "true"
		asOf, err := qualification.ParseAsOf(valueArg(args[1:], "as-of", ""))
		if err != nil {
			return err
		}
		if externalEffect && asOf.IsZero() {
			asOf = time.Now().UTC()
		}
		signed, err := readSignedQualificationBundle(input)
		if err != nil {
			return err
		}
		if err := qualification.ValidateBoundaryBundle(signed, qualification.BoundaryPublisherOptions{RequireExternalEffect: externalEffect, AsOf: asOf}); err != nil {
			return errors.New("boundary qualification bundle failed validation")
		}
		summary, err := qualification.Summary(signed, externalEffect)
		if err != nil {
			return errors.New("boundary qualification bundle could not be summarized")
		}
		return c.print(map[string]any{"verified": true, "signed_hash": summary.SignedHash, "observation_hash": summary.ObservationHash, "report_hash": summary.ReportHash, "manifest_hash": summary.ManifestHash, "evidence_count": summary.EvidenceCount, "external_effect": summary.ExternalEffect, "expires_at": summary.ExpiresAtRFC3339})
	case "sign":
		input := valueArg(args[1:], "file", "")
		output := valueArg(args[1:], "output", "")
		keyFile := valueArg(args[1:], "key-file", "")
		keyID := valueArg(args[1:], "key-id", "")
		if input == "" || output == "" || keyFile == "" || keyID == "" {
			return errors.New("qualification sign requires --file PATH --output PATH --key-file PATH --key-id ID")
		}
		bundle, err := readQualificationBundle(input)
		if err != nil {
			return err
		}
		privateKey, err := readQualificationPrivateKey(keyFile)
		if err != nil {
			return err
		}
		signed, err := contracts.SignQualificationBundle(bundle, keyID, privateKey)
		if err != nil {
			return errors.New("qualification bundle could not be signed")
		}
		if err := writeSignedQualificationBundle(output, signed); err != nil {
			return err
		}
		return c.print(map[string]any{"written": true, "file": output, "key_id": signed.Signature.KeyID, "signed_hash": signed.Signature.SignedHash, "observation_hash": signed.ObservationHash, "report_hash": signed.Bundle.Report.ReportHash, "manifest_hash": signed.Bundle.Manifest.ManifestHash})
	case "validate-signed", "import":
		path := valueArg(args[1:], "file", "")
		if path == "" {
			return fmt.Errorf("qualification %s requires --file PATH", args[0])
		}
		signed, err := readSignedQualificationBundle(path)
		if err != nil {
			return err
		}
		expectedPublicKey, err := readQualificationPublicKey(valueArg(args[1:], "public-key-file", ""))
		if err != nil {
			return err
		}
		if err := qualification.ValidateSignedBundle(signed, valueArg(args[1:], "workspace", ""), valueArg(args[1:], "target-hash", ""), valueArg(args[1:], "key-id", ""), expectedPublicKey); err != nil {
			return errors.New("signed qualification bundle failed trust or scope validation")
		}
		return c.print(map[string]any{"verified": true, "imported": args[0] == "import", "key_id": signed.Signature.KeyID, "signed_hash": signed.Signature.SignedHash, "observation_hash": signed.ObservationHash, "workspace_id": signed.Bundle.Report.WorkspaceID, "target_hash": signed.Bundle.Report.TargetHash, "report_hash": signed.Bundle.Report.ReportHash, "manifest_hash": signed.Bundle.Manifest.ManifestHash, "outcome": signed.Bundle.Report.Outcome})
	case "merge-signed":
		path := valueArg(args[1:], "file", "")
		inputs := splitCSV(valueArg(args[1:], "inputs", ""))
		keyFile := valueArg(args[1:], "key-file", "")
		keyID := valueArg(args[1:], "key-id", "")
		if path == "" || len(inputs) == 0 || keyFile == "" || keyID == "" {
			return errors.New("qualification merge-signed requires --file PATH --inputs PATH[,PATH...] --key-file PATH --key-id ID")
		}
		signed := make([]contracts.SignedQualificationBundle, 0, len(inputs))
		for _, input := range inputs {
			value, err := readSignedQualificationBundle(input)
			if err != nil {
				return err
			}
			signed = append(signed, value)
		}
		workspace := valueArg(args[1:], "workspace", signed[0].Bundle.Report.WorkspaceID)
		targetHash := valueArg(args[1:], "target-hash", signed[0].Bundle.Report.TargetHash)
		expectedPublicKey, err := readQualificationPublicKey(valueArg(args[1:], "public-key-file", ""))
		if err != nil {
			return err
		}
		result, err := qualification.MergeSignedBundles(context.Background(), valueArg(args[1:], "run-id", "qualification-signed-merge"), workspace, targetHash, signed, qualification.RunnerOptions{RunnerVersion: valueArg(args[1:], "runner-version", qualification.DefaultRunnerVersion), CommitHash: valueArg(args[1:], "commit-hash", ""), EnvironmentNames: splitCSV(valueArg(args[1:], "env-names", ""))}, valueArg(args[1:], "trusted-key-id", ""), expectedPublicKey)
		if err != nil {
			return err
		}
		privateKey, err := readQualificationPrivateKey(keyFile)
		if err != nil {
			return err
		}
		merged, err := contracts.SignQualificationBundle(result.Bundle, keyID, privateKey)
		if err != nil {
			return errors.New("merged qualification bundle could not be signed")
		}
		if err := writeSignedQualificationBundle(path, merged); err != nil {
			return err
		}
		return c.print(map[string]any{"written": true, "file": path, "key_id": merged.Signature.KeyID, "signed_hash": merged.Signature.SignedHash, "observation_hash": merged.ObservationHash, "report_hash": merged.Bundle.Report.ReportHash, "manifest_hash": merged.Bundle.Manifest.ManifestHash, "outcome": merged.Bundle.Report.Outcome})
	case "hash-signed":
		path := valueArg(args[1:], "file", "")
		if path == "" {
			return errors.New("qualification hash-signed requires --file PATH")
		}
		signed, err := readSignedQualificationBundle(path)
		if err != nil {
			return err
		}
		return c.print(map[string]any{"verified": true, "key_id": signed.Signature.KeyID, "signed_hash": signed.Signature.SignedHash, "observation_hash": signed.ObservationHash, "report_hash": signed.Bundle.Report.ReportHash, "manifest_hash": signed.Bundle.Manifest.ManifestHash})
	case "validate", "hash":
		path := valueArg(args[1:], "file", "")
		if path == "" {
			return errors.New("qualification requires --file PATH")
		}
		bundle, bundleErr := readQualificationBundle(path)
		var report contracts.QualificationReport
		if bundleErr == nil {
			report = bundle.Report
		} else {
			var err error
			report, err = readQualificationReport(path)
			if err != nil {
				return err
			}
		}
		if args[0] == "hash" {
			value := map[string]any{"verified": true, "report_hash": report.ReportHash}
			if bundleErr == nil {
				value["manifest_hash"] = bundle.Manifest.ManifestHash
			}
			return c.print(value)
		}
		value := map[string]any{
			"verified":                true,
			"run_id":                  report.RunID,
			"workspace_id":            report.WorkspaceID,
			"target_hash":             report.TargetHash,
			"outcome":                 report.Outcome,
			"case_count":              len(report.Cases),
			"boundary_evidence_count": len(report.BoundaryEvidence),
			"recovery_count":          len(report.RecoveryDrills),
			"report_hash":             report.ReportHash,
		}
		if bundleErr == nil {
			value["manifest_hash"] = bundle.Manifest.ManifestHash
		}
		return c.print(value)
	case "help", "--help", "-h":
		return errors.New("usage: fornix qualification adapter-conformance|external-boundary|boundary-sign|boundary-validate|run|merge|sign|validate-signed|import|merge-signed|validate|hash|hash-signed|signer-list|signer-register|signer-rotate|signer-revoke|snapshot-list|snapshot-sign|snapshot-publish|snapshot-get|snapshot-revoke|import-authorized|imports-list|import-get|release-register|release-list|release-get|evidence-link|evidence-replace|evidence-revoke|evidence-list|release-gate|refresh|refresh-plan|refresh-list|refresh-get|readiness-capture|readiness-list|readiness-get|incident-annotate|incident-list|freshness-policy-set|freshness-policy-get|freshness-policy-list|readiness-review|retention-policy-set|retention-policy-get|retention-policy-list|retention-sync|retention-plan|retention-recovery|release-verify|verification-get|verification-revoke|release-admission")
	default:
		return fmt.Errorf("unknown qualification command %q", args[0])
	}
}

type qualificationRefreshItemsFile struct {
	Items []contracts.QualificationRefreshItem `json:"items"`
}

func readQualificationRefreshScheduleRequest(path string) (contracts.QualificationRefreshScheduleRequest, error) {
	raw, err := readBoundedJSONFile(path, 32<<10)
	if err != nil {
		return contracts.QualificationRefreshScheduleRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request contracts.QualificationRefreshScheduleRequest
	if err := decoder.Decode(&request); err != nil {
		return contracts.QualificationRefreshScheduleRequest{}, errors.New("qualification schedule file must contain one JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.QualificationRefreshScheduleRequest{}, errors.New("qualification schedule file contains trailing data")
	}
	if len(request.RequiredEvidenceKinds) > contracts.MaxDeploymentEvidenceKinds || len(request.RequiredRecoveryDrills) > contracts.MaxQualificationRefreshScheduleDrills {
		return contracts.QualificationRefreshScheduleRequest{}, errors.New("qualification schedule file exceeds bounded requirement limits")
	}
	return request, nil
}

func readQualificationRefreshItems(path string) ([]contracts.QualificationRefreshItem, error) {
	raw, err := readBoundedJSONFile(path, 16<<10)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input qualificationRefreshItemsFile
	if err := decoder.Decode(&input); err != nil {
		return nil, errors.New("qualification refresh items file must contain an object with an items array")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("qualification refresh items file contains trailing data")
	}
	if len(input.Items) == 0 || len(input.Items) > contracts.MaxQualificationRefreshItems {
		return nil, fmt.Errorf("qualification refresh items file requires between 1 and %d items", contracts.MaxQualificationRefreshItems)
	}
	for index := range input.Items {
		if err := input.Items[index].Normalize(index); err != nil {
			return nil, err
		}
	}
	return input.Items, nil
}

func writeSignedQualificationBundle(path string, bundle contracts.SignedQualificationBundle) error {
	if err := bundle.Verify(); err != nil {
		return errors.New("signed qualification bundle failed validation")
	}
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil || len(raw) > contracts.MaxQualificationReportBytes {
		return errors.New("signed qualification bundle exceeds the bounded output limit")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".fornix-signed-qualification-*.tmp")
	if err != nil {
		return errors.New("signed qualification bundle could not be created")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return errors.New("signed qualification bundle permissions could not be set")
	}
	if _, err := temporary.Write(raw); err != nil {
		temporary.Close()
		return errors.New("signed qualification bundle could not be written")
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return errors.New("signed qualification bundle could not be synced")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("signed qualification bundle could not be closed")
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return errors.New("signed qualification bundle could not be committed")
	}
	return nil
}

func refuseExistingQualificationOutput(path string, force bool) error {
	if force {
		return nil
	}
	if _, err := os.Stat(path); err == nil {
		return errors.New("qualification output already exists; pass --force true to replace it")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("qualification output could not be inspected")
	}
	return nil
}

func readSignedQualificationBundle(path string) (contracts.SignedQualificationBundle, error) {
	signed, _, err := readSignedQualificationBytes(path)
	return signed, err
}

func readSignedQualificationBytes(path string) (contracts.SignedQualificationBundle, []byte, error) {
	raw, err := readBoundedQualificationFile(path)
	if err != nil {
		return contracts.SignedQualificationBundle{}, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var signed contracts.SignedQualificationBundle
	if err := decoder.Decode(&signed); err != nil {
		return contracts.SignedQualificationBundle{}, nil, errors.New("signed qualification bundle is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.SignedQualificationBundle{}, nil, errors.New("signed qualification bundle contains trailing data")
	}
	if err := signed.Verify(); err != nil {
		return contracts.SignedQualificationBundle{}, nil, errors.New("signed qualification bundle failed verification")
	}
	return signed, raw, nil
}

func readQualificationTrustSnapshot(path string) (contracts.QualificationTrustSnapshot, []byte, error) {
	raw, err := readBoundedQualificationFile(path)
	if err != nil {
		return contracts.QualificationTrustSnapshot{}, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var snapshot contracts.QualificationTrustSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return contracts.QualificationTrustSnapshot{}, nil, errors.New("qualification trust snapshot is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.QualificationTrustSnapshot{}, nil, errors.New("qualification trust snapshot contains trailing data")
	}
	if err := snapshot.Normalize(); err != nil {
		return contracts.QualificationTrustSnapshot{}, nil, errors.New("qualification trust snapshot failed validation")
	}
	if len(raw) > contracts.MaxQualificationTrustSnapshotBytes {
		return contracts.QualificationTrustSnapshot{}, nil, errors.New("qualification trust snapshot exceeds the bounded input limit")
	}
	return snapshot, raw, nil
}

func writeQualificationTrustSnapshot(path string, snapshot contracts.QualificationTrustSnapshot) error {
	if snapshot.Signature == "" || snapshot.VerifyWithKey("", nil, time.Now().UTC()) != nil {
		return errors.New("qualification trust snapshot failed verification")
	}
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil || len(raw) > contracts.MaxQualificationTrustSnapshotBytes {
		return errors.New("qualification trust snapshot exceeds the bounded output limit")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".fornix-trust-snapshot-*.tmp")
	if err != nil {
		return errors.New("qualification trust snapshot could not be created")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("qualification trust snapshot permissions could not be set")
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return errors.New("qualification trust snapshot could not be written")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("qualification trust snapshot could not be synced")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("qualification trust snapshot could not be closed")
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return errors.New("qualification trust snapshot could not be committed")
	}
	return nil
}

func readQualificationPrivateKey(path string) (ed25519.PrivateKey, error) {
	if path == "" {
		return nil, errors.New("qualification private key file is required")
	}
	raw, err := readQualificationKeyBytes(path, ed25519.PrivateKeySize)
	if err != nil {
		return nil, errors.New("qualification private key could not be read")
	}
	if len(raw) == ed25519.PrivateKeySize {
		return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
	}
	decoded, err := hex.DecodeString(string(bytes.TrimSpace(raw)))
	if err != nil || len(decoded) != ed25519.PrivateKeySize {
		return nil, errors.New("qualification private key has invalid format")
	}
	return ed25519.PrivateKey(decoded), nil
}

func readQualificationPublicKey(path string) (ed25519.PublicKey, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readQualificationKeyBytes(path, ed25519.PublicKeySize)
	if err != nil {
		return nil, errors.New("qualification public key could not be read")
	}
	if len(raw) == ed25519.PublicKeySize {
		return ed25519.PublicKey(append([]byte(nil), raw...)), nil
	}
	decoded, err := hex.DecodeString(string(bytes.TrimSpace(raw)))
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, errors.New("qualification public key has invalid format")
	}
	return ed25519.PublicKey(decoded), nil
}

func readQualificationKeyBytes(path string, rawLimit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, int64(rawLimit*2+1)))
	if err != nil || len(raw) > rawLimit*2 {
		return nil, errors.New("qualification key exceeds the bounded input limit")
	}
	return raw, nil
}

func writeQualificationBundle(path string, bundle contracts.QualificationBundle) error {
	if err := bundle.Normalize(); err != nil {
		return errors.New("qualification bundle failed validation")
	}
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil || len(raw) > contracts.MaxQualificationReportBytes {
		return errors.New("qualification bundle exceeds the bounded output limit")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".fornix-qualification-*.tmp")
	if err != nil {
		return errors.New("qualification bundle could not be created")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return errors.New("qualification bundle permissions could not be set")
	}
	if _, err := temporary.Write(raw); err != nil {
		temporary.Close()
		return errors.New("qualification bundle could not be written")
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return errors.New("qualification bundle could not be synced")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("qualification bundle could not be closed")
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return errors.New("qualification bundle could not be committed")
	}
	return nil
}

func readQualificationBundle(path string) (contracts.QualificationBundle, error) {
	raw, err := readBoundedQualificationFile(path)
	if err != nil {
		return contracts.QualificationBundle{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var bundle contracts.QualificationBundle
	if err := decoder.Decode(&bundle); err != nil {
		return contracts.QualificationBundle{}, errors.New("qualification bundle is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.QualificationBundle{}, errors.New("qualification bundle contains trailing data")
	}
	if err := bundle.Normalize(); err != nil {
		return contracts.QualificationBundle{}, errors.New("qualification bundle failed validation")
	}
	return bundle, nil
}

func readQualificationReport(path string) (contracts.QualificationReport, error) {
	raw, err := readBoundedQualificationFile(path)
	if err != nil {
		return contracts.QualificationReport{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var report contracts.QualificationReport
	if err := decoder.Decode(&report); err != nil {
		return contracts.QualificationReport{}, errors.New("qualification report is invalid")
	}
	if report.ReportHash == "" {
		return contracts.QualificationReport{}, errors.New("qualification report is missing report_hash")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.QualificationReport{}, errors.New("qualification report contains trailing data")
	}
	if err := report.Normalize(); err != nil {
		return contracts.QualificationReport{}, errors.New("qualification report failed validation")
	}
	return report, nil
}

func readBoundedQualificationFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("qualification evidence could not be opened")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, contracts.MaxQualificationReportBytes+1))
	if err != nil || len(raw) > contracts.MaxQualificationReportBytes {
		return nil, errors.New("qualification evidence exceeds the bounded input limit")
	}
	return raw, nil
}
