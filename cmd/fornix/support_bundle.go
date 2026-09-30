package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/omaveda/fornix/internal/profile"
	"github.com/omaveda/fornix/internal/version"
)

const (
	localSupportBundleSchemaVersion = 1
	maxLocalSupportBundleBytes      = 4 << 10
)

// localSupportBundle is an allowlist: never serialize profile.Metadata or
// diagnostic errors directly into a bundle that users may share publicly.
type localSupportBundle struct {
	SchemaVersion     int       `json:"schema_version"`
	FornixVersion     string    `json:"fornix_version"`
	GeneratedAt       time.Time `json:"generated_at"`
	Redacted          bool      `json:"redacted"`
	ProfileState      string    `json:"profile_state"`
	ProfileSchema     int       `json:"profile_schema_version,omitempty"`
	ServerConfigured  bool      `json:"server_configured"`
	RuntimeConfigured bool      `json:"runtime_configured"`
	Migration         int       `json:"migration,omitempty"`
}

func newLocalSupportBundle(metadata profile.Metadata, state string, generatedAt time.Time) localSupportBundle {
	bundle := localSupportBundle{
		SchemaVersion: localSupportBundleSchemaVersion,
		FornixVersion: version.Current().Version,
		GeneratedAt:   generatedAt.UTC(),
		Redacted:      true,
		ProfileState:  state,
	}
	if state == "initialized" {
		bundle.ProfileSchema = metadata.SchemaVersion
		bundle.ServerConfigured = metadata.ServerURL != ""
		bundle.RuntimeConfigured = metadata.RuntimeVersion != ""
		bundle.Migration = metadata.Migration
	}
	return bundle
}

func marshalLocalSupportBundle(bundle localSupportBundle) ([]byte, error) {
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode support bundle: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxLocalSupportBundleBytes {
		return nil, errors.New("support bundle exceeds the local size limit")
	}
	return data, nil
}

// writeLocalSupportBundle creates a private new file. It refuses to overwrite
// an existing path because os.WriteFile's mode argument does not tighten the
// permissions of an existing file.
func writeLocalSupportBundle(path string, data []byte) error {
	if len(data) > maxLocalSupportBundleBytes {
		return errors.New("support bundle exceeds the local size limit")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("support bundle destination already exists; choose a new path")
		}
		return fmt.Errorf("create support bundle: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("secure support bundle: %w", err)
	}
	n, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("write support bundle: %w", err)
	}
	if n != len(data) {
		return errors.New("write support bundle: incomplete write")
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync support bundle: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close support bundle: %w", err)
	}
	complete = true
	return nil
}
