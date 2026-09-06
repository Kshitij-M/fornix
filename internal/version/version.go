package version

import (
	"encoding/json"
	"fmt"
	"runtime"
)

// Version is the default development version. Release builds replace it with
// the Git tag through GoReleaser's -ldflags setting.
var Version = "0.10.1"

// Commit and Date are injected by release builds when available. Empty values
// are valid for local source builds and are rendered as "unknown".
var (
	Commit = ""
	Date   = ""
)

// Info is the complete, non-secret build identity shown by `fornix version`.
// It intentionally contains only reproducible build and compatibility data.
type Info struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	Commit           string `json:"commit"`
	BuildDate        string `json:"build_date"`
	OS               string `json:"os"`
	Arch             string `json:"arch"`
	SchemaVersion    int    `json:"schema_version"`
	SchemaCompatible string `json:"schema_compatibility"`
}

// Current returns the build identity for the current binary.
func Current() Info {
	commit := Commit
	if commit == "" {
		commit = "unknown"
	}
	date := Date
	if date == "" {
		date = "unknown"
	}
	return Info{
		Name:             "fornix",
		Version:          Version,
		Commit:           commit,
		BuildDate:        date,
		OS:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		SchemaVersion:    1,
		SchemaCompatible: "current",
	}
}

// String returns a stable human-readable build identity.
func (i Info) String() string {
	return fmt.Sprintf("%s %s (%s; %s/%s; schema %d)", i.Name, i.Version, i.Commit, i.OS, i.Arch, i.SchemaVersion)
}

// JSON returns a stable machine-readable build identity.
func (i Info) JSON() ([]byte, error) {
	return json.Marshal(i)
}
