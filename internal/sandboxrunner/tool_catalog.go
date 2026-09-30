package sandboxrunner

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	// ErrToolCatalogUnavailable means no operator-registered snapshot matches
	// the request's exact workspace-independent tool identity.
	ErrToolCatalogUnavailable = errors.New("sandbox tool definition is unavailable")
	// ErrInvocationDenied means the request is not a valid invocation of its
	// registered tool. Details are intentionally omitted to avoid echoing argv.
	ErrInvocationDenied = errors.New("sandbox tool invocation is denied")
)

type toolCatalogKey struct {
	id             string
	definitionHash string
}

type toolCatalogEntry struct {
	definition   contracts.ToolDefinition
	profileHash  string
	runnerPolicy contracts.SandboxRunnerProfile
}

// ToolCatalog is an immutable-snapshot catalog local to the trusted runner.
// A request can select a registered definition by its exact hashes but cannot
// add or modify entries. Re-registering an identical snapshot is idempotent.
type ToolCatalog struct {
	mu      sync.RWMutex
	entries map[toolCatalogKey]toolCatalogEntry
}

// NewToolCatalog validates every definition before making the catalog
// available. Multiple historical/policy snapshots for one tool ID are
// allowed because each has a distinct content hash.
func NewToolCatalog(definitions []contracts.ToolDefinition) (*ToolCatalog, error) {
	catalog := &ToolCatalog{entries: make(map[toolCatalogKey]toolCatalogEntry, len(definitions))}
	for _, definition := range definitions {
		if err := catalog.Register(definition); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

// Register adds an operator-selected OCI definition snapshot. It never
// resolves executables from PATH and rejects non-OCI profiles.
func (c *ToolCatalog) Register(definition contracts.ToolDefinition) error {
	if c == nil {
		return fmt.Errorf("%w: catalog is required", ErrToolCatalogUnavailable)
	}
	if err := definition.Normalize(); err != nil || definition.Sandbox.Backend != string(contracts.SandboxBackendOCI) {
		return fmt.Errorf("%w: invalid OCI tool snapshot", ErrToolCatalogUnavailable)
	}
	definitionHash, err := definition.Hash()
	if err != nil {
		return fmt.Errorf("%w: invalid OCI tool identity", ErrToolCatalogUnavailable)
	}
	profileHash, err := definition.Sandbox.Hash()
	if err != nil || profileHash == "" {
		return fmt.Errorf("%w: invalid OCI profile identity", ErrToolCatalogUnavailable)
	}
	runnerPolicy, err := contracts.NewSandboxRunnerProfile(definition.Sandbox)
	if err != nil {
		return fmt.Errorf("%w: invalid OCI runner policy", ErrToolCatalogUnavailable)
	}
	key := toolCatalogKey{id: definition.ID, definitionHash: definitionHash}
	entry := toolCatalogEntry{
		definition:   cloneToolDefinition(definition),
		profileHash:  profileHash,
		runnerPolicy: runnerPolicy,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[toolCatalogKey]toolCatalogEntry)
	}
	if previous, exists := c.entries[key]; exists {
		if previous.profileHash == entry.profileHash {
			return nil
		}
		// The profile is included in the definition hash. Reaching this branch
		// indicates an inconsistent/corrupt catalog identity.
		return ErrToolCatalogUnavailable
	}
	c.entries[key] = entry
	return nil
}

// ResolvedInvocation contains only the command and container-relative
// working directory derived from a trusted definition and resolved mount.
// Argv includes the catalog executable at index zero. Environment is always
// empty for the currently supported offline runner profile.
type ResolvedInvocation struct {
	ToolID           string
	DefinitionHash   string
	ProfileHash      string
	Executable       string
	Argv             []string
	Environment      []string
	WorkingDirectory string
}

// Resolve verifies the complete request against one catalog snapshot and a
// workspace-bound mount. It returns no host path and never logs user argv.
func (c *ToolCatalog) Resolve(request contracts.SandboxRunnerRequest, mount *ResolvedMount) (ResolvedInvocation, error) {
	if c == nil || mount == nil {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	request.Argv = append([]string(nil), request.Argv...)
	if request.Environment != nil {
		environment := request.Environment
		request.Environment = make(map[string]string, len(environment))
		for key, value := range environment {
			request.Environment[key] = value
		}
	}
	if err := request.Normalize(); err != nil || request.WorkspaceMount != mount.Reference() {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	key := toolCatalogKey{id: request.ToolID, definitionHash: request.ToolDefinitionHash}
	c.mu.RLock()
	entry, found := c.entries[key]
	c.mu.RUnlock()
	if !found {
		return ResolvedInvocation{}, ErrToolCatalogUnavailable
	}
	definition := cloneToolDefinition(entry.definition)
	if definition.ID != request.ToolID || definition.Sandbox.Backend != string(contracts.SandboxBackendOCI) ||
		request.SandboxProfileHash != entry.profileHash || request.Profile.Hash() != entry.runnerPolicy.Hash() {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	// Recompute both identities at the trust boundary; the map key is only an
	// index, not proof that the in-memory definition has not been corrupted.
	definitionHash, err := definition.Hash()
	if err != nil || definitionHash != request.ToolDefinitionHash {
		return ResolvedInvocation{}, ErrToolCatalogUnavailable
	}
	profileHash, err := definition.Sandbox.Hash()
	if err != nil || profileHash != request.SandboxProfileHash {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	argv := make([]string, 0, len(request.Argv)+1)
	argv = append(argv, definition.Executable)
	argv = append(argv, request.Argv...)
	if len(argv) > request.Profile.MaxArgCount || len(definition.Executable) > request.Profile.MaxArgBytes ||
		!matchesPrefix(request.Argv, definition.ArgvPrefix) {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	workdir := request.WorkingDirectory
	if workdir == "." {
		workdir = ""
	}
	containerWorkdir, err := mount.ContainerWorkdir(workdir)
	if err != nil {
		return ResolvedInvocation{}, ErrInvocationDenied
	}
	for _, index := range definition.PathArgvIndexes {
		argIndex := index - 1 // definition indexes include argv[0], the executable.
		if argIndex < 0 || argIndex >= len(request.Argv) || mount.validatePathArgument(workdir, request.Argv[argIndex]) != nil {
			return ResolvedInvocation{}, ErrInvocationDenied
		}
	}
	return ResolvedInvocation{
		ToolID: definition.ID, DefinitionHash: definitionHash,
		ProfileHash: profileHash, Executable: definition.Executable,
		Argv: argv, Environment: nil, WorkingDirectory: containerWorkdir,
	}, nil
}

func matchesPrefix(argv, prefix []string) bool {
	if len(argv) < len(prefix) {
		return false
	}
	for i := range prefix {
		if argv[i] != prefix[i] {
			return false
		}
	}
	return true
}

func cloneToolDefinition(definition contracts.ToolDefinition) contracts.ToolDefinition {
	definition.ArgvPrefix = append([]string(nil), definition.ArgvPrefix...)
	definition.PathArgvIndexes = append([]int(nil), definition.PathArgvIndexes...)
	definition.AllowedEnvKeys = append([]string(nil), definition.AllowedEnvKeys...)
	definition.Sandbox.RequiredCapabilities = append([]contracts.SandboxCapability(nil), definition.Sandbox.RequiredCapabilities...)
	return definition
}

func (m *ResolvedMount) validatePathArgument(workingDirectory, argument string) error {
	if m == nil {
		return ErrWorkdirUnavailable
	}
	if argument == "" || strings.Contains(argument, `\`) || strings.IndexByte(argument, 0) >= 0 || path.IsAbs(argument) || path.Clean(argument) != argument || argument == ".." || strings.HasPrefix(argument, "../") {
		return ErrWorkdirUnavailable
	}
	workingDirectory = strings.TrimSpace(workingDirectory)
	if workingDirectory == "" {
		workingDirectory = "."
	}
	if strings.Contains(workingDirectory, `\`) || strings.IndexByte(workingDirectory, 0) >= 0 || path.IsAbs(workingDirectory) || path.Clean(workingDirectory) != workingDirectory || workingDirectory == ".." || strings.HasPrefix(workingDirectory, "../") {
		return ErrWorkdirUnavailable
	}
	relative := path.Join(workingDirectory, argument)
	if relative == ".." || strings.HasPrefix(relative, "../") {
		return ErrWorkdirUnavailable
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.verifyCurrentLocked(); err != nil {
		return err
	}
	if _, err := m.root.Stat(filepath.FromSlash(relative)); err != nil {
		return ErrWorkdirUnavailable
	}
	return nil
}
