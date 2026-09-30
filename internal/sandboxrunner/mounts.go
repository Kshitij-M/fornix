// Package sandboxrunner contains host-side capabilities used by an isolated
// tool runner. It owns explicit workspace mount resolution, not operation
// authorization or durable execution state; those remain in the Fornix
// control plane and Postgres.
package sandboxrunner

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	// ErrInvalidMount reports invalid operator-provided mount registration.
	ErrInvalidMount = errors.New("invalid sandbox workspace mount")
	// ErrMountUnavailable reports an unknown, cross-workspace, or changed mount.
	ErrMountUnavailable = errors.New("sandbox workspace mount is unavailable")
	// ErrMountConflict reports an attempt to rebind an ID or overlap another
	// workspace's registered host root.
	ErrMountConflict = errors.New("sandbox workspace mount conflicts with an existing registration")
	// ErrWorkdirUnavailable reports a missing or escaping relative workdir.
	ErrWorkdirUnavailable = errors.New("sandbox working directory is unavailable")
)

const containerWorkspaceRoot = "/workspace"

// MountRegistration binds an opaque, workspace-scoped reference to an
// operator-selected host directory. The path is local runner configuration;
// callers must never serialize it into a control-plane request or log it.
type MountRegistration struct {
	Reference contracts.WorkspaceMountRef
	HostPath  string
}

type mountRecord struct {
	reference contracts.WorkspaceMountRef
	hostPath  string
	rootInfo  os.FileInfo
}

// MountCatalog is the runner's append-only trust catalog. Production loads it
// before accepting requests; Register can add a new reference but never rebind
// an existing one. It rejects overlapping roots across workspaces so mounting
// one workspace cannot expose another workspace's files through a parent
// directory.
type MountCatalog struct {
	mu      sync.RWMutex
	byID    map[string]mountRecord
	ordered []mountRecord
}

// NewMountCatalog validates all registrations before publishing any of them.
// An empty catalog is valid but cannot resolve a workspace reference.
func NewMountCatalog(registrations []MountRegistration) (*MountCatalog, error) {
	catalog := &MountCatalog{byID: make(map[string]mountRecord, len(registrations))}
	for _, registration := range registrations {
		if err := catalog.Register(registration.Reference, registration.HostPath); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

// Register adds one operator-authorized mount. Re-registering the identical
// reference and canonical directory is idempotent; changing either requires a
// new opaque reference so old durable requests cannot silently target new data.
func (c *MountCatalog) Register(reference contracts.WorkspaceMountRef, hostPath string) error {
	if c == nil {
		return fmt.Errorf("%w: catalog is required", ErrInvalidMount)
	}
	if err := reference.Normalize(); err != nil {
		return fmt.Errorf("%w: mount reference is invalid", ErrInvalidMount)
	}
	if strings.IndexByte(hostPath, 0) >= 0 || len(hostPath) > contracts.MaxToolWorkdirLength || !filepath.IsAbs(hostPath) || filepath.Clean(hostPath) != hostPath {
		return fmt.Errorf("%w: host root must be an absolute normalized directory", ErrInvalidMount)
	}
	if hasSymlinkComponent(hostPath) {
		return fmt.Errorf("%w: host root path contains a symlink", ErrInvalidMount)
	}
	resolved, err := filepath.EvalSymlinks(hostPath)
	if err != nil || resolved != hostPath {
		return fmt.Errorf("%w: host root cannot be resolved", ErrInvalidMount)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil || filepath.Clean(resolved) != resolved || isFilesystemRoot(resolved) {
		return fmt.Errorf("%w: host root is not an allowed directory", ErrInvalidMount)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%w: host root must be an existing directory", ErrInvalidMount)
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return fmt.Errorf("%w: host root cannot be opened safely", ErrInvalidMount)
	}
	defer root.Close()
	rootInfo, err := root.Stat(".")
	if err != nil || !rootInfo.IsDir() || !os.SameFile(info, rootInfo) {
		return fmt.Errorf("%w: host root changed during registration", ErrInvalidMount)
	}

	record := mountRecord{reference: reference, hostPath: resolved, rootInfo: rootInfo}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byID == nil {
		c.byID = make(map[string]mountRecord)
	}
	if previous, exists := c.byID[reference.ID]; exists {
		if previous.reference == reference && previous.hostPath == resolved && os.SameFile(previous.rootInfo, rootInfo) {
			return nil
		}
		return ErrMountConflict
	}
	for _, previous := range c.ordered {
		if previous.reference.WorkspaceID != reference.WorkspaceID && mountsOverlap(previous, record) {
			return ErrMountConflict
		}
	}
	c.byID[reference.ID] = record
	c.ordered = append(c.ordered, record)
	return nil
}

// ResolvedMount is a verified handle to one catalog entry. HostPath is only
// for the local Engine adapter; it must not be returned over IPC or logged.
// Close releases the root-confined filesystem handle.
type ResolvedMount struct {
	mu        sync.RWMutex
	reference contracts.WorkspaceMountRef
	hostPath  string
	root      *os.Root
	rootInfo  os.FileInfo
}

// Reference returns the opaque workspace-scoped identity, never the host path.
func (m *ResolvedMount) Reference() contracts.WorkspaceMountRef {
	if m == nil {
		return contracts.WorkspaceMountRef{}
	}
	return m.reference
}

// ReadOnly reports the immutable mount mode. The future Engine adapter must
// set its bind mount read-only and reject a writable request; no registration
// path can weaken this property.
func (m *ResolvedMount) ReadOnly() bool { return m != nil }

// HostPath returns the canonical host root for use only in a local Engine
// bind-mount request. Call VerifyCurrent immediately before container creation.
func (m *ResolvedMount) HostPath() (string, error) {
	if m == nil {
		return "", ErrMountUnavailable
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.verifyCurrentLocked(); err != nil {
		return "", err
	}
	return m.hostPath, nil
}

// ContainerWorkdir validates a workspace-relative path using a root-confined
// filesystem handle and returns its fixed in-container path. Symlinks may not
// escape the registered root.
func (m *ResolvedMount) ContainerWorkdir(relative string) (string, error) {
	if m == nil {
		return "", ErrMountUnavailable
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.verifyCurrentLocked(); err != nil {
		return "", err
	}
	relative = strings.TrimSpace(relative)
	if relative == "" {
		relative = "."
	}
	if strings.Contains(relative, `\`) || strings.IndexByte(relative, 0) >= 0 || path.IsAbs(relative) || path.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, "../") {
		return "", ErrWorkdirUnavailable
	}
	info, err := m.root.Stat(filepath.FromSlash(relative))
	if err != nil || !info.IsDir() {
		return "", ErrWorkdirUnavailable
	}
	if relative == "." {
		return containerWorkspaceRoot, nil
	}
	return path.Join(containerWorkspaceRoot, relative), nil
}

// VerifyCurrent rejects a mount whose canonical path no longer resolves to
// the directory registered in the catalog. The Engine adapter calls this
// immediately before using the bind source.
func (m *ResolvedMount) VerifyCurrent() error {
	if m == nil {
		return ErrMountUnavailable
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.verifyCurrentLocked()
}

func (m *ResolvedMount) verifyCurrentLocked() error {
	if m.root == nil || m.hostPath == "" || m.rootInfo == nil {
		return ErrMountUnavailable
	}
	resolved, err := filepath.EvalSymlinks(m.hostPath)
	if err != nil || resolved != m.hostPath {
		return ErrMountUnavailable
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() || !os.SameFile(info, m.rootInfo) {
		return ErrMountUnavailable
	}
	return nil
}

// Close releases the OS root associated with the resolved mount.
func (m *ResolvedMount) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil
	}
	err := m.root.Close()
	m.root = nil
	return err
}

// Resolve returns a root-confined handle only when both the opaque ID and
// workspace match the operator registration.
func (c *MountCatalog) Resolve(reference contracts.WorkspaceMountRef) (*ResolvedMount, error) {
	if c == nil {
		return nil, ErrMountUnavailable
	}
	if err := reference.Normalize(); err != nil {
		return nil, ErrMountUnavailable
	}
	c.mu.RLock()
	record, ok := c.byID[reference.ID]
	c.mu.RUnlock()
	if !ok || record.reference.WorkspaceID != reference.WorkspaceID {
		return nil, ErrMountUnavailable
	}
	resolved, err := filepath.EvalSymlinks(record.hostPath)
	if err != nil || resolved != record.hostPath {
		return nil, ErrMountUnavailable
	}
	root, err := os.OpenRoot(record.hostPath)
	if err != nil {
		return nil, ErrMountUnavailable
	}
	info, err := root.Stat(".")
	if err != nil || !info.IsDir() || !os.SameFile(info, record.rootInfo) {
		_ = root.Close()
		return nil, ErrMountUnavailable
	}
	return &ResolvedMount{reference: record.reference, hostPath: record.hostPath, root: root, rootInfo: info}, nil
}

func isFilesystemRoot(candidate string) bool {
	volume := filepath.VolumeName(candidate)
	return candidate == string(filepath.Separator) || (volume != "" && candidate == volume+string(filepath.Separator))
}

func pathsOverlap(left, right string) bool {
	return pathWithin(left, right) || pathWithin(right, left)
}

func pathWithin(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func hasSymlinkComponent(candidate string) bool {
	volume := filepath.VolumeName(candidate)
	root := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(candidate, root)
	current := root
	if info, err := os.Lstat(current); err != nil || info.Mode()&os.ModeSymlink != 0 {
		return true
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

func mountsOverlap(left, right mountRecord) bool {
	if pathsOverlap(left.hostPath, right.hostPath) || os.SameFile(left.rootInfo, right.rootInfo) {
		return true
	}
	if runtime.GOOS == "darwin" && pathsOverlapFolded(left.hostPath, right.hostPath) {
		return true
	}
	return physicalAncestor(left.hostPath, right.rootInfo) || physicalAncestor(right.hostPath, left.rootInfo)
}

func pathsOverlapFolded(left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	leftParts, rightParts := strings.Split(left, string(filepath.Separator)), strings.Split(right, string(filepath.Separator))
	common := len(leftParts)
	if len(rightParts) < common {
		common = len(rightParts)
	}
	for i := 0; i < common; i++ {
		if !strings.EqualFold(leftParts[i], rightParts[i]) {
			return false
		}
	}
	return true
}

func physicalAncestor(candidate string, targetInfo os.FileInfo) bool {
	for current := filepath.Clean(candidate); ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil && os.SameFile(info, targetInfo) {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}
