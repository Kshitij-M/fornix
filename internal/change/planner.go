// Package change plans and applies bounded, structured repository file
// operations. It deliberately does not execute commands or invoke a shell.
package change

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrUnsafePath     = errors.New("unsafe repository path")
	ErrSourceConflict = errors.New("repository source precondition conflict")
	ErrOperation      = errors.New("invalid repository change operation")
	ErrChangeBudget   = errors.New("repository change budget exceeded")
	ErrRecovery       = errors.New("repository change requires recovery")
)

// PlannedChange contains the persisted packet and the transient content
// bytes that the store moves into content-addressed artifacts.
type PlannedChange struct {
	Packet           contracts.ChangePacket
	Contents         map[string][]byte
	Diff             []byte
	ExpectedTreeHash string
}

// AppliedChange is the result of applying or validating a packet against a
// configured mount. It contains only bounded hashes and operation outcomes.
type AppliedChange struct {
	ResultTreeHash    string
	ExpectedTreeHash  string
	Changed           bool
	AppliedOperations int
	Operations        []contracts.ChangeOperation
	Conflict          *contracts.ChangeConflict
}

// PacketStateObservation is the bounded, hash-only filesystem state observed
// by post-change validators. It deliberately shares the same precondition and
// tree hashing code as Executor so validation cannot drift from application
// semantics.
type PacketStateObservation struct {
	ResultTreeHash string
	Files          int
	Bytes          int64
	Conflict       *contracts.ChangeConflict
}

// ObservePacketState checks all packet preconditions and computes the exact
// affected-tree hash used by the change executor. It never mutates files and
// never returns file contents.
func ObservePacketState(ctx context.Context, root string, packet contracts.ChangePacket) (PacketStateObservation, error) {
	if strings.TrimSpace(root) == "" {
		return PacketStateObservation{}, fmt.Errorf("repository root is required")
	}
	if packet.WorkspaceID == "" {
		return PacketStateObservation{}, fmt.Errorf("packet workspace_id is required")
	}
	repositoryRoot, err := openChangeRoot(root)
	if err != nil {
		return PacketStateObservation{}, err
	}
	defer repositoryRoot.Close()
	operations := sortedOperations(packet.Operations)
	for _, operation := range operations {
		if err := ctx.Err(); err != nil {
			return PacketStateObservation{}, err
		}
		if conflict := checkPrecondition(repositoryRoot, operation); conflict != nil {
			return PacketStateObservation{Conflict: conflict}, fmt.Errorf("%w: %s", ErrSourceConflict, conflict.Path)
		}
	}
	hash, err := observedTreeHashFull(repositoryRoot, packet.Source, operations)
	if err != nil {
		return PacketStateObservation{}, err
	}
	var files int
	var bytes int64
	paths := make(map[string]struct{}, len(packet.Source.Files)+len(operations)*2)
	for _, file := range packet.Source.Files {
		paths[file.Path] = struct{}{}
	}
	for _, operation := range operations {
		paths[operation.Path] = struct{}{}
		if operation.Type == contracts.ChangeOpRename {
			paths[operation.Destination] = struct{}{}
		}
	}
	for path := range paths {
		info, statErr := lstatChangePath(repositoryRoot, path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return PacketStateObservation{}, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return PacketStateObservation{}, fmt.Errorf("%w: %s", ErrUnsafePath, path)
		}
		data, readErr := repositoryRoot.ReadFile(filepath.FromSlash(path))
		if readErr != nil {
			return PacketStateObservation{}, readErr
		}
		files++
		bytes += int64(len(data))
	}
	return PacketStateObservation{ResultTreeHash: hash, Files: files, Bytes: bytes}, nil
}

// ObserveAppliedPacketState computes the post-application tree observation
// without re-running preconditions that were intentionally consumed by the
// filesystem application. It is the correct boundary for validation after a
// successful external change; it remains read-only and rejects unsafe paths.
func ObserveAppliedPacketState(ctx context.Context, root string, packet contracts.ChangePacket) (PacketStateObservation, error) {
	if strings.TrimSpace(root) == "" {
		return PacketStateObservation{}, fmt.Errorf("repository root is required")
	}
	if packet.WorkspaceID == "" {
		return PacketStateObservation{}, fmt.Errorf("packet workspace_id is required")
	}
	if err := ctx.Err(); err != nil {
		return PacketStateObservation{}, err
	}
	repositoryRoot, err := openChangeRoot(root)
	if err != nil {
		return PacketStateObservation{}, err
	}
	defer repositoryRoot.Close()
	hash, err := observedTreeHashFull(repositoryRoot, packet.Source, packet.Operations)
	if err != nil {
		return PacketStateObservation{}, err
	}
	var files int
	var bytes int64
	paths := make(map[string]struct{}, len(packet.Source.Files)+len(packet.Operations)*2)
	for _, file := range packet.Source.Files {
		paths[file.Path] = struct{}{}
	}
	for _, operation := range packet.Operations {
		paths[operation.Path] = struct{}{}
		if operation.Type == contracts.ChangeOpRename {
			paths[operation.Destination] = struct{}{}
		}
	}
	for path := range paths {
		if err := ctx.Err(); err != nil {
			return PacketStateObservation{}, err
		}
		info, statErr := lstatChangePath(repositoryRoot, path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return PacketStateObservation{}, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return PacketStateObservation{}, fmt.Errorf("%w: %s", ErrUnsafePath, path)
		}
		data, readErr := repositoryRoot.ReadFile(filepath.FromSlash(path))
		if readErr != nil {
			return PacketStateObservation{}, readErr
		}
		files++
		bytes += int64(len(data))
	}
	return PacketStateObservation{ResultTreeHash: hash, Files: files, Bytes: bytes}, nil
}

// ContentResolver supplies immutable content artifacts to the executor.
type ContentResolver func(context.Context, string, string) ([]byte, error)

// Executor applies a packet through one opened filesystem root. Hooks exist
// only for deterministic crash tests and are nil in production.
type Executor struct {
	BeforeOperation func(contracts.ChangeOperation) error
	AfterOperation  func(contracts.ChangeOperation) error
}

// NormalizeRelativePath rejects paths that can escape a configured repository
// root or be interpreted differently across API and filesystem boundaries.
func NormalizeRelativePath(value string) (string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if value == "" || len(value) > contracts.MaxChangePathLength || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: empty, oversized, or NUL path", ErrUnsafePath)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("%w: control character", ErrUnsafePath)
		}
	}
	if strings.HasPrefix(value, "/") || filepath.IsAbs(filepath.FromSlash(value)) {
		return "", fmt.Errorf("%w: absolute path", ErrUnsafePath)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(value)))
	if clean == "." || clean == "" || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: traversal path", ErrUnsafePath)
	}
	return clean, nil
}

// SafeJoin is a compatibility path validator. Its returned host path must not
// be used for a later mutation; use the root-handle executor instead.
func SafeJoin(root, relative string) (string, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	path, err := NormalizeRelativePath(relative)
	if err != nil {
		return "", err
	}
	repositoryRoot, err := openChangeRoot(root)
	if err != nil {
		return "", err
	}
	defer repositoryRoot.Close()
	if _, err := lstatChangePath(repositoryRoot, path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return filepath.Join(root, filepath.FromSlash(path)), nil
}

// openChangeRoot pins one validated repository directory for an entire
// observation or application. Subsequent path operations must use this handle
// rather than rebuilding absolute host paths.
func openChangeRoot(path string) (*os.Root, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%w: repository root must be absolute", ErrUnsafePath)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat repository root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: repository root must be a non-symlink directory", ErrUnsafePath)
	}
	repositoryRoot, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("open repository root: %w", err)
	}
	openedInfo, err := repositoryRoot.Stat(".")
	if err != nil || !os.SameFile(info, openedInfo) {
		_ = repositoryRoot.Close()
		if err == nil {
			err = fmt.Errorf("repository root changed while opening")
		}
		return nil, fmt.Errorf("%w: %v", ErrUnsafePath, err)
	}
	return repositoryRoot, nil
}

// lstatChangePath checks each normalized component through an opened root.
// It rejects observed symlinks and non-directory parents. Rooted operations
// still enforce containment if the tree changes after this inspection.
func lstatChangePath(root *os.Root, relative string) (os.FileInfo, error) {
	if root == nil {
		return nil, fmt.Errorf("%w: repository root is unavailable", ErrUnsafePath)
	}
	normalized, err := NormalizeRelativePath(relative)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(normalized, "/")
	current := ""
	var finalInfo os.FileInfo
	for index, part := range parts {
		if current == "" {
			current = part
		} else {
			current = filepath.Join(current, part)
		}
		info, err := root.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: symlink component %s", ErrUnsafePath, part)
		}
		if index < len(parts)-1 && !info.IsDir() {
			return nil, fmt.Errorf("%w: non-directory parent %s", ErrUnsafePath, part)
		}
		finalInfo = info
	}
	return finalInfo, nil
}

// CaptureSnapshot reads only the affected paths in stable order. It rejects
// symlinks and records absent paths explicitly for create operations.
func CaptureSnapshot(ctx context.Context, workspaceID, repository, root string, paths []string, actor contracts.ActorRef) (contracts.ChangeSourceSnapshot, error) {
	workspaceID, repository = strings.TrimSpace(workspaceID), strings.TrimSpace(repository)
	if workspaceID == "" || repository == "" {
		return contracts.ChangeSourceSnapshot{}, fmt.Errorf("workspace_id and repository are required")
	}
	root = filepath.Clean(strings.TrimSpace(root))
	if !filepath.IsAbs(root) {
		return contracts.ChangeSourceSnapshot{}, fmt.Errorf("repository root must be absolute")
	}
	normalized, err := normalizePaths(paths)
	if err != nil {
		return contracts.ChangeSourceSnapshot{}, err
	}
	repositoryRoot, err := openChangeRoot(root)
	if err != nil {
		return contracts.ChangeSourceSnapshot{}, fmt.Errorf("open repository root: %w", err)
	}
	defer repositoryRoot.Close()
	files := make([]contracts.ChangeSourceFile, 0, len(normalized))
	for _, path := range normalized {
		if err := ctx.Err(); err != nil {
			return contracts.ChangeSourceSnapshot{}, err
		}
		relative := filepath.FromSlash(path)
		info, statErr := lstatChangePath(repositoryRoot, path)
		if errors.Is(statErr, os.ErrNotExist) {
			files = append(files, contracts.ChangeSourceFile{Path: path, Exists: false})
			continue
		}
		if statErr != nil {
			return contracts.ChangeSourceSnapshot{}, fmt.Errorf("stat %s: %w", path, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return contracts.ChangeSourceSnapshot{}, fmt.Errorf("%w: symlink %s", ErrUnsafePath, path)
		}
		if !info.Mode().IsRegular() {
			return contracts.ChangeSourceSnapshot{}, fmt.Errorf("%w: non-regular file %s", ErrUnsafePath, path)
		}
		data, err := repositoryRoot.ReadFile(relative)
		if err != nil {
			return contracts.ChangeSourceSnapshot{}, fmt.Errorf("read %s: %w", path, err)
		}
		files = append(files, contracts.ChangeSourceFile{Path: path, Mode: uint32(info.Mode().Perm()), ByteSize: int64(len(data)), ContentHash: contracts.ArtifactContentHash(data), Exists: true})
	}
	snapshot := contracts.ChangeSourceSnapshot{WorkspaceID: workspaceID, Repository: repository, SourceRoot: root, Files: files, Actor: actor, CapturedAt: time.Now().UTC()}
	snapshot.ManifestHash = manifestHash(snapshot)
	return snapshot, nil
}

// Plan normalizes operations against an observed snapshot and computes the
// stable packet, expected output tree, and hash-only review diff.
func Plan(request contracts.ChangeProposalRequest, source contracts.ChangeSourceSnapshot) (PlannedChange, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return PlannedChange{}, err
	}
	if source.WorkspaceID != normalized.WorkspaceID || source.Repository != normalized.Repository {
		return PlannedChange{}, fmt.Errorf("source snapshot crosses workspace or repository boundary")
	}
	if len(normalized.Operations) > normalized.Budgets.MaxOperations {
		return PlannedChange{}, ErrChangeBudget
	}
	source.Files = append([]contracts.ChangeSourceFile(nil), source.Files...)
	source.ManifestHash = manifestHash(source)
	lookup := make(map[string]contracts.ChangeSourceFile, len(source.Files))
	for _, file := range source.Files {
		lookup[file.Path] = file
	}
	operations := make([]contracts.ChangeOperation, 0, len(normalized.Operations))
	contents := make(map[string][]byte)
	seen := make(map[string]struct{}, len(normalized.Operations))
	var totalBytes int64
	for index, input := range normalized.Operations {
		path, pathErr := NormalizeRelativePath(input.Path)
		if pathErr != nil {
			return PlannedChange{}, pathErr
		}
		if _, exists := seen[path]; exists {
			return PlannedChange{}, fmt.Errorf("%w: duplicate path %s", ErrOperation, path)
		}
		seen[path] = struct{}{}
		operation := contracts.ChangeOperation{ID: strings.TrimSpace(input.ID), Ordinal: input.Ordinal, Type: strings.ToLower(strings.TrimSpace(input.Type)), Path: path, Destination: strings.TrimSpace(input.Destination), ExpectedHash: strings.ToLower(strings.TrimSpace(input.ExpectedHash)), ExpectedMode: input.ExpectedMode, NewMode: input.NewMode}
		if operation.ID == "" {
			operation.ID = fmt.Sprintf("op-%03d", index+1)
		}
		if operation.Ordinal == 0 {
			operation.Ordinal = index + 1
		}
		if err := validateType(operation.Type); err != nil {
			return PlannedChange{}, err
		}
		file := lookup[path]
		switch operation.Type {
		case contracts.ChangeOpCreate:
			if file.Exists {
				return PlannedChange{}, fmt.Errorf("%w: %s already exists", ErrSourceConflict, path)
			}
			if len(input.Content) == 0 {
				return PlannedChange{}, fmt.Errorf("%w: create content is required", ErrOperation)
			}
			operation.NewContentHash = contracts.ArtifactContentHash(input.Content)
			operation.NewByteSize = int64(len(input.Content))
			if operation.NewMode == 0 {
				operation.NewMode = 0o644
			}
			contents[operation.ID] = append([]byte(nil), input.Content...)
		case contracts.ChangeOpReplace:
			if !file.Exists || operation.ExpectedHash == "" || operation.ExpectedHash != file.ContentHash {
				return PlannedChange{}, fmt.Errorf("%w: %s", ErrSourceConflict, path)
			}
			if len(input.Content) == 0 {
				return PlannedChange{}, fmt.Errorf("%w: replacement content is required", ErrOperation)
			}
			operation.NewContentHash = contracts.ArtifactContentHash(input.Content)
			operation.NewByteSize = int64(len(input.Content))
			if operation.NewMode == 0 {
				operation.NewMode = file.Mode
			}
			contents[operation.ID] = append([]byte(nil), input.Content...)
		case contracts.ChangeOpDelete:
			if !file.Exists || operation.ExpectedHash == "" || operation.ExpectedHash != file.ContentHash {
				return PlannedChange{}, fmt.Errorf("%w: %s", ErrSourceConflict, path)
			}
		case contracts.ChangeOpRename:
			if !file.Exists || operation.ExpectedHash == "" || operation.ExpectedHash != file.ContentHash {
				return PlannedChange{}, fmt.Errorf("%w: %s", ErrSourceConflict, path)
			}
			operation.Destination, err = NormalizeRelativePath(operation.Destination)
			if err != nil {
				return PlannedChange{}, err
			}
			if destination := lookup[operation.Destination]; destination.Exists {
				return PlannedChange{}, fmt.Errorf("%w: destination %s exists", ErrSourceConflict, operation.Destination)
			}
			if _, exists := seen[operation.Destination]; exists {
				return PlannedChange{}, fmt.Errorf("%w: duplicate destination %s", ErrOperation, operation.Destination)
			}
			seen[operation.Destination] = struct{}{}
			operation.NewContentHash, operation.NewByteSize, operation.NewMode = file.ContentHash, file.ByteSize, file.Mode
		case contracts.ChangeOpChmod:
			if !file.Exists || operation.ExpectedHash == "" || operation.ExpectedHash != file.ContentHash || operation.NewMode == 0 {
				return PlannedChange{}, fmt.Errorf("%w: chmod precondition %s", ErrSourceConflict, path)
			}
			operation.NewContentHash, operation.NewByteSize = file.ContentHash, file.ByteSize
		}
		if operation.NewByteSize > normalized.Budgets.MaxFileBytes {
			return PlannedChange{}, ErrChangeBudget
		}
		totalBytes += operation.NewByteSize
		if totalBytes > normalized.Budgets.MaxTotalBytes {
			return PlannedChange{}, ErrChangeBudget
		}
		operations = append(operations, operation)
	}
	packet := contracts.ChangePacket{SchemaVersion: contracts.ChangeSchemaVersion, WorkspaceID: normalized.WorkspaceID, Repository: normalized.Repository, Source: source, Operations: operations, Budgets: normalized.Budgets, Policy: clonePolicyRef(normalized.Policy)}
	result := PlannedChange{Packet: packet, Contents: contents}
	result.ExpectedTreeHash = predictedTreeHash(source, operations)
	result.Packet.ExpectedTreeHash = result.ExpectedTreeHash
	result.Diff, _ = json.Marshal(struct {
		PacketHash         string                      `json:"packet_hash"`
		SourceManifestHash string                      `json:"source_manifest_hash"`
		Operations         []contracts.ChangeOperation `json:"operations"`
		ExpectedTreeHash   string                      `json:"expected_tree_hash"`
	}{result.Packet.StableHash(), source.ManifestHash, operations, result.ExpectedTreeHash})
	return result, nil
}

// Apply performs a dry-run or direct structured operation application. The
// caller must perform durable approval, RBAC, and task-fence admission first.
func (e Executor) Apply(ctx context.Context, root string, packet contracts.ChangePacket, resolve ContentResolver, dryRun bool) (AppliedChange, error) {
	if root == "" {
		return AppliedChange{}, fmt.Errorf("repository root is required")
	}
	if packet.WorkspaceID == "" {
		return AppliedChange{}, fmt.Errorf("packet workspace_id is required")
	}
	repositoryRoot, err := openChangeRoot(root)
	if err != nil {
		return AppliedChange{}, err
	}
	defer repositoryRoot.Close()
	result := AppliedChange{ExpectedTreeHash: predictedTreeHash(packet.Source, packet.Operations), Operations: append([]contracts.ChangeOperation(nil), packet.Operations...)}
	for _, operation := range sortedOperations(packet.Operations) {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if e.BeforeOperation != nil {
			if err := e.BeforeOperation(operation); err != nil {
				return result, err
			}
		}
		if conflict := checkPrecondition(repositoryRoot, operation); conflict != nil {
			result.Conflict = conflict
			return result, fmt.Errorf("%w: %s", ErrSourceConflict, conflict.Path)
		}
		if !dryRun {
			if err := e.applyOne(ctx, packet.WorkspaceID, repositoryRoot, operation, resolve); err != nil {
				return result, err
			}
			result.AppliedOperations++
		}
		if e.AfterOperation != nil {
			if err := e.AfterOperation(operation); err != nil {
				return result, err
			}
		}
	}
	if dryRun {
		result.ResultTreeHash = result.ExpectedTreeHash
		return result, nil
	}
	result.ResultTreeHash, err = observedTreeHashFull(repositoryRoot, packet.Source, packet.Operations)
	if err != nil {
		return result, fmt.Errorf("%w: post-state observation: %w", ErrRecovery, err)
	}
	result.Changed = !dryRun
	if result.ResultTreeHash != result.ExpectedTreeHash {
		return result, fmt.Errorf("%w: resulting tree hash mismatch", ErrRecovery)
	}
	return result, nil
}

func (e Executor) applyOne(ctx context.Context, workspaceID string, root *os.Root, operation contracts.ChangeOperation, resolve ContentResolver) error {
	target, err := NormalizeRelativePath(operation.Path)
	if err != nil {
		return err
	}
	switch operation.Type {
	case contracts.ChangeOpCreate, contracts.ChangeOpReplace:
		if resolve == nil {
			return fmt.Errorf("content resolver is required")
		}
		content, err := resolve(ctx, workspaceID, operation.NewContentHash)
		if err != nil {
			return err
		}
		if contracts.ArtifactContentHash(content) != operation.NewContentHash {
			return fmt.Errorf("%w: content hash mismatch", ErrRecovery)
		}
		parent := filepath.Dir(filepath.FromSlash(target))
		if err := root.MkdirAll(parent, 0o755); err != nil {
			return err
		}
		temporaryName := filepath.Join(parent, ".fornix-change-"+contracts.NewID("tmp"))
		temporary, err := root.OpenFile(temporaryName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		defer func() {
			_ = temporary.Close()
			_ = root.Remove(temporaryName)
		}()
		if err := temporary.Chmod(os.FileMode(operation.NewMode)); err != nil && operation.NewMode != 0 {
			_ = temporary.Close()
			return err
		}
		if _, err := temporary.Write(content); err != nil {
			_ = temporary.Close()
			return err
		}
		if err := temporary.Sync(); err != nil {
			_ = temporary.Close()
			return err
		}
		if err := temporary.Close(); err != nil {
			return err
		}
		if operation.Type == contracts.ChangeOpCreate {
			// Link creates the destination only if it is still absent. Unlike a
			// rename, it cannot replace a file created after precondition checking.
			if err := root.Link(temporaryName, filepath.FromSlash(target)); err != nil {
				return err
			}
			if err := root.Remove(temporaryName); err != nil {
				return fmt.Errorf("remove temporary create link: %w", err)
			}
		} else if err := root.Rename(temporaryName, filepath.FromSlash(target)); err != nil {
			return err
		}
	case contracts.ChangeOpDelete:
		if err := root.Remove(filepath.FromSlash(target)); err != nil {
			return err
		}
	case contracts.ChangeOpRename:
		destination, err := NormalizeRelativePath(operation.Destination)
		if err != nil {
			return err
		}
		if err := root.MkdirAll(filepath.Dir(filepath.FromSlash(destination)), 0o755); err != nil {
			return err
		}
		// Create a no-overwrite destination link first. If a crash interrupts
		// the following unlink, recovery observes both names and fails closed
		// rather than silently replacing a concurrently-created destination.
		if err := root.Link(filepath.FromSlash(target), filepath.FromSlash(destination)); err != nil {
			return err
		}
		if err := root.Remove(filepath.FromSlash(target)); err != nil {
			return fmt.Errorf("remove renamed source link: %w", err)
		}
	case contracts.ChangeOpChmod:
		file, err := root.OpenFile(filepath.FromSlash(target), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: chmod target is not a regular file", ErrUnsafePath)
		}
		if err := file.Chmod(os.FileMode(operation.NewMode)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported type %s", ErrOperation, operation.Type)
	}
	return nil
}

func checkPrecondition(root *os.Root, operation contracts.ChangeOperation) *contracts.ChangeConflict {
	path, err := NormalizeRelativePath(operation.Path)
	if err != nil {
		return &contracts.ChangeConflict{Path: operation.Path, Reason: err.Error()}
	}
	info, statErr := lstatChangePath(root, path)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return &contracts.ChangeConflict{Path: operation.Path, ExpectedHash: operation.ExpectedHash, Reason: statErr.Error()}
	}
	exists := statErr == nil
	if exists && info.Mode()&os.ModeSymlink != 0 {
		return &contracts.ChangeConflict{Path: operation.Path, ExpectedHash: operation.ExpectedHash, ObservedExists: true, Reason: "symlink"}
	}
	observed := ""
	if exists && info.Mode().IsRegular() {
		data, readErr := root.ReadFile(filepath.FromSlash(path))
		if readErr != nil {
			return &contracts.ChangeConflict{Path: operation.Path, ExpectedHash: operation.ExpectedHash, ObservedExists: true, Reason: readErr.Error()}
		}
		observed = contracts.ArtifactContentHash(data)
	}
	expectedExists := operation.Type != contracts.ChangeOpCreate
	if exists != expectedExists || (expectedExists && observed != operation.ExpectedHash) {
		return &contracts.ChangeConflict{Path: operation.Path, ExpectedHash: operation.ExpectedHash, ObservedHash: observed, ExpectedExists: expectedExists, ObservedExists: exists, Reason: "source precondition mismatch"}
	}
	if operation.Type == contracts.ChangeOpRename {
		destination, err := NormalizeRelativePath(operation.Destination)
		if err != nil {
			return &contracts.ChangeConflict{Path: operation.Destination, Reason: err.Error()}
		}
		if _, err := lstatChangePath(root, destination); err == nil {
			return &contracts.ChangeConflict{Path: operation.Destination, ObservedExists: true, Reason: "rename destination exists"}
		} else if !errors.Is(err, os.ErrNotExist) {
			return &contracts.ChangeConflict{Path: operation.Destination, Reason: err.Error()}
		}
	}
	return nil
}

func validateType(value string) error {
	switch value {
	case contracts.ChangeOpCreate, contracts.ChangeOpReplace, contracts.ChangeOpDelete, contracts.ChangeOpRename, contracts.ChangeOpChmod:
		return nil
	default:
		return fmt.Errorf("%w: unsupported type %q", ErrOperation, value)
	}
}

func normalizePaths(paths []string) ([]string, error) {
	set := map[string]struct{}{}
	for _, raw := range paths {
		path, err := NormalizeRelativePath(raw)
		if err != nil {
			return nil, err
		}
		set[path] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for path := range set {
		result = append(result, path)
	}
	sort.Strings(result)
	return result, nil
}
func sortedOperations(operations []contracts.ChangeOperation) []contracts.ChangeOperation {
	result := append([]contracts.ChangeOperation(nil), operations...)
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Ordinal != result[j].Ordinal {
			return result[i].Ordinal < result[j].Ordinal
		}
		return result[i].Path < result[j].Path
	})
	return result
}

func manifestHash(source contracts.ChangeSourceSnapshot) string {
	clone := source
	clone.SourceRoot = ""
	clone.CapturedAt = time.Time{}
	clone.Files = append([]contracts.ChangeSourceFile(nil), source.Files...)
	sort.Slice(clone.Files, func(i, j int) bool { return clone.Files[i].Path < clone.Files[j].Path })
	raw, _ := json.Marshal(clone)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func predictedTreeHash(source contracts.ChangeSourceSnapshot, operations []contracts.ChangeOperation) string {
	files := map[string]contracts.ChangeSourceFile{}
	for _, file := range source.Files {
		files[file.Path] = file
	}
	for _, op := range sortedOperations(operations) {
		switch op.Type {
		case contracts.ChangeOpCreate, contracts.ChangeOpReplace:
			files[op.Path] = contracts.ChangeSourceFile{Path: op.Path, Mode: op.NewMode, ByteSize: op.NewByteSize, ContentHash: op.NewContentHash, Exists: true}
		case contracts.ChangeOpDelete:
			delete(files, op.Path)
		case contracts.ChangeOpRename:
			file := files[op.Path]
			delete(files, op.Path)
			file.Path = op.Destination
			files[op.Destination] = file
		case contracts.ChangeOpChmod:
			file := files[op.Path]
			file.Mode = op.NewMode
			files[op.Path] = file
		}
	}
	ordered := make([]contracts.ChangeSourceFile, 0, len(files))
	for _, file := range files {
		ordered = append(ordered, file)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	raw, _ := json.Marshal(ordered)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func observedTreeHashFull(repositoryRoot *os.Root, source contracts.ChangeSourceSnapshot, operations []contracts.ChangeOperation) (string, error) {
	paths := make(map[string]struct{}, len(source.Files)+len(operations)*2)
	for _, file := range source.Files {
		paths[file.Path] = struct{}{}
	}
	for _, operation := range operations {
		paths[operation.Path] = struct{}{}
		if operation.Type == contracts.ChangeOpRename {
			paths[operation.Destination] = struct{}{}
		}
	}
	orderedPaths := make([]string, 0, len(paths))
	for path := range paths {
		orderedPaths = append(orderedPaths, path)
	}
	sort.Strings(orderedPaths)
	files := make([]contracts.ChangeSourceFile, 0, len(orderedPaths))
	for _, raw := range orderedPaths {
		relative := filepath.FromSlash(raw)
		info, err := lstatChangePath(repositoryRoot, raw)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", ErrUnsafePath
		}
		data, err := repositoryRoot.ReadFile(relative)
		if err != nil {
			return "", err
		}
		files = append(files, contracts.ChangeSourceFile{Path: raw, Mode: uint32(info.Mode().Perm()), ByteSize: int64(len(data)), ContentHash: contracts.ArtifactContentHash(data), Exists: true})
	}
	raw, _ := json.Marshal(files)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
