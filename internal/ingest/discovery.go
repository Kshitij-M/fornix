package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

// DiscoveredFile contains the immutable manifest entry and bytes observed in
// one discovery snapshot.
type DiscoveredFile struct {
	File    contracts.IngestFile
	Content []byte
}

// SkippedFile records a bounded, auditable reason a path was not indexed.
type SkippedFile struct {
	Path     string `json:"path"`
	Reason   string `json:"reason"`
	ByteSize int64  `json:"byte_size,omitempty"`
}

// DiscoveryResult is the sorted source snapshot consumed by durable ingest
// batches and its stable manifest identity.
type DiscoveryResult struct {
	Files        []DiscoveredFile
	Skipped      []SkippedFile
	ManifestHash string
	TotalBytes   int64
	Duration     time.Duration
}

var defaultIgnoreRules = []string{".git", ".hg", ".svn", "node_modules", "vendor", "dist", "build", "target", ".cache"}

// Discover returns a sorted, content-addressed snapshot. It never follows a
// symlink: a source tree that contains one is rejected so the manifest cannot
// change meaning between discovery and batch processing.
func Discover(ctx context.Context, source contracts.RepositorySource) (DiscoveryResult, error) {
	started := time.Now()
	normalized, err := source.Normalize()
	if err != nil {
		return DiscoveryResult{}, err
	}
	if err := ValidateConfiguredRoot(normalized.SourceRoot, normalized.MountRoot); err != nil {
		return DiscoveryResult{}, err
	}
	mount, err := canonicalDirectory(normalized.MountRoot)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("canonicalize configured mount: %w", err)
	}
	rootInfo, err := os.Lstat(normalized.SourceRoot)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("stat source root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return DiscoveryResult{}, fmt.Errorf("source root symlink is rejected")
	}
	root, err := canonicalDirectory(normalized.SourceRoot)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("canonicalize source root: %w", err)
	}
	if !within(root, mount) {
		return DiscoveryResult{}, fmt.Errorf("source root escapes configured mount")
	}

	rules := append(append([]string(nil), defaultIgnoreRules...), normalized.IgnoreRules...)
	files := make([]DiscoveredFile, 0)
	result := DiscoveryResult{}
	err = filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, current)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if hasPathTraversal(rel) {
			return fmt.Errorf("discovered path escapes source root: %q", rel)
		}
		if ignored(rel, entry.IsDir(), rules) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink path rejected: %s", rel)
		}
		if entry.IsDir() {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if !info.Mode().IsRegular() {
			result.Skipped = append(result.Skipped, SkippedFile{Path: rel, Reason: "non_regular"})
			return nil
		}
		if len(files) >= normalized.MaxFiles {
			return fmt.Errorf("file limit exceeded: %d", normalized.MaxFiles)
		}
		if info.Size() > normalized.MaxFileBytes {
			result.Skipped = append(result.Skipped, SkippedFile{Path: rel, Reason: "file_too_large", ByteSize: info.Size()})
			return nil
		}
		data, readErr := os.ReadFile(current)
		if readErr != nil {
			return readErr
		}
		if int64(len(data)) != info.Size() {
			return fmt.Errorf("file changed while discovering: %s", rel)
		}
		if !utf8.Valid(data) {
			result.Skipped = append(result.Skipped, SkippedFile{Path: rel, Reason: "binary_or_invalid_utf8", ByteSize: info.Size()})
			return nil
		}
		if result.TotalBytes+int64(len(data)) > normalized.MaxTotalBytes {
			return fmt.Errorf("total byte limit exceeded: %d", normalized.MaxTotalBytes)
		}
		digest := sha256.Sum256(data)
		file := contracts.IngestFile{Path: rel, Mode: uint32(info.Mode().Perm()), ByteSize: int64(len(data)), ContentHash: hex.EncodeToString(digest[:]), State: contracts.IngestFilePresent}
		files = append(files, DiscoveredFile{File: file, Content: data})
		result.TotalBytes += int64(len(data))
		return nil
	})
	if err != nil {
		return DiscoveryResult{}, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].File.Path < files[j].File.Path })
	manifestFiles := make([]contracts.IngestFile, len(files))
	for i := range files {
		files[i].File.Ordinal = i
		manifestFiles[i] = files[i].File
	}
	result.Files = files
	result.ManifestHash = contracts.ManifestHash(manifestFiles)
	result.Duration = time.Since(started)
	return result, nil
}

// ValidateConfiguredRoot ensures a source root is a real directory inside the
// configured mount before discovery or resume reads it.
func ValidateConfiguredRoot(sourceRoot, mountRoot string) error {
	if strings.TrimSpace(sourceRoot) == "" || strings.TrimSpace(mountRoot) == "" || !filepath.IsAbs(sourceRoot) || !filepath.IsAbs(mountRoot) {
		return fmt.Errorf("source and mount roots must be absolute")
	}
	rootInfo, err := os.Lstat(sourceRoot)
	if err != nil {
		return fmt.Errorf("stat source root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("source root symlink is rejected")
	}
	root, err := canonicalDirectory(sourceRoot)
	if err != nil {
		return fmt.Errorf("canonicalize source root: %w", err)
	}
	mount, err := canonicalDirectory(mountRoot)
	if err != nil {
		return fmt.Errorf("canonicalize configured mount: %w", err)
	}
	if !within(root, mount) {
		return fmt.Errorf("source root escapes configured mount")
	}
	return nil
}

func canonicalDirectory(raw string) (string, error) {
	clean := filepath.Clean(raw)
	info, err := os.Stat(clean)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", clean)
	}
	return filepath.EvalSymlinks(clean)
}

func within(candidate, root string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func hasPathTraversal(p string) bool {
	clean := path.Clean("/" + filepath.ToSlash(p))
	return clean != "/"+filepath.ToSlash(p) || strings.HasPrefix(filepath.ToSlash(p), "../") || filepath.ToSlash(p) == ".."
}

func ignored(rel string, isDir bool, rules []string) bool {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for _, part := range parts {
		for _, rule := range rules {
			if part == rule {
				return true
			}
		}
	}
	for _, rule := range rules {
		rule = strings.TrimSuffix(rule, "/")
		if strings.HasSuffix(rule, "**") {
			prefix := strings.TrimSuffix(rule, "**")
			prefix = strings.TrimSuffix(prefix, "/")
			if rel == prefix || strings.HasPrefix(rel, prefix+"/") {
				return true
			}
		}
		if matched, _ := path.Match(rule, filepath.ToSlash(rel)); matched {
			return true
		}
		if isDir {
			if matched, _ := path.Match(rule, parts[len(parts)-1]); matched {
				return true
			}
		}
	}
	return false
}

// ReadAndVerify makes the source immutable from the indexer's point of view.
// A changed file is a hard failure; already committed batches remain valid.
func ReadAndVerify(root string, file contracts.IngestFile) ([]byte, error) {
	rel := filepath.FromSlash(file.Path)
	if filepath.IsAbs(rel) || hasPathTraversal(file.Path) {
		return nil, fmt.Errorf("unsafe ingest path %q", file.Path)
	}
	full := filepath.Join(root, rel)
	info, err := os.Lstat(full)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("source path is not a regular file: %s", file.Path)
	}
	if uint32(info.Mode().Perm()) != file.Mode || info.Size() != file.ByteSize {
		return nil, fmt.Errorf("source metadata changed: %s", file.Path)
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	d := sha256.Sum256(data)
	if hex.EncodeToString(d[:]) != strings.ToLower(file.ContentHash) {
		return nil, fmt.Errorf("source content changed: %s", file.Path)
	}
	return data, nil
}
