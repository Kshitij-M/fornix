package profile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func ensurePrivateDirectory(path string) (string, error) {
	clean, err := ValidateRoot(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(clean, DirectoryMode); err != nil {
		return "", fmt.Errorf("create private profile directory: %w", err)
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("inspect private profile directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", ErrUnsafePath
	}
	if err := os.Chmod(clean, DirectoryMode); err != nil {
		return "", fmt.Errorf("restrict private profile directory: %w", err)
	}
	return clean, nil
}

func writeAtomic(directory, name string, data []byte) (resultErr error) {
	directory, err := ensurePrivateDirectory(directory)
	if err != nil {
		return err
	}
	if !safeLeaf(name) {
		return ErrUnsafePath
	}
	temporary, err := os.CreateTemp(directory, ".fornix-write-*")
	if err != nil {
		return fmt.Errorf("create private temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if resultErr != nil {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(FileMode); err != nil {
		return fmt.Errorf("restrict private temporary file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write private temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync private temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close private temporary file: %w", err)
	}
	target := filepath.Join(directory, name)
	if err := os.Rename(temporaryName, target); err != nil {
		return fmt.Errorf("publish private file: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	return nil
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open private directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync private directory: %w", err)
	}
	return nil
}

func safeLeaf(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.ContainsAny(name, `/\\`) && !strings.ContainsRune(name, 0)
}

func privateRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, ErrUnsafePath
	}
	if info.Mode().Perm() != FileMode {
		return nil, ErrInsecurePermissions
	}
	return info, nil
}
