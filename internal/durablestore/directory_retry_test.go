package durablestore

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestStoreDirectoryRetriesFailedParentBarrier(t *testing.T) {
	for _, failure := range []string{"first", "second", "ledger"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			paths := []string{filepath.Join(root, "first"), filepath.Join(root, "first", "second"), filepath.Join(root, "first", "second", "ledger")}
			dir := paths[len(paths)-1]
			var failedPath string
			for _, path := range paths {
				if filepath.Base(path) == failure {
					failedPath = path
				}
			}
			failedParent := filepath.Dir(failedPath)
			failedCalls := 0
			for attempt := range 2 {
				lock, _, err := OpenLock(dir, "retry.lock", func(parent string) error {
					if parent == failedParent {
						failedCalls++
						return syscall.EIO
					}
					return SyncDirectory(parent)
				})
				if lock != nil {
					_ = lock.Close()
					t.Fatal("retry initialized ownership without completing the failed parent barrier")
				}
				if !errors.Is(err, syscall.EIO) || failedCalls != attempt+1 {
					t.Fatal("retry skipped the failed directory entry's durability barrier")
				}
				info, statErr := os.Lstat(failedPath)
				if statErr != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
					t.Fatal("failed barrier did not preserve its private directory for retry")
				}
				for _, name := range []string{"retry.lock", "state.json"} {
					if _, statErr := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(statErr) {
						t.Fatal("failed retry created an ownership record")
					}
				}
			}
			retried := false
			lock, created, err := OpenLock(dir, "retry.lock", func(parent string) error {
				if parent == failedParent {
					retried = true
				}
				if _, statErr := os.Lstat(filepath.Join(dir, "retry.lock")); !os.IsNotExist(statErr) {
					t.Error("initialization witness preceded retry durability")
				}
				return SyncDirectory(parent)
			})
			if err != nil || lock == nil || !created || !retried {
				t.Fatal("successful parent barrier could not resume initialization")
			}
			if err := lock.Close(); err != nil {
				t.Fatal("could not close initialized lock")
			}
		})
	}
}

func TestStoreDirectoryCompetingMkdirRequiresParentBarrier(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "first")
	dir := filepath.Join(parent, "ledger")
	competingMkdir, checked := false, false
	lock, _, err := OpenLock(dir, "retry.lock", func(path string) error {
		if path == root {
			// The target was absent at entry. Materialize it while the first
			// ancestor barrier runs, so its later mkdir takes the EEXIST branch.
			if err := os.Mkdir(dir, 0o700); err != nil {
				return err
			}
			competingMkdir = true
		}
		if path == parent {
			checked = true
			return syscall.EIO
		}
		return SyncDirectory(path)
	})
	if lock != nil {
		_ = lock.Close()
		t.Fatal("competing mkdir bypassed its parent durability barrier")
	}
	if !competingMkdir || !checked || !errors.Is(err, syscall.EIO) {
		t.Fatal("EEXIST did not require a successful parent barrier")
	}
	if _, err := os.Lstat(filepath.Join(dir, "retry.lock")); !os.IsNotExist(err) {
		t.Fatal("competing mkdir permitted ownership initialization after failed sync")
	}
	lock, created, err := OpenLock(dir, "retry.lock", SyncDirectory)
	if err != nil || lock == nil || !created {
		t.Fatal("durable retry after competing mkdir failed")
	}
	if err := lock.Close(); err != nil {
		t.Fatal("could not close initialized lock")
	}
}
