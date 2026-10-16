package durablestore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestStoreDirectorySyncsNewEntriesBeforeInitialization(t *testing.T) {
	root := t.TempDir()
	parents := []string{root, filepath.Join(root, "first"), filepath.Join(root, "first", "second")}
	dir := filepath.Join(root, "first", "second", "ledger")
	var synced []string
	lock, created, err := OpenLock(dir, "fixture.lock", func(parent string) error {
		synced = append(synced, parent)
		if _, err := os.Lstat(filepath.Join(dir, "fixture.lock")); !os.IsNotExist(err) {
			t.Error("initialization witness preceded parent durability")
		}
		return SyncDirectory(parent)
	})
	if err != nil {
		t.Fatal("could not initialize nested private state directory")
	}
	if err := lock.Close(); err != nil {
		t.Fatal("could not release initialized directory")
	}
	if !created || !reflect.DeepEqual(synced, append([]string{filepath.Dir(root)}, parents...)) {
		t.Fatal("created directory entries were not durably ordered before initialization")
	}
	for _, path := range append(parents[1:], dir) {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatal("new state directory component was not private")
		}
	}
	synced = nil
	lock, created, err = OpenLock(dir, "fixture.lock", func(parent string) error {
		synced = append(synced, parent)
		return SyncDirectory(parent)
	})
	if err != nil {
		t.Fatal("existing directory could not recover after parent durability")
	}
	if err := lock.Close(); err != nil || created {
		t.Fatal("existing initialization witness was recreated")
	}
	if !reflect.DeepEqual(synced, []string{filepath.Dir(dir)}) {
		t.Fatal("existing directory skipped its parent durability barrier")
	}
}

func TestStoreDirectoryParentSyncFailurePreventsInitialization(t *testing.T) {
	for _, failure := range []string{"first", "second", "ledger"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			paths := []string{filepath.Join(root, "first"), filepath.Join(root, "first", "second"), filepath.Join(root, "first", "second", "ledger")}
			parents := []string{filepath.Dir(root), root, paths[0], paths[1]}
			dir := paths[len(paths)-1]
			calls := 0
			failureIndex := -1
			for i, path := range paths {
				if filepath.Base(path) == failure {
					failureIndex = i
				}
			}
			lock, _, err := OpenLock(dir, "fixture.lock", func(parent string) error {
				index := calls
				calls++
				if parent != parents[index] {
					t.Error("unexpected parent sync order")
				}
				if index == failureIndex+1 {
					return syscall.EIO
				}
				return SyncDirectory(parent)
			})
			if lock != nil {
				_ = lock.Close()
				t.Fatal("failed parent sync granted an initialization lock")
			}
			if !errors.Is(err, syscall.EIO) || calls != failureIndex+2 {
				t.Fatal("parent durability failure was ignored or initialization continued")
			}
			for _, name := range []string{"fixture.lock", "state.json"} {
				if _, err := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(err) {
					t.Fatal("failed parent sync created an ownership record")
				}
			}
			for _, path := range paths[failureIndex+1:] {
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Fatal("directory creation continued beyond a failed durability barrier")
				}
			}
		})
	}
}
