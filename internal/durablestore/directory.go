package durablestore

import (
	"os"
	"path/filepath"
	"syscall"
)

// The permanent lock is also an initialization witness. Only its exclusive
// creator may initialize absent state; existing ledgers do not need a new marker.
func OpenLock(dir, name string, syncParent func(string) error) (*os.File, bool, error) {
	if err := makeDirectory(dir, syncParent); err != nil {
		return nil, false, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, false, err
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || !info.IsDir() || info.Mode().Perm() != 0o700 || int(stat.Uid) != os.Geteuid() {
		return nil, false, os.ErrPermission
	}
	path := filepath.Join(dir, name)
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	created := err == nil
	if os.IsExist(err) {
		lock, err = os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	}
	if err != nil {
		return nil, false, err
	}
	if info, err := lock.Stat(); err != nil || !PrivateFile(info) {
		_ = lock.Close()
		return nil, false, os.ErrPermission
	}
	if err := LockFile(lock, created); err != nil {
		_ = lock.Close()
		return nil, false, err
	}
	return lock, created, nil
}

func LockFile(lock *os.File, created bool) error {
	operation := syscall.LOCK_EX | syscall.LOCK_NB
	if created {
		_, err := os.Lstat(filepath.Join(filepath.Dir(lock.Name()), "state.json"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if os.IsNotExist(err) {
			// Keep the creator's O_EXCL authority on the same descriptor
			// while an earlier locker rejects the absent ledger. With an
			// existing legacy ledger, that locker may be its active owner,
			// so recovery must retain nonblocking exclusion instead.
			operation = syscall.LOCK_EX
		}
	}
	for {
		err := syscall.Flock(int(lock.Fd()), operation)
		if err != syscall.EINTR {
			return err
		}
	}
}

// Persist a directory entry before descending into it or creating ownership
// records. An existing directory may remain after a failed sync or be created
// by a competing initializer, so it needs the same parent barrier. Recursion
// stops at the first existing ancestor; every new descendant follows its sync.
func makeDirectory(dir string, syncParent func(string) error) error {
	parent := filepath.Dir(dir)
	info, err := os.Stat(dir)
	if err == nil {
		if info.IsDir() {
			if parent == dir {
				return nil
			}
			return syncParent(parent)
		}
		return &os.PathError{Op: "mkdir", Path: dir, Err: syscall.ENOTDIR}
	}
	if !os.IsNotExist(err) {
		return err
	}
	if parent == dir {
		return err
	}
	if err := makeDirectory(parent, syncParent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		if os.IsExist(err) {
			if info, statErr := os.Stat(dir); statErr == nil && info.IsDir() {
				return syncParent(parent)
			}
		}
		return err
	}
	return syncParent(parent)
}

func SyncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
