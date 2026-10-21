package storetest

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/durablestore"
)

type Fixture struct {
	LockName string
	Digest   string
	Open     func(string) (func(), string, error)
	Save     func(string) error
}

func MissingLedgerFailsClosed(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	closeStore, _, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("could not initialize ownership fixture")
	}
	err = fixture.Save(dir)
	closeStore()
	if err != nil {
		t.Fatal("could not persist owned lifetime")
	}
	lockPath := filepath.Join(dir, fixture.LockName)
	lockInfo, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal("ownership witness is missing")
	}
	statePath := filepath.Join(dir, "state.json")
	retained := filepath.Join(t.TempDir(), "retained-state.json")
	if os.Rename(statePath, retained) != nil {
		t.Fatal("could not simulate missing ownership state")
	}
	for range 2 {
		closeStore, _, err = fixture.Open(dir)
		if err == nil {
			closeStore()
			t.Fatal("missing ownership ledger was silently reinitialized")
		}
		if _, err := os.Lstat(statePath); !os.IsNotExist(err) {
			t.Fatal("rejected recovery recreated state")
		}
		after, err := os.Lstat(lockPath)
		if err != nil || !os.SameFile(lockInfo, after) {
			t.Fatal("rejected recovery replaced its initialization witness")
		}
	}
	if os.Rename(retained, statePath) != nil {
		t.Fatal("could not restore exact original ownership")
	}
	closeStore, restored, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("valid original ownership could not recover")
	}
	closeStore()
	if restored != fixture.Digest {
		t.Fatal("recovery changed the owned lifetime")
	}

}

func EmptyDirectoryAndLegacyRecovery(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	if os.Mkdir(dir, 0o700) != nil {
		t.Fatal("could not create private pre-existing directory")
	}
	closeStore, _, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("pre-existing empty directory could not initialize")
	}
	err = fixture.Save(dir)
	closeStore()
	if err != nil {
		t.Fatal("could not persist owned lifetime")
	}
	for _, missingLock := range []bool{false, true} {
		if missingLock && os.Remove(filepath.Join(dir, fixture.LockName)) != nil {
			t.Fatal("could not prepare valid ledger without a lock")
		}
		closeStore, restored, err := fixture.Open(dir)
		if err != nil {
			t.Fatal("valid existing ownership could not recover")
		}
		closeStore()
		if restored != fixture.Digest {
			t.Fatal("existing ownership was rebound during recovery")
		}
	}

}

func InitializerRace(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	if os.Mkdir(dir, 0o700) != nil {
		t.Fatal("could not create private initializer directory")
	}
	// Pause the original creator after exclusive lock-file creation,
	// before flock. A competing opener can acquire flock first, but
	// it must not claim the original creator's initialization rights.
	witness, err := os.OpenFile(filepath.Join(dir, fixture.LockName), os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal("could not pause original initialization")
	}
	defer witness.Close()
	closeStore, _, err := fixture.Open(dir)
	if err == nil {
		closeStore()
		t.Fatal("competing opener initialized another process's store")
	}
	if _, err := os.Lstat(filepath.Join(dir, "state.json")); !os.IsNotExist(err) {
		t.Fatal("competing opener published a ledger")
	}
	// Resume the original creator using its same descriptor. The
	// rejected contender must release flock and preserve that inode.
	if syscall.Flock(int(witness.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		t.Fatal("rejected contender retained the initialization lock")
	}
	if fixture.Save(dir) != nil {
		t.Fatal("original creator could not publish its ledger")
	}
	if syscall.Flock(int(witness.Fd()), syscall.LOCK_UN) != nil {
		t.Fatal("original creator could not release the store")
	}
	closeStore, restored, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("competing opener could not read the completed original store")
	}
	closeStore()
	if restored != fixture.Digest {
		t.Fatal("competing opener replaced original ownership")
	}

}

func CreatorKeepsLock(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal("could not create private initializer directory")
	}
	path := filepath.Join(dir, fixture.LockName)
	// Pause creator A after its successful O_EXCL, before the exact
	// acquisition helper used by openStoreLock. B opens A's same inode.
	creator, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal("could not create initialization witness")
	}
	defer creator.Close()
	original, err := creator.Stat()
	if err != nil {
		t.Fatal("could not inspect creator witness")
	}
	contender, created, err := durablestore.OpenLock(dir, fixture.LockName, durablestore.SyncDirectory)
	if err != nil || contender == nil || created {
		t.Fatal("contender did not acquire the existing initialization inode")
	}
	defer contender.Close()
	contenderInfo, err := contender.Stat()
	if err != nil || !os.SameFile(original, contenderInfo) {
		t.Fatal("contender did not lock the creator's exact inode")
	}
	if _, err := os.Lstat(filepath.Join(dir, "state.json")); !os.IsNotExist(err) {
		t.Fatal("contender found unexpected ownership state")
	}
	started := make(chan struct{})
	acquired := make(chan error, 1)
	go func() {
		close(started)
		acquired <- durablestore.LockFile(creator, true)
	}()
	<-started
	select {
	case err := <-acquired:
		// This is the original bug: A abandons O_EXCL authority while
		// B is still checking the absent ledger. Neither can initialize.
		_ = contender.Close()
		_ = creator.Close()
		for range 2 {
			closeStore, _, openErr := fixture.Open(dir)
			if openErr == nil {
				closeStore()
				t.Fatal("a later opener fabricated absent initialization authority")
			}
		}
		if errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatal("exclusive creator lost authority to transient flock contention; later openers remain stranded")
		}
		t.Fatal("creator completed acquisition while a contender still held the inode")
	case <-time.After(75 * time.Millisecond):
		// B has no O_EXCL authority. Its caller rejects the absent
		// ledger and closes; A must keep its descriptor while waiting.
	}
	if err := contender.Close(); err != nil {
		t.Fatal("could not release the rejected contender")
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal("original creator could not resume after contender release")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("original creator remained blocked after contender release")
	}
	after, err := creator.Stat()
	pathInfo, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !os.SameFile(original, after) || !os.SameFile(original, pathInfo) {
		t.Fatal("creator replaced or reopened the initialization witness")
	}
	if err := fixture.Save(dir); err != nil {
		t.Fatal("retained creator could not publish original ownership")
	}
	if err := creator.Close(); err != nil {
		t.Fatal("could not close initialized creator")
	}
	closeStore, restored, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("completed original initialization could not recover")
	}
	closeStore()
	if restored != fixture.Digest {
		t.Fatal("recovery replaced the original ownership ledger")
	}

}

func ExistingWriterNonblocking(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	closeWriter, _, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("could not open original writer")
	}
	defer closeWriter()
	if fixture.Save(dir) != nil {
		t.Fatal("could not preserve original ownership")
	}
	finished := make(chan error, 1)
	go func() {
		other, created, err := durablestore.OpenLock(dir, fixture.LockName, durablestore.SyncDirectory)
		if other != nil {
			_ = other.Close()
		}
		if other != nil || created {
			finished <- errors.New("existing writer was not excluded")
			return
		}
		finished <- err
	}()
	select {
	case err := <-finished:
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatal("existing-inode writer did not fail with nonblocking contention")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("existing-inode writer waited for the active owner")
	}
	closeWriter()
	closeRecovered, digest, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("original ownership did not recover after active writer closed")
	}
	closeRecovered()
	if digest != fixture.Digest {
		t.Fatal("excluded writer changed original ownership")
	}

}

func LegacyRecoveryNonblocking(t *testing.T, fixture Fixture) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "ledger")
	if os.Mkdir(dir, 0o700) != nil || fixture.Save(dir) != nil {
		t.Fatal("could not prepare valid legacy ownership without a lock")
	}
	statePath := filepath.Join(dir, "state.json")
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal("could not inspect original legacy ownership")
	}
	path := filepath.Join(dir, fixture.LockName)
	// A creates the missing lock for a valid legacy ledger, then B
	// recovers that ledger on the same inode before A calls flock.
	creator, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal("could not create the legacy recovery lock")
	}
	defer creator.Close()
	original, err := creator.Stat()
	if err != nil {
		t.Fatal("could not inspect the legacy recovery lock")
	}
	closeWriter, restored, err := fixture.Open(dir)
	if err != nil || restored != fixture.Digest {
		if closeWriter != nil {
			closeWriter()
		}
		t.Fatal("contender could not recover the original legacy ledger")
	}
	defer closeWriter()
	contenderInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(original, contenderInfo) {
		t.Fatal("contender replaced the creator's lock inode")
	}
	finished := make(chan error, 1)
	go func() { finished <- durablestore.LockFile(creator, true) }()
	select {
	case err := <-finished:
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatal("new legacy recovery lock did not fail with nonblocking contention")
		}
	case <-time.After(2 * time.Second):
		// Release B only after proving A waited behind a valid owner.
		// Join A so the regression never leaks a blocked syscall.
		closeWriter()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("legacy recovery acquisition did not finish after owner release")
		}
		t.Fatal("newly created legacy recovery lock waited behind the retained ledger owner")
	}
	after, err := os.ReadFile(statePath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("excluded legacy recovery changed original ownership")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(original, pathInfo) {
		t.Fatal("excluded legacy recovery replaced the lock inode")
	}
	closeWriter()
	closeRecovered, restored, err := fixture.Open(dir)
	if err != nil {
		t.Fatal("legacy ownership could not recover after the active owner closed")
	}
	closeRecovered()
	if restored != fixture.Digest {
		t.Fatal("legacy recovery changed the owned lifetime")
	}

}
