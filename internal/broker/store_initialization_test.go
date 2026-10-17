package broker

import (
	"encoding/json"
	"testing"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/durablestore/storetest"
)

// brokerStoreFixture runs the shared durable-store initialization checks
// against the broker's ledger store.
func brokerStoreFixture(t *testing.T) storetest.Fixture {
	t.Helper()
	digest := config.Digest([]byte("durable-broker-fixture"))
	saved := &ledger{Version: 1, ConfigDigest: digest, Identity: config.Digest([]byte("fixture-ledger")), Sessions: map[string]*session{}}
	if !ledgerValid(saved, digest) {
		t.Fatal("invalid broker ledger fixture")
	}
	data, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	return storetest.Fixture{
		LockName: "broker.lock", Digest: config.JSONDigest(saved),
		Open: func(dir string) (func(), string, error) {
			s, loaded, err := openStore(dir, digest)
			if err != nil {
				return nil, "", err
			}
			return s.close, config.JSONDigest(loaded), nil
		},
		Save: func(dir string) error { return (&store{dir: dir}).save(data) },
	}
}

func TestDurableStoreMissingLedgerFailsClosed(t *testing.T) {
	storetest.MissingLedgerFailsClosed(t, brokerStoreFixture(t))
}

func TestDurableStoreEmptyDirectoryAndLegacyRecovery(t *testing.T) {
	storetest.EmptyDirectoryAndLegacyRecovery(t, brokerStoreFixture(t))
}

func TestDurableStoreInitializerRace(t *testing.T) {
	storetest.InitializerRace(t, brokerStoreFixture(t))
}

func TestStoreInitializerKeepsCreatorAcrossFlockContention(t *testing.T) {
	storetest.CreatorKeepsLock(t, brokerStoreFixture(t))
}

func TestStoreInitializerExistingWriterRemainsNonblocking(t *testing.T) {
	storetest.ExistingWriterNonblocking(t, brokerStoreFixture(t))
}

func TestStoreInitializerLegacyRecoveryRemainsNonblocking(t *testing.T) {
	storetest.LegacyRecoveryNonblocking(t, brokerStoreFixture(t))
}
