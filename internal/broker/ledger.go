package broker

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/durablestore"
	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

const (
	maxLedgerBytes = 32 << 20
	// Ordinary growth stops below the hard limit so settlement and retirement
	// receipts can always be recorded for sessions already admitted.
	ordinaryLedgerBytes  = 24 << 20
	maxSessions          = 4096
	maxPromptsPerSession = 4096
	// maxFencedPods bounds the Agent pods a kagent v0 fence records.
	maxFencedPods = 16
	maxOperations = 16384
	// A seal is permanent for the ledger's lifetime. At capacity, retiring
	// another boot fails closed until the ledger is replaced.
	maxSealedBoots       = 4096
	settleOperationLimit = maxOperations + 1
	retireOperationLimit = settleOperationLimit + 1
)

// Remote session creation states.
const (
	createNone    = "none"
	createIntent  = "intent"
	createKnown   = "known"
	createDeleted = "deleted"
)

// Invocation states. reserved: admitted, nothing sent. intent: the message may
// have reached kagent. accepted: kagent assigned a task. completed: the task
// was observed terminal. rejected: kagent proved the message never ran.
// settled: closed by settlement without a terminal observation of its own,
// either because nothing was sent or because kagent deleted the session.
const (
	invocationReserved  = "reserved"
	invocationIntent    = "intent"
	invocationAccepted  = "accepted"
	invocationCompleted = "completed"
	invocationRejected  = "rejected"
	invocationSettled   = "settled"
)

// ledger holds identifiers and states only. Prompts, outputs, and tool
// arguments are never persisted.
type ledger struct {
	Version      uint32 `json:"version"`
	ConfigDigest string `json:"configDigest"`
	// Identity distinguishes this ledger from any replacement, so a new ledger
	// can never prove that an older one's sessions were cleaned up.
	Identity string              `json:"identity,omitempty"`
	Sessions map[string]*session `json:"sessions"`
	// SealedBoots are supervisor boots Orka retired: they admit nothing again.
	SealedBoots map[string]*sealedBoot `json:"sealedBoots,omitempty"`
}

type sealedBoot struct {
	Fence    owner     `json:"fence"`
	SealedAt time.Time `json:"sealedAt"`
}

type session struct {
	Owner     owner  `json:"owner"`
	UserID    string `json:"userID"`
	RequestID string `json:"requestID"`
	// RemoteID is the kagent Session ID, which is also the A2A context ID.
	RemoteID         string `json:"remoteID,omitempty"`
	Revision         string `json:"revision,omitempty"`
	RevisionMismatch bool   `json:"revisionMismatch,omitempty"`
	// RemoteGone records that kagent reported the session deleted or
	// expiring. A gone session never dispatches again.
	RemoteGone  bool   `json:"remoteGone,omitempty"`
	CreateState string `json:"createState"`
	Retiring    bool   `json:"retiring"`
	Retired     bool   `json:"retired"`
	// RetiredAt starts the retention period after which the record is pruned.
	RetiredAt   time.Time `json:"retiredAt,omitzero"`
	ProofDigest string    `json:"proofDigest,omitempty"`
	// QuarantinedAt is set when a prompt's cleanup outlives its deadline.
	// A quarantined session accepts no new prompts; cleanup keeps trying.
	QuarantinedAt time.Time          `json:"quarantinedAt,omitzero"`
	CurrentPrompt string             `json:"currentPrompt,omitempty"`
	Prompts       map[string]*prompt `json:"prompts"`
	Operations    map[string]string  `json:"operations"`
}

type prompt struct {
	Identity        brokerContext          `json:"identity"`
	LeaseGeneration uint64                 `json:"leaseGeneration"`
	LeaseExpiresAt  time.Time              `json:"leaseExpiresAt"`
	Closing         bool                   `json:"closing"`
	ClosingSince    time.Time              `json:"closingSince,omitzero"`
	Settled         bool                   `json:"settled"`
	ProofDigest     string                 `json:"proofDigest,omitempty"`
	LastSequence    uint64                 `json:"lastSequence"`
	Invocations     map[uint64]*invocation `json:"invocations"`
}

type invocation struct {
	Sequence    uint64    `json:"sequence"`
	OperationID string    `json:"operationID"`
	BodyDigest  string    `json:"bodyDigest"`
	State       string    `json:"state"`
	MessageID   string    `json:"messageID,omitempty"`
	TaskID      string    `json:"taskID,omitempty"`
	SentAt      time.Time `json:"sentAt,omitzero"`
	// FencedAt records when the broker confirmed a kagent v0 session deleted
	// to fence an ambiguous send, and FencedPods the Agent pods that existed
	// then. An observed send settles once none of those pods is left.
	FencedAt   time.Time `json:"fencedAt,omitzero"`
	FencedPods []string  `json:"fencedPods,omitempty"`
	Terminal   string    `json:"terminal,omitempty"`
}

type store struct {
	dir  string
	lock *os.File
}

func openStore(dir, digest string) (*store, *ledger, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == string(filepath.Separator) || !config.DigestValid(digest) {
		return nil, nil, errStorage
	}
	lock, created, err := durablestore.OpenLock(dir, "broker.lock", durablestore.SyncDirectory)
	if err != nil {
		return nil, nil, errStorage
	}
	s := &store{dir: dir, lock: lock}
	l := &ledger{Version: 1, ConfigDigest: digest, Identity: newLedgerIdentity(), Sessions: map[string]*session{}}
	path := filepath.Join(dir, "state.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if !created {
			// A ledger that existed once must never be silently reinitialized.
			s.close()
			return nil, nil, errStorage
		}
		data, _ := json.Marshal(l)
		if err := s.save(data); err != nil {
			s.close()
			return nil, nil, err
		}
		return s, l, nil
	}
	if err != nil || !durablestore.PrivateFile(info) || info.Size() > maxLedgerBytes {
		s.close()
		return nil, nil, errStorage
	}
	// Decode into an empty ledger so that a null or empty document cannot
	// inherit the defaults above and pass as a valid, empty ledger.
	data, err := os.ReadFile(path)
	loaded := &ledger{}
	if err != nil || strictjson.Decode(data, loaded, true) != nil || !ledgerValid(loaded, digest) {
		s.close()
		return nil, nil, errStorage
	}
	return s, loaded, nil
}

func (s *store) save(data []byte) error {
	if len(data) > maxLedgerBytes {
		return errCapacity
	}
	name := filepath.Join(s.dir, ".state-"+uuid.NewString())
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errStorage
	}
	defer func() { _ = os.Remove(name) }()
	_, err = io.Copy(file, strings.NewReader(string(data)))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil || os.Rename(name, filepath.Join(s.dir, "state.json")) != nil {
		return errStorage
	}
	if durablestore.SyncDirectory(s.dir) != nil {
		return errStorage
	}
	return nil
}

func (s *store) close() {
	if s != nil && s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
	}
}

func newLedgerIdentity() string {
	return config.Digest([]byte(uuid.NewString()))
}

// Recovery never interprets an unknown state as quiescence.
func ledgerValid(l *ledger, digest string) bool {
	if l.Version != 1 || l.ConfigDigest != digest || l.Sessions == nil || len(l.Sessions) > maxSessions ||
		(l.Identity != "" && !config.DigestValid(l.Identity)) || len(l.SealedBoots) > maxSealedBoots {
		return false
	}
	for key, sealed := range l.SealedBoots {
		if sealed == nil || !sealed.Fence.bootValid() || key != config.JSONDigest(sealed.Fence) || sealed.SealedAt.IsZero() {
			return false
		}
	}
	for key, s := range l.Sessions {
		if !sessionValid(s, key, digest) {
			return false
		}
	}
	return true
}

func sessionValid(s *session, key, digest string) bool {
	if s == nil || !s.Owner.valid() || key != config.JSONDigest(s.Owner) || s.Prompts == nil || s.Operations == nil ||
		len(s.Prompts) > maxPromptsPerSession || len(s.Operations) > retireOperationLimit ||
		!config.SafeString(s.UserID, 128) || !config.SafeString(s.RequestID, 128) {
		return false
	}
	switch s.CreateState {
	case createNone:
		if s.RemoteID != "" || s.Revision != "" || s.RevisionMismatch {
			return false
		}
	case createIntent:
		if s.RemoteID != "" || s.Revision != "" || s.RevisionMismatch {
			return false
		}
	case createKnown, createDeleted:
		// kagent v1 assigns a UUID; kagent v0 keeps the request ID the broker
		// creates the session with.
		if id, err := uuid.Parse(s.RemoteID); (err != nil || id.String() != s.RemoteID) && s.RemoteID != s.RequestID {
			return false
		}
	default:
		return false
	}
	if s.RemoteGone && s.CreateState != createKnown && s.CreateState != createDeleted {
		return false
	}
	if s.Retired {
		if !s.Retiring || (s.CreateState != createNone && s.CreateState != createDeleted) || !config.DigestValid(s.ProofDigest) {
			return false
		}
	} else if s.CreateState == createDeleted || s.ProofDigest != "" || !s.RetiredAt.IsZero() {
		return false
	}
	if (len(s.Prompts) == 0) != (s.CurrentPrompt == "") || (s.CurrentPrompt != "" && s.Prompts[s.CurrentPrompt] == nil) {
		return false
	}
	for id, operation := range s.Operations {
		if !config.SafeString(id, 512) || !config.DigestValid(operation) {
			return false
		}
	}
	for key, p := range s.Prompts {
		if !promptValid(p, key, digest, s) {
			return false
		}
	}
	return true
}

func promptValid(p *prompt, key, digest string, s *session) bool {
	if p == nil || p.Identity.promptKey() != key || p.Identity.Owner != s.Owner || p.Identity.Protocol != brokerProtocol ||
		p.Identity.AgentConfigurationDigest != digest || !config.SafeString(p.Identity.PromptID, 512) ||
		s.Operations[p.Identity.OperationID] == "" || p.Invocations == nil || len(p.Invocations) > 1 ||
		p.Identity.LeaseGeneration == 0 || p.LeaseGeneration < p.Identity.LeaseGeneration || p.LeaseExpiresAt.IsZero() {
		return false
	}
	if p.Settled {
		if !p.Closing || !config.DigestValid(p.ProofDigest) {
			return false
		}
	} else if p.ProofDigest != "" || s.Retired || key != s.CurrentPrompt {
		return false
	}
	if (s.Retiring && !p.Closing) || (!p.Closing && !p.ClosingSince.IsZero()) {
		return false
	}
	var last uint64
	for sequence, inv := range p.Invocations {
		if !invocationValid(inv, sequence, p, s) {
			return false
		}
		last = max(last, sequence)
	}
	return p.LastSequence == last
}

func invocationValid(inv *invocation, sequence uint64, p *prompt, s *session) bool {
	if inv == nil || inv.Sequence != sequence || sequence == 0 || !config.DigestValid(inv.BodyDigest) ||
		!config.SafeString(inv.OperationID, 512) || s.Operations[inv.OperationID] == "" ||
		(!inv.FencedAt.IsZero() && inv.State != invocationIntent && inv.State != invocationSettled) ||
		(inv.FencedAt.IsZero() && len(inv.FencedPods) != 0) || len(inv.FencedPods) > maxFencedPods {
		return false
	}
	for _, uid := range inv.FencedPods {
		if !config.SafeString(uid, 64) {
			return false
		}
	}
	switch inv.State {
	case invocationReserved:
		return inv.MessageID == "" && inv.TaskID == "" && !p.Settled
	case invocationIntent:
		return config.SafeString(inv.MessageID, 128) && inv.TaskID == "" && !inv.SentAt.IsZero() && !p.Settled
	case invocationAccepted:
		return config.SafeString(inv.MessageID, 128) && config.SafeString(inv.TaskID, 512) && !p.Settled
	case invocationCompleted:
		return config.SafeString(inv.MessageID, 128) && config.SafeString(inv.TaskID, 512) && config.SafeString(inv.Terminal, 64)
	case invocationRejected:
		return config.SafeString(inv.MessageID, 128) && inv.TaskID == ""
	case invocationSettled:
		return p.Settled
	default:
		return false
	}
}
