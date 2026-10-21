package broker

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

// These shapes mirror harnessv2.FoundryBrokerIdentity, the broker body of
// FoundryBootRetirementRequest, and FoundryBootRetirementProof in Orka.
type identityResponse struct {
	Protocol                 string `json:"protocol"`
	LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
}

type retireBootRequest struct {
	Protocol                 string `json:"protocol"`
	LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
	RetiredFence             owner  `json:"retiredFence"`
	OperationID              string `json:"operationID"`
}

type bootRetirementProof struct {
	Protocol                 string `json:"protocol"`
	LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
	OperationID              string `json:"operationID"`
	ContextSHA256            string `json:"contextSHA256"`
	RetiredFenceDigest       string `json:"retiredFenceDigest"`
	State                    string `json:"state"`
	Sealed                   bool   `json:"sealed"`
	SettlementProven         bool   `json:"settlementProven"`
	RetirementProven         bool   `json:"retirementProven"`
	OwnerCount               uint64 `json:"ownerCount"`
	OwnerSetDigest           string `json:"ownerSetDigest"`
	ActiveInvocations        uint64 `json:"activeInvocations"`
	AmbiguousInvocations     uint64 `json:"ambiguousInvocations"`
	PendingCreates           uint64 `json:"pendingCreates"`
	ProofDigest              string `json:"proofDigest"`
}

// canonicalDigest matches FoundryBootRetirementProof.CanonicalProofDigest: it
// binds the evidence, not the operation that asked for it.
func (p bootRetirementProof) canonicalDigest() string {
	return config.JSONDigest(struct {
		Protocol                 string `json:"protocol"`
		LedgerIdentityDigest     string `json:"ledgerIdentityDigest"`
		AgentConfigurationDigest string `json:"agentConfigurationDigest"`
		RetiredFenceDigest       string `json:"retiredFenceDigest"`
		State                    string `json:"state"`
		Sealed                   bool   `json:"sealed"`
		SettlementProven         bool   `json:"settlementProven"`
		RetirementProven         bool   `json:"retirementProven"`
		OwnerCount               uint64 `json:"ownerCount"`
		OwnerSetDigest           string `json:"ownerSetDigest"`
		ActiveInvocations        uint64 `json:"activeInvocations"`
		AmbiguousInvocations     uint64 `json:"ambiguousInvocations"`
		PendingCreates           uint64 `json:"pendingCreates"`
	}{p.Protocol, p.LedgerIdentityDigest, p.AgentConfigurationDigest, p.RetiredFenceDigest,
		p.State, p.Sealed, p.SettlementProven, p.RetirementProven, p.OwnerCount,
		p.OwnerSetDigest, p.ActiveInvocations, p.AmbiguousInvocations, p.PendingCreates})
}

func (b *lifecycleBroker) serveRecovery(w http.ResponseWriter, path string, body []byte) {
	if path == identityPath {
		b.mu.Lock()
		response := identityResponse{Protocol: brokerProtocol, LedgerIdentityDigest: b.ledger.Identity, AgentConfigurationDigest: b.cfg.configDigest}
		storageErr := b.storageError
		b.mu.Unlock()
		if storageErr != nil {
			writeError(w, storageErr)
			return
		}
		writeJSON(w, http.StatusOK, response)
		return
	}
	var request retireBootRequest
	b.mu.Lock()
	identity := b.ledger.Identity
	b.mu.Unlock()
	if strictjson.Decode(body, &request, true) != nil || request.Protocol != brokerProtocol ||
		request.LedgerIdentityDigest != identity || request.AgentConfigurationDigest != b.cfg.configDigest ||
		!request.RetiredFence.bootValid() || !config.SafeString(request.OperationID, 512) {
		writeError(w, errInvalid)
		return
	}
	keys, err := b.sealBoot(request.RetiredFence)
	if err != nil {
		writeError(w, err)
		return
	}
	for _, key := range keys {
		b.startReconcile(key, true)
	}
	b.mu.Lock()
	proof := b.bootProofLocked(request, config.Digest(body))
	storageErr := b.storageError
	b.mu.Unlock()
	if storageErr != nil {
		writeError(w, storageErr)
		return
	}
	status := http.StatusOK
	if proof.State != stateRetired {
		status = http.StatusConflict
	}
	writeJSON(w, status, proof)
}

// sealBoot permanently refuses admission for the boot, then retires each of
// its sessions through the ordinary proven path.
func (b *lifecycleBroker) sealBoot(fence owner) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var keys []string
	now := b.cfg.now()
	err := b.commitLocked(false, func(next *ledger) error {
		sealKey := config.JSONDigest(fence)
		if next.SealedBoots[sealKey] == nil {
			if len(next.SealedBoots) >= maxSealedBoots {
				return errCapacity
			}
			if next.SealedBoots == nil {
				next.SealedBoots = map[string]*sealedBoot{}
			}
			next.SealedBoots[sealKey] = &sealedBoot{Fence: fence, SealedAt: now}
		}
		keys = keys[:0]
		for key, s := range next.Sessions {
			if !s.Owner.sameBoot(fence) {
				continue
			}
			keys = append(keys, key)
			if !s.Retired {
				s.Retiring = true
				for _, p := range s.Prompts {
					p.startClosing(now)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if a, ok := b.active[key]; ok {
			a.cancel()
		}
	}
	return keys, nil
}

func (b *lifecycleBroker) bootProofLocked(request retireBootRequest, contextDigest string) bootRetirementProof {
	proof := bootRetirementProof{
		Protocol: brokerProtocol, LedgerIdentityDigest: b.ledger.Identity, AgentConfigurationDigest: b.cfg.configDigest,
		OperationID: request.OperationID, ContextSHA256: contextDigest, RetiredFenceDigest: config.JSONDigest(request.RetiredFence),
		Sealed: b.ledger.SealedBoots[config.JSONDigest(request.RetiredFence)] != nil, SettlementProven: true, RetirementProven: true,
	}
	owners := []string{}
	for key, s := range b.ledger.Sessions {
		if !s.Owner.sameBoot(request.RetiredFence) {
			continue
		}
		owners = append(owners, key)
		if s.CreateState == createIntent {
			proof.PendingCreates++
		}
		for _, p := range s.Prompts {
			proof.SettlementProven = proof.SettlementProven && p.Settled
			for _, inv := range p.Invocations {
				switch inv.State {
				case invocationIntent:
					proof.AmbiguousInvocations++
				case invocationReserved, invocationAccepted:
					proof.ActiveInvocations++
				}
			}
		}
		proof.RetirementProven = proof.RetirementProven && s.Retired
	}
	slices.Sort(owners)
	proof.OwnerCount, proof.OwnerSetDigest = uint64(len(owners)), config.JSONDigest(owners)
	proof.State = "retiring"
	if proof.Sealed && proof.SettlementProven && proof.RetirementProven &&
		proof.ActiveInvocations+proof.AmbiguousInvocations+proof.PendingCreates == 0 {
		proof.State = stateRetired
	}
	proof.ProofDigest = proof.canonicalDigest()
	return proof
}

// sealed reports whether an owner's boot was retired.
func (l *ledger) sealed(o owner) bool {
	for _, sealed := range l.SealedBoots {
		if o.sameBoot(sealed.Fence) {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
