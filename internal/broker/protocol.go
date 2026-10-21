package broker

import (
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

// The lifecycle broker protocol is shared with Orka's supervisor, which
// defined it first for Foundry. Keep these values identical to
// workers/acp/supervisor/foundry_broker.go in Orka.
const (
	brokerProtocol      = "orka.foundry.broker.v1"
	brokerContextHeader = "X-Orka-Foundry-Context"
	brokerMaxContext    = 16 << 10

	responsesPath = "/v1/responses"
	renewPath     = "/internal/v1/renew"
	settlePath    = "/internal/v1/settle"
	retirePath    = "/internal/v1/retire"
	statusPath    = "/internal/v1/status"

	// Supervisor restart recovery: Orka reads the ledger identity before
	// admission, then retires a dead supervisor boot through its successor.
	identityPath   = "/internal/v1/identity"
	retireBootPath = "/internal/v1/retire-boot"
)

// Proven control states; Orka treats every other state as unproven.
const (
	stateSettled = "settled"
	stateRetired = "retired"
)

var (
	errInvalid  = errors.New("invalid kagent broker request")
	errConflict = errors.New("kagent broker operation conflicts with durable ownership")
	errClosed   = errors.New("kagent broker authority is closed")
	errPending  = errors.New("kagent broker remote settlement is pending")
	errStorage  = errors.New("kagent broker durable state is unavailable")
	errCapacity = errors.New("kagent broker durable state is at capacity")
	errRemote   = errors.New("kagent broker remote operation failed")
	errGone     = errors.New("kagent session no longer exists")
)

// owner mirrors Orka's harnessv2.Fence field order and tags. Both sides digest
// json.Marshal(owner), never the untrusted wire bytes.
type owner struct {
	RuntimeInstanceID          string `json:"runtimeInstanceID"`
	SupervisorBootID           string `json:"supervisorBootID"`
	ControllerEpoch            uint64 `json:"controllerEpoch"`
	RuntimePoolUID             string `json:"runtimePoolUID"`
	RuntimePoolGeneration      uint64 `json:"runtimePoolGeneration"`
	RuntimeSessionUID          string `json:"runtimeSessionUID,omitempty"`
	RuntimeSessionGeneration   uint64 `json:"runtimeSessionGeneration,omitempty"`
	RuntimeProfileDigest       string `json:"runtimeProfileDigest"`
	ProfileDigestSchemaVersion uint32 `json:"profileDigestSchemaVersion"`
}

// brokerContext is constructed by the supervisor from authenticated
// controller requests; the ACP child cannot choose any of it.
type brokerContext struct {
	Protocol                 string `json:"protocol"`
	Owner                    owner  `json:"owner"`
	AgentConfigurationDigest string `json:"agentConfigurationDigest"`
	TaskUID                  string `json:"taskUID,omitempty"`
	TaskAttempt              uint32 `json:"taskAttempt,omitempty"`
	PromptID                 string `json:"promptID,omitempty"`
	PromptRequestDigest      string `json:"promptRequestDigest,omitempty"`
	LeaseGeneration          uint64 `json:"leaseGeneration,omitempty"`
	LeaseExpiresAt           string `json:"leaseExpiresAt,omitempty"`
	OperationID              string `json:"operationID"`
	InvocationSequence       uint64 `json:"invocationSequence,omitempty"`
	BodySHA256               string `json:"bodySHA256"`
}

type controlResponse struct {
	Protocol             string `json:"protocol"`
	OwnerDigest          string `json:"ownerDigest"`
	OperationID          string `json:"operationID"`
	ContextSHA256        string `json:"contextSHA256"`
	State                string `json:"state"`
	SettlementProven     bool   `json:"settlementProven"`
	RetirementProven     bool   `json:"retirementProven"`
	ActiveInvocations    uint32 `json:"activeInvocations"`
	AmbiguousInvocations uint32 `json:"ambiguousInvocations"`
	CreatePending        bool   `json:"createPending"`
	RemoteSessionCreated bool   `json:"remoteSessionCreated"`
	LeaseGeneration      uint64 `json:"leaseGeneration"`
	LeaseExpiresAt       string `json:"leaseExpiresAt"`
	ProofDigest          string `json:"proofDigest"`
}

func (o owner) valid() bool {
	return config.SafeString(o.RuntimeInstanceID, 512) && config.SafeString(o.SupervisorBootID, 512) &&
		o.ControllerEpoch > 0 && config.SafeString(o.RuntimePoolUID, 512) && o.RuntimePoolGeneration > 0 &&
		config.SafeString(o.RuntimeSessionUID, 512) && o.RuntimeSessionGeneration > 0 &&
		config.DigestValid(o.RuntimeProfileDigest) && o.ProfileDigestSchemaVersion == 1
}

// bootValid checks a fence that names a whole supervisor boot.
func (o owner) bootValid() bool {
	session := o
	session.RuntimeSessionUID, session.RuntimeSessionGeneration = "runtime-session", 1
	return o.RuntimeSessionUID == "" && o.RuntimeSessionGeneration == 0 && session.valid()
}

// sameBoot reports whether o belongs to the boot that fence names, at any
// controller epoch.
func (o owner) sameBoot(fence owner) bool {
	return o.RuntimeInstanceID == fence.RuntimeInstanceID && o.SupervisorBootID == fence.SupervisorBootID &&
		o.RuntimePoolUID == fence.RuntimePoolUID && o.RuntimePoolGeneration == fence.RuntimePoolGeneration &&
		o.RuntimeProfileDigest == fence.RuntimeProfileDigest && o.ProfileDigestSchemaVersion == fence.ProfileDigestSchemaVersion
}

func (c brokerContext) promptKey() string {
	return config.JSONDigest(struct {
		TaskUID             string `json:"taskUID"`
		TaskAttempt         uint32 `json:"taskAttempt"`
		PromptID            string `json:"promptID"`
		PromptRequestDigest string `json:"promptRequestDigest"`
	}{c.TaskUID, c.TaskAttempt, c.PromptID, c.PromptRequestDigest})
}

func parseContext(r *http.Request, body []byte, configDigest string, now time.Time) (brokerContext, string, error) {
	values := r.Header.Values(brokerContextHeader)
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > brokerMaxContext {
		return brokerContext{}, "", errInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(values[0])
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != values[0] {
		return brokerContext{}, "", errInvalid
	}
	var c brokerContext
	if strictjson.Decode(raw, &c, true) != nil || c.Protocol != brokerProtocol || !c.Owner.valid() ||
		c.AgentConfigurationDigest != configDigest || !config.DigestValid(configDigest) ||
		!config.SafeString(c.OperationID, 512) || c.BodySHA256 != config.Digest(body) {
		return brokerContext{}, "", errInvalid
	}
	if r.URL.Path != retirePath && r.URL.Path != statusPath {
		expiry, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		if !config.SafeString(c.TaskUID, 512) || c.TaskAttempt == 0 || !config.SafeString(c.PromptID, 512) ||
			!config.DigestValid(c.PromptRequestDigest) || c.LeaseGeneration == 0 || err != nil {
			return brokerContext{}, "", errInvalid
		}
		if (r.URL.Path == responsesPath || r.URL.Path == renewPath) && (!expiry.After(now) || expiry.After(now.Add(5*time.Minute))) {
			return brokerContext{}, "", errClosed
		}
	} else if c.TaskUID != "" || c.TaskAttempt != 0 || c.PromptID != "" || c.PromptRequestDigest != "" ||
		c.LeaseGeneration != 0 || c.LeaseExpiresAt != "" {
		return brokerContext{}, "", errInvalid
	}
	if (r.URL.Path == responsesPath) != (c.InvocationSequence > 0) {
		return brokerContext{}, "", errInvalid
	}
	return c, config.Digest(raw), nil
}

func operationDigest(path string, c brokerContext) string {
	return config.JSONDigest(struct {
		Path    string        `json:"path"`
		Context brokerContext `json:"context"`
	}{path, c})
}
