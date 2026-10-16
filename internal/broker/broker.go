// Package broker implements the lifecycle broker sidecar for wrapped kagent
// Agents. It owns the kagent identity and a durable ledger, and it only
// reports settlement or retirement once kagent state proves it.
package broker

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

const (
	maxRequestBytes = 4 << 20
	// kagent fences an unused dispatch reservation two minutes after it
	// grants it. An ambiguous send that shows no task after this window is
	// fenced by deleting its session: the window is a courtesy, not a proof,
	// because admission may start later than the send.
	dispatchFenceWindow = 150 * time.Second
	reconcileBackoff    = time.Second
	sweepInterval       = 250 * time.Millisecond
	// Cleanup that has not proven settlement by this deadline needs an
	// operator: kagent can lose a task to an agent crash before settlement.
	defaultCleanupDeadline = 10 * time.Minute
	quarantineBackoff      = 30 * time.Second
	// Orka records retirement durably once proven, so a retired record is
	// kept only long enough to answer retried retire calls.
	defaultRetiredRetention = 24 * time.Hour
)

// agentPods is the kagent v0 client's view of the pods that run the Agent.
// An execution lives in its pod's process, so a pod that is gone has nothing
// left running.
type agentPods interface {
	AgentPodUIDs(ctx context.Context) ([]string, error)
}

// remote is the kagent surface the broker depends on.
type remote interface {
	CreateSession(ctx context.Context, userID, requestID string) (kagent.Session, error)
	GetSession(ctx context.Context, userID, id string) (kagent.Session, error)
	DeleteSession(ctx context.Context, userID, id string) error
	Stream(ctx context.Context, userID, contextID, messageID, text, authorization string) iter.Seq2[a2atype.Event, error]
	GetTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error)
	FindTaskByMessage(ctx context.Context, userID, contextID, messageID string) (*a2atype.Task, error)
	CancelTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error)
}

type settings struct {
	bearer           string
	configDigest     string
	stateDir         string
	model            string
	expectedRevision string
	userIDPrefix     string
	// kagentV0 applies kagent v0.x rules: deleting a session does not stop its
	// work, so deletion never proves settlement.
	kagentV0         bool
	operationTimeout time.Duration
	keepalive        time.Duration
	cleanupDeadline  time.Duration
	retiredRetention time.Duration
	cancelGrace      time.Duration
	now              func() time.Time
	log              *slog.Logger
	// governed is set when Orka executes the Agent's tools and serves its model.
	governed *governedSettings
}

type governedSettings struct {
	UpstreamBaseURL, UpstreamModel, UpstreamToken string
}

type active struct {
	prompt string
	cancel context.CancelFunc
}

type lifecycleBroker struct {
	cfg    settings
	remote remote
	store  *store

	mu            sync.Mutex
	ledger        *ledger
	storageError  error
	active        map[string]active
	workers       map[string]bool
	nextReconcile map[string]time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Turn tokens authorize relayed calls; they live only in memory.
	turnMu      sync.Mutex
	turns       map[string]*turnBinding
	relaySlots  chan struct{}
	relayClient *http.Client
}

func newLifecycleBroker(ctx context.Context, cfg settings, r remote) (*lifecycleBroker, error) {
	if r == nil || len(cfg.bearer) < 32 || !config.DigestValid(cfg.configDigest) || cfg.model == "" || cfg.expectedRevision == "" || cfg.userIDPrefix == "" {
		return nil, errInvalid
	}
	if cfg.operationTimeout <= 0 {
		cfg.operationTimeout = 45 * time.Second
	}
	if cfg.keepalive <= 0 {
		cfg.keepalive = 15 * time.Second
	}
	if cfg.cleanupDeadline <= 0 {
		cfg.cleanupDeadline = defaultCleanupDeadline
	}
	if cfg.retiredRetention <= 0 {
		cfg.retiredRetention = defaultRetiredRetention
	}
	if cfg.cancelGrace <= 0 {
		cfg.cancelGrace = cancelAdmissionGrace
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.log == nil {
		cfg.log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	s, l, err := openStore(cfg.stateDir, cfg.configDigest)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &lifecycleBroker{cfg: cfg, remote: r, store: s, ledger: l, active: map[string]active{},
		workers: map[string]bool{}, nextReconcile: map[string]time.Time{}, ctx: ctx, cancel: cancel,
		turns: map[string]*turnBinding{}, relaySlots: make(chan struct{}, maxRelayInflight), relayClient: &http.Client{
			// Never follow a redirect with Orka's credential attached.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}
	// A restarted broker never resumes an old request. Recovery closes each
	// unsettled prompt and obtains kagent evidence before settling it.
	now := cfg.now()
	b.mu.Lock()
	err = b.commitLocked(false, func(next *ledger) error {
		if next.Identity == "" {
			next.Identity = newLedgerIdentity()
		}
		for _, s := range next.Sessions {
			if s.Retired && s.RetiredAt.IsZero() {
				s.RetiredAt = now
			}
			for _, p := range s.Prompts {
				if !p.Settled {
					p.startClosing(now)
				}
			}
		}
		return nil
	})
	b.mu.Unlock()
	if err != nil {
		cancel()
		s.close()
		return nil, err
	}
	b.wg.Add(1)
	go b.sweep()
	return b, nil
}

func (b *lifecycleBroker) close() {
	b.cancel()
	b.mu.Lock()
	for _, a := range b.active {
		a.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
	b.store.close()
}

// commitLocked applies change to a copy of the ledger and persists it before
// publishing. Ordinary growth stops below the hard limit so cleanup receipts
// for already-admitted owners always fit.
func (b *lifecycleBroker) commitLocked(ordinary bool, change func(*ledger) error) error {
	if b.storageError != nil {
		return b.storageError
	}
	data, err := json.Marshal(b.ledger)
	var next ledger
	if err != nil || json.Unmarshal(data, &next) != nil {
		return b.failStorageLocked()
	}
	if err := change(&next); err != nil {
		return err
	}
	nextData, err := json.Marshal(&next)
	if err != nil {
		return b.failStorageLocked()
	}
	limit := maxLedgerBytes
	if ordinary {
		limit = ordinaryLedgerBytes
	}
	if len(nextData) > limit {
		return errCapacity
	}
	if err := b.store.save(nextData); err != nil {
		return b.failStorageLocked()
	}
	b.ledger = &next
	return nil
}

func (b *lifecycleBroker) failStorageLocked() error {
	b.storageError = errStorage
	// Failed storage cannot publish settlement or retirement; stop live work.
	b.cancel()
	for _, a := range b.active {
		a.cancel()
	}
	return b.storageError
}

func (b *lifecycleBroker) identity(o owner) (userID, requestID string) {
	digest := config.JSONDigest(o)[len("sha256:"):]
	return b.cfg.userIDPrefix + "-" + digest[:40], "orka-" + digest[:48]
}

func (b *lifecycleBroker) ensureSession(next *ledger, c brokerContext) (*session, error) {
	key := config.JSONDigest(c.Owner)
	if s := next.Sessions[key]; s != nil {
		return s, nil
	}
	if len(next.Sessions) >= maxSessions {
		return nil, errCapacity
	}
	userID, requestID := b.identity(c.Owner)
	// A session under a retired boot is born retiring: it admits nothing.
	s := &session{Owner: c.Owner, UserID: userID, RequestID: requestID, CreateState: createNone,
		Retiring: next.sealed(c.Owner), Prompts: map[string]*prompt{}, Operations: map[string]string{}}
	next.Sessions[key] = s
	return s, nil
}

// stalled reports a quarantined session whose cleanup is still unproven.
func (s *session) stalled() bool {
	if s == nil || s.QuarantinedAt.IsZero() {
		return false
	}
	for _, p := range s.Prompts {
		if !p.Settled {
			return true
		}
	}
	return false
}

// startClosing begins cleanup. The first close starts the cleanup deadline;
// a ledger written before deadlines existed starts it on recovery.
func (p *prompt) startClosing(now time.Time) {
	if !p.Closing || (!p.Settled && p.ClosingSince.IsZero()) {
		p.Closing, p.ClosingSince = true, now
	}
}

func ensurePrompt(s *session, c brokerContext) (*prompt, error) {
	key := c.promptKey()
	if p := s.Prompts[key]; p != nil {
		return p, nil
	}
	if s.Retiring || s.Retired || !s.QuarantinedAt.IsZero() {
		return nil, errClosed
	}
	for _, p := range s.Prompts {
		if p.Identity.PromptID == c.PromptID || (p.Identity.TaskUID == c.TaskUID && p.Identity.TaskAttempt == c.TaskAttempt) {
			return nil, errConflict
		}
	}
	if current := s.Prompts[s.CurrentPrompt]; current != nil && !current.Settled {
		return nil, errConflict
	}
	if len(s.Prompts) >= maxPromptsPerSession {
		return nil, errCapacity
	}
	expires, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
	if err != nil {
		return nil, errInvalid
	}
	p := &prompt{Identity: c, LeaseGeneration: c.LeaseGeneration, LeaseExpiresAt: expires, Invocations: map[uint64]*invocation{}}
	s.Prompts[key] = p
	s.CurrentPrompt = key
	return p, nil
}

func recordOperation(s *session, path string, c brokerContext) (bool, error) {
	digest := operationDigest(path, c)
	if previous, ok := s.Operations[c.OperationID]; ok {
		if previous != digest {
			return true, errConflict
		}
		return true, nil
	}
	limit := maxOperations
	switch path {
	case settlePath:
		limit = settleOperationLimit
	case retirePath:
		limit = retireOperationLimit
	}
	if len(s.Operations) >= limit {
		return false, errCapacity
	}
	s.Operations[c.OperationID] = digest
	return false, nil
}

func (b *lifecycleBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		b.mu.Lock()
		healthy := b.storageError == nil && b.ctx.Err() == nil
		b.mu.Unlock()
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+b.cfg.bearer)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	path := r.URL.Path
	if path == identityPath || path == retireBootPath {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
		if err != nil || len(body) > maxRequestBytes || r.URL.RawQuery != "" || r.URL.RawPath != "" ||
			r.Header.Get("Content-Encoding") != "" || len(r.Header.Values(brokerContextHeader)) != 0 ||
			(path == identityPath && (r.Method != http.MethodGet || len(body) != 0)) || (path == retireBootPath && r.Method != http.MethodPost) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.serveRecovery(w, path, body)
		return
	}
	validPath := path == responsesPath || path == renewPath || path == settlePath || path == retirePath || path == statusPath
	if !validPath || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" ||
		(path == statusPath && r.Method != http.MethodGet) || (path != statusPath && r.Method != http.MethodPost) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil || len(body) > maxRequestBytes ||
		(path == statusPath && len(body) != 0) || (path != responsesPath && path != statusPath && string(body) != "{}") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c, contextDigest, err := parseContext(r, body, b.cfg.configDigest, b.cfg.now())
	if err != nil {
		writeError(w, err)
		return
	}
	if path == responsesPath {
		b.serveResponses(w, r, c, body)
		return
	}
	if path != statusPath {
		if err := b.control(path, c); err != nil {
			writeError(w, err)
			return
		}
	}
	if path == settlePath || path == retirePath {
		b.startReconcile(config.JSONDigest(c.Owner), true)
	}
	b.mu.Lock()
	response := b.controlResponseLocked(c, contextDigest)
	storageErr := b.storageError
	b.mu.Unlock()
	if storageErr != nil {
		writeError(w, storageErr)
		return
	}
	status := http.StatusOK
	if (path == settlePath && !response.SettlementProven) || (path == retirePath && !response.RetirementProven) {
		status = http.StatusConflict
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, errInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, errConflict), errors.Is(err, errPending):
		status = http.StatusConflict
	case errors.Is(err, errClosed):
		status = http.StatusGone
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"kagent_broker_request_failed"}`))
}

func (b *lifecycleBroker) control(path string, c brokerContext) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := config.JSONDigest(c.Owner)
	existing := b.ledger.Sessions[key]
	ordinary := path == renewPath || existing == nil || (path == settlePath && existing.Prompts[c.promptKey()] == nil)
	err := b.commitLocked(ordinary, func(next *ledger) error {
		s, err := b.ensureSession(next, c)
		if err != nil {
			return err
		}
		duplicate, err := recordOperation(s, path, c)
		if err != nil {
			return err
		}
		now := b.cfg.now()
		if path == retirePath {
			s.Retiring = true
			for _, p := range s.Prompts {
				p.startClosing(now)
			}
			return nil
		}
		if (s.Retiring || s.Retired) && path == renewPath {
			return errClosed
		}
		p, err := ensurePrompt(s, c)
		if err != nil {
			return err
		}
		if path == settlePath {
			p.startClosing(now)
			return nil
		}
		if duplicate {
			return nil
		}
		if p.Closing || p.Settled {
			return errClosed
		}
		expires, _ := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		// A renewal may establish the prompt before inference; its generation
		// is already authoritative and need not start at one.
		if p.Identity.OperationID == c.OperationID && p.LastSequence == 0 && p.LeaseGeneration == c.LeaseGeneration {
			return nil
		}
		if c.LeaseGeneration != p.LeaseGeneration+1 || !p.LeaseExpiresAt.After(now) || expires.Before(p.LeaseExpiresAt) {
			return errConflict
		}
		p.LeaseGeneration, p.LeaseExpiresAt = c.LeaseGeneration, expires
		return nil
	})
	if err == nil && (path == settlePath || path == retirePath) {
		if a, ok := b.active[key]; ok && (path == retirePath || a.prompt == c.promptKey()) {
			a.cancel()
		}
	}
	return err
}

func (b *lifecycleBroker) controlResponseLocked(c brokerContext, contextDigest string) controlResponse {
	key := config.JSONDigest(c.Owner)
	response := controlResponse{Protocol: brokerProtocol, OwnerDigest: key, OperationID: c.OperationID,
		ContextSHA256: contextDigest, State: "open"}
	s := b.ledger.Sessions[key]
	if s == nil {
		return response
	}
	response.CreatePending = s.CreateState == createIntent
	response.RemoteSessionCreated = s.CreateState == createKnown || s.CreateState == createDeleted
	response.RetirementProven = s.Retired
	if s.Retiring {
		response.State = "retiring"
	}
	if s.Retired {
		response.State = stateRetired
		response.SettlementProven = true
		response.ProofDigest = s.ProofDigest
	}
	a, running := b.active[key]
	for promptKey, p := range s.Prompts {
		if c.PromptID != "" && promptKey != c.promptKey() {
			continue
		}
		if c.PromptID != "" {
			response.LeaseGeneration = p.LeaseGeneration
			response.LeaseExpiresAt = p.LeaseExpiresAt.UTC().Format(time.RFC3339Nano)
			response.SettlementProven = p.Settled
			if p.Closing {
				response.State = "settling"
			}
			if p.Settled {
				response.State = stateSettled
				response.ProofDigest = p.ProofDigest
			}
		}
		for _, inv := range p.Invocations {
			switch inv.State {
			case invocationIntent:
				if running && a.prompt == promptKey {
					response.ActiveInvocations++
				} else {
					response.AmbiguousInvocations++
				}
			case invocationReserved, invocationAccepted:
				if !p.Settled {
					response.ActiveInvocations++
				}
			}
		}
	}
	if response.CreatePending || response.AmbiguousInvocations > 0 {
		response.State = "blocked"
		response.SettlementProven = false
		response.RetirementProven = false
		response.ProofDigest = ""
	}
	// Orka accepts only settled or retired, so quarantine stays unproven.
	if !s.QuarantinedAt.IsZero() && response.State != stateSettled && response.State != stateRetired {
		response.State = "quarantined"
	}
	return response
}

func (b *lifecycleBroker) sweep() {
	defer b.wg.Done()
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
		}
		now := b.cfg.now()
		b.mu.Lock()
		b.expireLocked(now)
		keys := make([]string, 0)
		for key, s := range b.ledger.Sessions {
			needs := (s.Retiring && !s.Retired) || s.CreateState == createIntent
			for promptKey, p := range s.Prompts {
				if p.Closing && !p.Settled {
					needs = true
					if a, ok := b.active[key]; ok && a.prompt == promptKey {
						a.cancel()
					}
				}
			}
			if needs {
				keys = append(keys, key)
			}
		}
		b.mu.Unlock()
		for _, key := range keys {
			b.startReconcile(key, false)
		}
	}
}

// expireLocked closes prompts whose lease ended, quarantines sessions whose
// cleanup outlived its deadline, and prunes retired records past retention.
func (b *lifecycleBroker) expireLocked(now time.Time) {
	var expired, quarantine, prune []string
	for key, s := range b.ledger.Sessions {
		if s.Retired && !s.RetiredAt.IsZero() && now.Sub(s.RetiredAt) >= b.cfg.retiredRetention {
			prune = append(prune, key)
		}
		lease, stuck := false, false
		for _, p := range s.Prompts {
			lease = lease || (!p.Closing && !p.LeaseExpiresAt.After(now))
			stuck = stuck || (p.Closing && !p.Settled && !p.ClosingSince.IsZero() && now.Sub(p.ClosingSince) >= b.cfg.cleanupDeadline)
		}
		if lease {
			expired = append(expired, key)
		}
		if stuck && s.QuarantinedAt.IsZero() {
			quarantine = append(quarantine, key)
		}
	}
	if len(expired)+len(quarantine)+len(prune) == 0 {
		return
	}
	if b.commitLocked(false, func(next *ledger) error {
		for _, key := range expired {
			for _, p := range next.Sessions[key].Prompts {
				if !p.Closing && !p.LeaseExpiresAt.After(now) {
					p.startClosing(now)
				}
			}
		}
		for _, key := range quarantine {
			next.Sessions[key].QuarantinedAt = now
		}
		for _, key := range prune {
			delete(next.Sessions, key)
		}
		return nil
	}) != nil {
		return
	}
	for _, key := range prune {
		delete(b.nextReconcile, key)
	}
	for _, key := range quarantine {
		s := b.ledger.Sessions[key]
		tasks := []string{}
		for _, p := range s.Prompts {
			for _, inv := range p.Invocations {
				if !p.Settled && inv.TaskID != "" {
					tasks = append(tasks, inv.TaskID)
				}
			}
		}
		// Identifiers only: the runbook needs them to find the kagent work.
		b.cfg.log.Error("kagent session quarantined: cleanup did not prove settlement before its deadline",
			"owner_digest", key, "kagent_user", s.UserID, "kagent_session", s.RemoteID, "kagent_tasks", tasks,
			"deadline", b.cfg.cleanupDeadline.String())
	}
}

func (b *lifecycleBroker) startReconcile(key string, force bool) {
	b.mu.Lock()
	// Orka retries cleanup quickly; a stalled session is polled slowly.
	stalled := b.ledger.Sessions[key].stalled()
	if b.ctx.Err() != nil || b.storageError != nil || b.workers[key] || ((!force || stalled) && b.cfg.now().Before(b.nextReconcile[key])) {
		b.mu.Unlock()
		return
	}
	b.workers[key] = true
	b.wg.Add(1)
	b.mu.Unlock()
	go func() {
		defer b.wg.Done()
		ctx, cancel := context.WithTimeout(b.ctx, b.cfg.operationTimeout)
		defer cancel()
		b.reconcile(ctx, key)
		b.mu.Lock()
		delete(b.workers, key)
		backoff := reconcileBackoff
		if b.ledger.Sessions[key].stalled() {
			backoff = quarantineBackoff
		}
		b.nextReconcile[key] = b.cfg.now().Add(backoff)
		b.mu.Unlock()
	}()
}
