package broker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

// Settlement and retirement proof kinds recorded in proof digests.
const (
	proofNeverDispatched = "never-dispatched"
	proofNotAccepted     = "kagent-send-not-accepted"
	proofTaskTerminal    = "kagent-task-terminal"
	proofSessionDeleted  = "kagent-session-not-found"
	// kagent v0's proofs for an ambiguous send whose task cannot be found,
	// once its session is deleted: a governed turn's token is revoked, and
	// every Agent pod that could have run an observed turn is gone.
	proofTurnTokenRevoked = "kagent-v0-turn-token-revoked"
	proofPodsReplaced     = "kagent-v0-agent-pods-replaced"
	proofNeverCreated     = "never-created"
	proofDeleted          = "kagent-delete-get-not-found"
)

func (b *lifecycleBroker) snapshot(key string) (*session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.ledger.Sessions[key]
	_, running := b.active[key]
	if s == nil || s.Retired || running {
		return nil, false
	}
	data, err := json.Marshal(s)
	var copy session
	if err != nil || json.Unmarshal(data, &copy) != nil {
		return nil, false
	}
	return &copy, true
}

func (b *lifecycleBroker) reconcile(ctx context.Context, key string) {
	s, ok := b.snapshot(key)
	if !ok {
		return
	}
	if s.CreateState == createIntent {
		if !b.resolveCreate(ctx, key, s) {
			return
		}
		if s, ok = b.snapshot(key); !ok {
			return
		}
	}
	allSettled := true
	for promptKey, p := range s.Prompts {
		if p.Settled {
			continue
		}
		if !p.Closing {
			allSettled = false
			continue
		}
		if !b.settlePrompt(ctx, key, s, promptKey, p) {
			allSettled = false
		}
	}
	if s.Retiring && allSettled {
		b.retire(ctx, key, s)
	}
}

// resolveCreate retries the idempotent CreateSession for an unacknowledged
// creation. Only kagent's answer moves the session out of intent, except for
// a retiring session whose ID is known in advance (kagent v0): once no create
// can still commit, deleting that ID and reading it gone proves no session is
// left. That matters when the Agent is deleted, because v0 then refuses every
// create without saying whether an earlier one committed.
func (b *lifecycleBroker) resolveCreate(ctx context.Context, key string, s *session) bool {
	if id, known := b.remote.SessionIDForRequest(s.RequestID); known && s.Retiring && !s.CreateAttemptAt.IsZero() {
		if b.cfg.now().Sub(s.CreateAttemptAt) < v0CreateSettle {
			return false
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.commitLocked(false, func(next *ledger) error {
			current := next.Sessions[key]
			if current == nil || current.CreateState != createIntent || !current.CreateAttemptAt.Equal(s.CreateAttemptAt) {
				return errPending
			}
			// retire deletes the session by this ID and requires it gone.
			current.CreateState, current.RemoteID = createKnown, id
			return nil
		}) == nil
	}
	if !b.stampCreateAttempt(key) {
		return false
	}
	remoteSession, err := b.remote.CreateSession(ctx, s.UserID, s.RequestID)
	// An unpinned session is still kagent's, so it is owned and can be
	// retired; dispatch pins it again first.
	unpinned := errors.Is(err, kagent.ErrUnpinned) && remoteSession.ID != ""
	if err != nil && !unpinned && !errors.Is(err, kagent.ErrRejected) {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.commitLocked(false, func(next *ledger) error {
		current := next.Sessions[key]
		if current == nil || current.CreateState != createIntent {
			return errPending
		}
		switch {
		case unpinned:
			current.CreateState, current.RemoteID = createKnown, remoteSession.ID
			return nil
		case err != nil:
			// A definite refusal created nothing.
			current.CreateState = createNone
			return nil
		}
		current.CreateState, current.RemoteID, current.Revision = createKnown, remoteSession.ID, remoteSession.Revision
		current.RevisionMismatch = remoteSession.Revision != b.cfg.expectedRevision
		return nil
	}) == nil
}

// stampCreateAttempt records that a session create is about to be sent.
func (b *lifecycleBroker) stampCreateAttempt(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.commitLocked(false, func(next *ledger) error {
		current := next.Sessions[key]
		if current == nil || current.CreateState != createIntent {
			return errPending
		}
		current.CreateAttemptAt = b.cfg.now()
		return nil
	}) == nil
}

// settlePrompt proves that the prompt's kagent work can no longer change:
// nothing was dispatched, kagent refused it, or its task is terminal.
func (b *lifecycleBroker) settlePrompt(ctx context.Context, key string, s *session, promptKey string, p *prompt) bool {
	kinds := make([]string, 0, len(p.Invocations))
	updates := map[uint64]invocation{}
	for sequence, inv := range p.Invocations {
		resolved := *inv
		switch inv.State {
		case invocationReserved:
			kinds = append(kinds, proofNeverDispatched)
		case invocationRejected:
			kinds = append(kinds, proofNotAccepted)
		case invocationCompleted, invocationIntent, invocationAccepted:
			// A terminal task settles a v1 prompt. kagent v0 can record a
			// cancellation before its execution has unwound, so v0 reads the
			// task once more.
			if inv.State == invocationCompleted && !b.cfg.kagentV0 {
				kinds = append(kinds, proofTaskTerminal)
				continue
			}
			if s.CreateState != createKnown {
				return false
			}
			kind, ok := b.resolveInvocation(ctx, s, &resolved)
			updates[sequence] = resolved
			if !ok {
				b.applyInvocations(key, promptKey, updates, "", false)
				return false
			}
			kinds = append(kinds, kind)
		default:
			return false
		}
	}
	if len(kinds) == 0 {
		kinds = append(kinds, proofNeverDispatched)
	}
	proof := config.JSONDigest(struct {
		Owner, Prompt, Remote string
		Kinds                 []string
		At                    string
	}{key, promptKey, config.Digest([]byte(s.RemoteID)), kinds, b.cfg.now().UTC().Format(time.RFC3339Nano)})
	gone := slices.Contains(kinds, proofSessionDeleted) || slices.Contains(kinds, proofTurnTokenRevoked) ||
		slices.Contains(kinds, proofPodsReplaced)
	return b.applyInvocations(key, promptKey, updates, proof, gone)
}

// resolveInvocation turns an intent or accepted invocation into a terminal
// fact. It cancels unfinished tasks; it never resends a message. kagent
// reports a deleted session's tasks as unauthorized, not missing, so any failed
// task read falls back to asking whether the session itself is gone.
func (b *lifecycleBroker) resolveInvocation(ctx context.Context, s *session, inv *invocation) (string, bool) {
	if inv.Unbound && b.cfg.kagentV0 {
		return b.fenceV0(ctx, s, inv)
	}
	if inv.State == invocationIntent {
		// Absence is never proof: kagent's admission latency is unbounded.
		// Waiting out its dispatch fence gives an admitted send time to
		// surface; after that, deleting the session fences it for certain.
		patient := b.cfg.now().Sub(inv.SentAt) < dispatchFenceWindow
		task, err := b.remote.FindTaskByMessage(ctx, s.UserID, s.RemoteID, inv.MessageID)
		switch {
		case err == nil && task != nil:
		case b.cfg.kagentV0 && !inv.FencedAt.IsZero():
			// The session is already deleted, so it can no longer be listed.
			return b.fenceV0(ctx, s, inv)
		case !b.cfg.kagentV0 && b.sessionDeleted(ctx, s):
			inv.State = invocationSettled
			return proofSessionDeleted, true
		case patient:
			return "", false
		case b.cfg.kagentV0 && (errors.Is(err, kagent.ErrNotFound) || b.sessionDeleted(ctx, s)):
			// No task holds the message, or the session is gone and its tasks
			// can no longer be listed.
			return b.fenceV0(ctx, s, inv)
		case !errors.Is(err, kagent.ErrNotFound) && !errors.Is(err, kagent.ErrAmbiguous):
			return "", false
		case b.cfg.kagentV0:
			return "", false
		default:
			if err := b.remote.DeleteSession(ctx, s.UserID, s.RemoteID); err != nil && !errors.Is(err, kagent.ErrNotFound) {
				return "", false
			}
			if !b.sessionDeleted(ctx, s) {
				return "", false
			}
			inv.State = invocationSettled
			return proofSessionDeleted, true
		}
		inv.State, inv.TaskID = invocationAccepted, string(task.ID)
		if task.Status.State.Terminal() && !b.cfg.kagentV0 {
			inv.State, inv.Terminal = invocationCompleted, string(task.Status.State)
			return proofTaskTerminal, true
		}
	}
	task, err := b.remote.GetTask(ctx, s.UserID, a2atype.TaskID(inv.TaskID))
	if err == nil && task != nil && (!task.Status.State.Terminal() || (b.cfg.kagentV0 && task.Status.State == a2atype.TaskStateCanceled)) {
		// Waiting states (input-required, auth-required) are not terminal;
		// observer disconnection never stops kagent, so cancel explicitly.
		// kagent v0 records a cancellation before its execution has unwound,
		// so a canceled v0 task is cancelled again: the call returns only once
		// no execution of it is left. kagent v0 writes completed and failed
		// only as its executor's final statement, so those need no call;
		// a2a-go's own failed status, written when processing an event fails,
		// is followed by cancelling the execution's context, which aborts
		// kagent's in-flight model and MCP calls.
		task, err = b.remote.CancelTask(ctx, s.UserID, a2atype.TaskID(inv.TaskID))
	}
	if err != nil || task == nil || !task.Status.State.Terminal() {
		// v0 keeps a deleted session's tasks readable and running, so only
		// the task's own state can settle it there.
		if !b.cfg.kagentV0 && b.sessionDeleted(ctx, s) {
			inv.State = invocationSettled
			return proofSessionDeleted, true
		}
		return "", false
	}
	inv.State, inv.Terminal = invocationCompleted, string(task.Status.State)
	return proofTaskTerminal, true
}

// fenceV0 settles a kagent v0 send whose task cannot be found. A listing is
// only a snapshot: v0 may save a late-admitted turn's task after it. Deleting
// the session stops such a turn before its next tool, but not a model or tool
// call it already has in flight, and no timeout bounds that call once
// retries are counted.
//
// A governed turn needs nothing more: its token died with the turn, so a late
// execution reaches neither Orka's tools nor its model. An observed turn runs
// kagent's own tools. It settles once every Agent pod that existed when the
// session was deleted is gone: an execution lives in its pod's process, and a
// newer pod cannot run a turn whose session it can no longer load. Restarting
// the Agent's Deployment resolves it. This trusts that a pod object is removed
// only after its containers stop, which holds unless someone force-deletes it.
func (b *lifecycleBroker) fenceV0(ctx context.Context, s *session, inv *invocation) (string, bool) {
	if err := b.remote.DeleteSession(ctx, s.UserID, s.RemoteID); err != nil && !errors.Is(err, kagent.ErrNotFound) {
		return "", false
	}
	if !b.sessionDeleted(ctx, s) {
		return "", false
	}
	// An unbound send may have reached a pod on a config that never uses
	// the turn token, so only the pods' replacement proves it stopped.
	if b.cfg.governed != nil && !inv.Unbound {
		inv.State = invocationSettled
		return proofTurnTokenRevoked, true
	}
	pods, ok := b.remote.(agentPods)
	if !ok {
		return "", false
	}
	current, err := pods.AgentPodUIDs(ctx)
	if err != nil || len(current) > maxFencedPods {
		return "", false
	}
	if inv.FencedAt.IsZero() {
		inv.FencedAt, inv.FencedPods = b.cfg.now(), current
		return "", false
	}
	for _, uid := range current {
		if slices.Contains(inv.FencedPods, uid) {
			return "", false
		}
	}
	inv.State = invocationSettled
	return proofPodsReplaced, true
}

// sessionDeleted reports kagent's proof that the session no longer exists.
// Deletion tears down the session's work, so nothing in it can still change;
// this is how idle expiry, or an operator resolving a quarantine, settles.
func (b *lifecycleBroker) sessionDeleted(ctx context.Context, s *session) bool {
	_, err := b.remote.GetSession(ctx, s.UserID, s.RemoteID)
	return errors.Is(err, kagent.ErrNotFound)
}

// applyInvocations records resolved invocation facts and, when proof is set,
// settles the prompt; gone records that kagent deleted the session. It
// refuses if a live request has since taken ownership.
func (b *lifecycleBroker) applyInvocations(key, promptKey string, updates map[uint64]invocation, proof string, gone bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.commitLocked(false, func(next *ledger) error {
		if _, running := b.active[key]; running {
			return errPending
		}
		s := next.Sessions[key]
		if s == nil {
			return errPending
		}
		p := s.Prompts[promptKey]
		if p == nil || p.Settled || !p.Closing {
			return errPending
		}
		for sequence, resolved := range updates {
			current := p.Invocations[sequence]
			if current == nil || current.OperationID != resolved.OperationID {
				return errPending
			}
			value := resolved
			p.Invocations[sequence] = &value
		}
		if proof == "" {
			return nil
		}
		for _, inv := range p.Invocations {
			if inv.State == invocationIntent || inv.State == invocationAccepted {
				return errPending
			}
			if inv.State == invocationReserved {
				inv.State = invocationSettled
			}
		}
		p.Settled, p.ProofDigest = true, proof
		s.RemoteGone = s.RemoteGone || gone
		// Quarantine ends once cleanup has proven every prompt. A deleted
		// session stays unusable through RemoteGone.
		unproven := false
		for _, other := range s.Prompts {
			unproven = unproven || (other.Closing && !other.Settled)
		}
		if !unproven {
			s.QuarantinedAt = time.Time{}
		}
		return nil
	}) == nil && proof != ""
}

// retire deletes the kagent session and requires kagent to report it gone.
func (b *lifecycleBroker) retire(ctx context.Context, key string, s *session) {
	kind := proofNeverCreated
	switch s.CreateState {
	case createNone:
	case createKnown:
		if err := b.remote.DeleteSession(ctx, s.UserID, s.RemoteID); err != nil && !errors.Is(err, kagent.ErrNotFound) {
			return
		}
		if _, err := b.remote.GetSession(ctx, s.UserID, s.RemoteID); !errors.Is(err, kagent.ErrNotFound) {
			return
		}
		kind = proofDeleted
	default:
		return
	}
	proof := config.JSONDigest(struct{ Owner, Remote, Kind, At string }{
		key, config.Digest([]byte(s.RemoteID)), kind, b.cfg.now().UTC().Format(time.RFC3339Nano)})
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.commitLocked(false, func(next *ledger) error {
		current := next.Sessions[key]
		if _, running := b.active[key]; running || current == nil || !current.Retiring || current.Retired || current.CreateState != s.CreateState {
			return errPending
		}
		for _, p := range current.Prompts {
			if !p.Settled {
				return errPending
			}
		}
		if current.CreateState == createKnown {
			current.CreateState = createDeleted
		}
		current.Retired, current.RetiredAt, current.ProofDigest = true, b.cfg.now(), proof
		return nil
	})
}
