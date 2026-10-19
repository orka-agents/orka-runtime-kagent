package broker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

const (
	maxOutputBytes   = 4 << 20
	textChunkBytes   = 16 << 10
	maxToolEvents    = 256
	notAcceptedRetry = 100 * time.Millisecond
	notAcceptedLimit = 30 * time.Second
	terminalPoll     = 500 * time.Millisecond
	// podCheckEvery is how often a kagent v0 turn checks that the pod
	// running it still exists.
	podCheckEvery    = 5 * time.Second
	waitingCancelMax = 30 * time.Second
	// A cancelled turn keeps its send open this long so kagent can name the
	// task it admitted; settlement then cancels that task by ID instead of
	// waiting out an ambiguous dispatch.
	cancelAdmissionGrace = 10 * time.Second
)

var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

// responseRequest is the only request shape the kagent child sends.
type responseRequest struct {
	Model  string `json:"model"`
	Input  string `json:"input"`
	Stream bool   `json:"stream"`
	Store  bool   `json:"store"`
	// MCP is the Orka session MCP server, sent only in governed mode.
	MCP *relayMCP `json:"mcp,omitempty"`
}

// Failure codes are fixed strings; kagent error text is never relayed.
const (
	failTurn          = "kagent_turn_failed"
	failInputRequired = "kagent_input_required"
	failUnknown       = "kagent_outcome_unknown"
	failRevision      = "kagent_revision_mismatch"
	failSessionGone   = "kagent_session_gone"
	failUnverified    = "kagent_build_unverified"
	failIsolation     = "kagent_isolation_broken"
)

func (b *lifecycleBroker) serveResponses(w http.ResponseWriter, r *http.Request, c brokerContext, raw []byte) {
	var request responseRequest
	if strictjson.Decode(raw, &request, true) != nil || request.Model != b.cfg.model || !request.Stream || !request.Store ||
		request.Input == "" || !utf8.ValidString(request.Input) || (b.cfg.governed != nil) != (request.MCP != nil) ||
		(request.MCP != nil && !request.MCP.valid()) {
		writeError(w, errInvalid)
		return
	}
	key, promptKey := config.JSONDigest(c.Owner), c.promptKey()
	runCtx, cancel := context.WithCancel(b.ctx)
	defer cancel()
	if err := b.reserve(c, cancel); err != nil {
		writeError(w, err)
		return
	}
	defer b.wg.Done()
	stopDisconnect := context.AfterFunc(r.Context(), func() { b.closePrompt(key, promptKey); cancel() })
	defer stopDisconnect()

	if err := b.admit(runCtx); err != nil {
		b.finish(key, promptKey, c.InvocationSequence, nil)
		switch {
		case errors.Is(err, errUnverifiedBuild):
			writeFailedJSON(w, failUnverified)
		case errors.Is(err, errIsolation):
			writeFailedJSON(w, failIsolation)
		default:
			writeError(w, errRemote)
		}
		return
	}
	s, err := b.ensureRemote(runCtx, key)
	if err != nil {
		b.finish(key, promptKey, c.InvocationSequence, nil)
		switch {
		case errors.Is(err, errConflict):
			writeFailedJSON(w, failRevision)
		case errors.Is(err, errGone):
			writeFailedJSON(w, failSessionGone)
		default:
			writeError(w, err)
		}
		return
	}
	messageID := "orka-" + config.JSONDigest(struct {
		Owner, Prompt string
		Sequence      uint64
	}{key, promptKey, c.InvocationSequence})[len("sha256:"):][:48]
	if err := b.markIntent(key, promptKey, c.InvocationSequence, messageID); err != nil {
		b.finish(key, promptKey, c.InvocationSequence, nil)
		writeError(w, err)
		return
	}

	stream := newSSE(w)
	defer stream.release()
	responseID := "kr_" + uuid.NewString()
	if stream.event("response.created", map[string]any{"type": "response.created", "response": map[string]string{"id": responseID, "status": "in_progress"}}) != nil {
		b.finish(key, promptKey, c.InvocationSequence, nil)
		return
	}
	authorization, turn := "", (*turnBinding)(nil)
	if request.MCP != nil {
		token, digest, err := newTurnToken()
		if err != nil {
			b.finish(key, promptKey, c.InvocationSequence, nil)
			return
		}
		turn = newTurnBinding(promptKey, s.UserID, runCtx, *request.MCP)
		b.bindTurn(digest, turn)
		defer b.unbindTurn(digest)
		authorization = "Bearer " + token
	}
	stopKeepalive := stream.keepalive(runCtx, b.cfg.keepalive)
	outcome := b.runTurn(runCtx, s, key, promptKey, c.InvocationSequence, messageID, request.Input, authorization, stream)
	stopKeepalive()
	// The token dies with the turn, before its result is reported. Ending the
	// turn also ends its relay calls; wait for them so their usage counts.
	cancel()
	if turn != nil {
		turn.drain(relayDrainLimit)
	}
	b.finish(key, promptKey, c.InvocationSequence, &outcome.invocation)
	if outcome.failure != "" {
		_ = stream.event("response.failed", map[string]any{"type": "response.failed", "response": map[string]any{
			"id": responseID, "status": "failed", "error": map[string]string{"code": outcome.failure, "message": failureMessage(outcome.failure)},
		}})
		return
	}
	for text := outcome.text; text != ""; {
		n := min(len(text), textChunkBytes)
		for n < len(text) && !utf8.RuneStart(text[n]) {
			n--
		}
		if stream.event("response.output_text.delta", map[string]string{"type": "response.output_text.delta", "delta": text[:n]}) != nil {
			return
		}
		text = text[n:]
	}
	completed := map[string]any{"id": responseID, "status": "completed"}
	// A governed turn's usage is what the relay metered. An observed turn's
	// model calls bypass Orka, so its usage is what kagent reported.
	usage := outcome.kagentUsage
	if turn != nil {
		usage = turn.totalUsage()
	}
	if usage.reported {
		reported := map[string]any{"input_tokens": usage.Input, "output_tokens": usage.Output}
		if usage.cached {
			reported["input_tokens_details"] = map[string]uint64{"cached_tokens": usage.Cached}
		}
		completed["usage"] = reported
	}
	_ = stream.event("response.completed", map[string]any{"type": "response.completed", "response": completed})
}

type turnOutcome struct {
	invocation invocation
	text       string
	failure    string
	// kagentUsage totals the token usage kagent reported on the turn's events.
	kagentUsage tokenUsage
}

// runTurn sends the message once (retrying only kagent's explicit
// not-accepted proof), relays tool activity, and resolves a terminal task.
func (b *lifecycleBroker) runTurn(ctx context.Context, s *session, key, promptKey string, sequence uint64, messageID, text, authorization string, stream *sse) turnOutcome {
	out := turnOutcome{invocation: invocation{State: invocationIntent, MessageID: messageID}}
	tools := newToolRelay(stream)
	var taskID a2atype.TaskID
	var state a2atype.TaskState
	// sendPod is the kagent v0 pod the client confirmed served the send.
	var sendPod string
	// kagent v1 reports usage on artifacts, which several events may repeat.
	countedArtifacts := map[a2atype.ArtifactID]bool{}
	streamCtx, stopStream := context.WithCancel(b.ctx)
	defer stopStream()
	stopGrace := context.AfterFunc(ctx, func() { time.AfterFunc(b.cfg.cancelGrace, stopStream) })
	defer stopGrace()
	deadline := b.cfg.now().Add(notAcceptedLimit)
	for {
		if ctx.Err() != nil {
			// Cancelled before this attempt was sent: nothing new reached kagent.
			if taskID == "" {
				out.invocation.State = invocationRejected
			}
			out.failure = failUnknown
			return out
		}
		// kagent starts its dispatch fence when it accepts each attempt, so
		// the fence is measured from a timestamp persisted just before it.
		if b.stampAttempt(key, promptKey, sequence) != nil {
			out.failure = failUnknown
			return out
		}
		// kagent v0's client confirms the pinned pod served the send before it
		// yields the first event, and marks an error it confirmed.
		notAccepted, pinLost, bound := false, false, !b.cfg.kagentV0
		for event, err := range b.remote.Stream(streamCtx, s.UserID, s.RemoteID, messageID, text, authorization) {
			if err != nil {
				notAccepted = taskID == "" && kagent.NotAccepted(err)
				var lost *kagent.PinLostError
				if pinLost = errors.As(err, &lost); pinLost {
					// Record that the send's pod is unknown, with any task kagent
					// admitted, in one commit, before anything could read the
					// task as evidence.
					taskID = cmp.Or(taskID, lost.TaskID)
					b.markUnbound(key, promptKey, sequence, string(taskID))
				} else if !bound && errors.Is(err, kagent.ErrPinHeld) {
					sendPod = b.sendPod(messageID)
					b.markSent(key, promptKey, sequence, "")
				}
				break
			}
			if usage, ok := kagent.EventUsage(event); ok {
				out.kagentUsage.add(usage.Input, usage.Output, usage.Cached, usage.HasCached)
			}
			for id, usage := range kagent.ArtifactUsage(event) {
				if !countedArtifacts[id] {
					countedArtifacts[id] = true
					out.kagentUsage.add(usage.Input, usage.Output, usage.Cached, usage.HasCached)
				}
			}
			if id := event.TaskInfo().TaskID; !bound || (id != "" && taskID == "") {
				if !bound {
					sendPod = b.sendPod(messageID)
				}
				bound, taskID = true, cmp.Or(taskID, id)
				b.markSent(key, promptKey, sequence, string(id))
			}
			switch e := event.(type) {
			case *a2atype.TaskArtifactUpdateEvent:
				if e.Artifact != nil {
					tools.relay(e.Artifact.Parts)
				}
			case *a2atype.TaskStatusUpdateEvent:
				state = e.Status.State
				if e.Status.Message != nil {
					tools.relay(e.Status.Message.Parts)
				}
			case *a2atype.Task:
				state = e.Status.State
			}
			// Once a cancelled turn knows its task, settlement takes over.
			if state.Terminal() || waiting(state) || (taskID != "" && ctx.Err() != nil) {
				break
			}
		}
		if notAccepted && b.cfg.now().Before(deadline) && sleepCtx(ctx, notAcceptedRetry) {
			continue
		}
		if notAccepted {
			out.invocation.State = invocationRejected
			out.failure = failTurn
			return out
		}
		if pinLost {
			// The pinned pod was replaced while the send was in flight, so the
			// turn may run on another config, on a pod the broker cannot name.
			// A cancel would reach whichever pod is current, so it proves
			// nothing; settlement fences the session instead.
			out.invocation.Unbound, out.failure = true, failRevision
			if taskID != "" {
				out.invocation.State, out.invocation.TaskID = invocationAccepted, string(taskID)
			}
			return out
		}
		break
	}
	if taskID == "" {
		// The send outcome is unknown. Keep intent; reconciliation proves it.
		out.failure = failUnknown
		return out
	}
	out.invocation.State, out.invocation.TaskID = invocationAccepted, string(taskID)
	if waiting(state) {
		// Wrapped agents cannot answer kagent approval or input requests.
		cancelCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), waitingCancelMax)
		defer stop()
		if task, err := b.remote.CancelTask(cancelCtx, s.UserID, taskID); err == nil && task != nil && task.Status.State.Terminal() {
			out.invocation.State, out.invocation.Terminal = invocationCompleted, string(task.Status.State)
		}
		out.failure = failInputRequired
		return out
	}
	task, err := b.awaitTerminal(ctx, s.UserID, taskID, sendPod)
	if err != nil {
		out.failure = failUnknown
		return out
	}
	// The terminal task may hold artifacts the stream did not carry.
	for id, usage := range kagent.ArtifactUsage(task) {
		if !countedArtifacts[id] {
			countedArtifacts[id] = true
			out.kagentUsage.add(usage.Input, usage.Output, usage.Cached, usage.HasCached)
		}
	}
	out.invocation.State, out.invocation.Terminal = invocationCompleted, string(task.Status.State)
	if task.Status.State != a2atype.TaskStateCompleted {
		out.failure = failTurn
		return out
	}
	text, ok := taskText(task)
	if !ok {
		out.failure = failTurn
		return out
	}
	out.text = text
	return out
}

func waiting(state a2atype.TaskState) bool {
	return state == a2atype.TaskStateInputRequired || state == a2atype.TaskStateAuthRequired
}

// awaitTerminal reads committed task state until it is terminal. Gateway
// stream completion is observed, not trusted, before reading the result.
//
// kagent v0 never finishes a task whose pod died: it stays working. Once
// every pod that existed at the send is gone, nothing can run it, so the turn
// ends and settlement cancels it.
func (b *lifecycleBroker) awaitTerminal(ctx context.Context, userID string, taskID a2atype.TaskID, sendPod string) (*a2atype.Task, error) {
	nextPodCheck := b.cfg.now().Add(podCheckEvery)
	for {
		task, err := b.remote.GetTask(ctx, userID, taskID)
		if err == nil && task != nil && task.Status.State.Terminal() {
			return task, nil
		}
		if err == nil && task != nil && waiting(task.Status.State) {
			return nil, errRemote
		}
		if sendPod != "" && !b.cfg.now().Before(nextPodCheck) {
			nextPodCheck = b.cfg.now().Add(podCheckEvery)
			// A failed listing proves nothing; the next check tries again.
			if current, ok := b.agentPodsKnown(ctx); ok && !slices.Contains(current, sendPod) {
				return nil, errRemote
			}
		}
		if !sleepCtx(ctx, terminalPoll) {
			return nil, ctx.Err()
		}
	}
}

// sendPod names the kagent v0 pod confirmed to serve messageID's open send,
// or nothing when it is unknown.
func (b *lifecycleBroker) sendPod(messageID string) string {
	pods, ok := b.remote.(agentPods)
	if !b.cfg.kagentV0 || !ok {
		return ""
	}
	uid, _ := pods.SendPod(messageID)
	return uid
}

func (b *lifecycleBroker) agentPodsKnown(ctx context.Context) ([]string, bool) {
	pods, ok := b.remote.(agentPods)
	if !b.cfg.kagentV0 || !ok {
		return nil, false
	}
	current, err := pods.AgentPodUIDs(ctx)
	return current, err == nil
}

// taskText joins the task's text artifacts, skipping tool activity. kagent
// replaces the answer of an Agent with an output schema by a structured JSON
// part, which is reported as its JSON text, as kagent's own CLI does.
func taskText(task *a2atype.Task) (string, bool) {
	var builder strings.Builder
	for _, artifact := range task.Artifacts {
		if artifact == nil {
			continue
		}
		var run strings.Builder
		for _, part := range artifact.Parts {
			if _, tool := kagent.ToolActivity(part); tool || part == nil {
				continue
			}
			if apia2a.IsStructuredOutputPart(part) {
				text, err := apia2a.StructuredOutputJSON(part)
				if err != nil {
					return "", false
				}
				run.WriteString(text)
				continue
			}
			run.WriteString(part.Text())
		}
		if run.Len() == 0 {
			continue
		}
		if builder.Len() > 0 && !strings.HasSuffix(builder.String(), "\n") {
			builder.WriteString("\n")
		}
		builder.WriteString(run.String())
		if builder.Len() > maxOutputBytes {
			return "", false
		}
	}
	return builder.String(), utf8.ValidString(builder.String())
}

func failureMessage(code string) string {
	switch code {
	case failInputRequired:
		return "The kagent agent paused for human input; wrapped agents cannot answer kagent approval or input requests."
	case failUnknown:
		return "The kagent turn outcome could not be confirmed; cleanup will resolve it."
	case failRevision:
		return "The kagent Agent revision does not match the approved revision."
	case failSessionGone:
		return "The kagent session was deleted or expired; this Orka session cannot continue."
	case failUnverified:
		return "The kagent controller build is not one this adapter was verified against."
	case failIsolation:
		return "The kagent install lets the adapter reach a component that should admit only kagent's controller."
	default:
		return "The kagent turn did not complete."
	}
}

func writeFailedJSON(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// reserve admits exactly one invocation for a prompt's first round.
func (b *lifecycleBroker) reserve(c brokerContext, cancel context.CancelFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.storageError != nil {
		return b.storageError
	}
	if b.ctx.Err() != nil {
		return errClosed
	}
	key := config.JSONDigest(c.Owner)
	if _, running := b.active[key]; running {
		return errConflict
	}
	now := b.cfg.now()
	err := b.commitLocked(true, func(next *ledger) error {
		s, err := b.ensureSession(next, c)
		if err != nil {
			return err
		}
		if s.Retiring || s.Retired {
			return errClosed
		}
		duplicate, err := recordOperation(s, responsesPath, c)
		if err != nil {
			return err
		}
		if duplicate {
			return errConflict // No response replay and no inference retry.
		}
		p, err := ensurePrompt(s, c)
		if err != nil {
			return err
		}
		if p.Closing || p.Settled || !p.LeaseExpiresAt.After(now) {
			return errClosed
		}
		expires, _ := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
		if c.LeaseGeneration != p.LeaseGeneration || !expires.Equal(p.LeaseExpiresAt) || c.InvocationSequence <= p.LastSequence || p.LastSequence != 0 {
			// The kagent child sends one request per prompt.
			return errConflict
		}
		p.LastSequence = c.InvocationSequence
		p.Invocations[c.InvocationSequence] = &invocation{Sequence: c.InvocationSequence, OperationID: c.OperationID,
			BodyDigest: c.BodySHA256, State: invocationReserved}
		return nil
	})
	if err != nil {
		return err
	}
	b.active[key] = active{prompt: c.promptKey(), cancel: cancel}
	b.wg.Add(1)
	return nil
}

// ensureRemote creates (or recovers) the owner's kagent session, then checks
// it before every dispatch: kagent pins a session's revision at creation, but
// idle expiry or an operator can delete the session at any time.
func (b *lifecycleBroker) ensureRemote(ctx context.Context, key string) (*session, error) {
	b.mu.Lock()
	s := b.ledger.Sessions[key]
	if s == nil {
		b.mu.Unlock()
		return nil, errPending
	}
	userID, requestID, remoteID, state := s.UserID, s.RequestID, s.RemoteID, s.CreateState
	if s.RemoteGone {
		b.mu.Unlock()
		return nil, errGone
	}
	if state == createNone {
		if err := b.commitLocked(false, func(next *ledger) error {
			next.Sessions[key].CreateState = createIntent
			return nil
		}); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		state = createIntent
	}
	b.mu.Unlock()
	var observed kagent.Session
	var err error
	switch state {
	case createIntent:
		if !b.stampCreateAttempt(key) {
			return nil, errPending
		}
		observed, err = b.remote.CreateSession(ctx, userID, requestID)
	case createKnown:
		observed, err = b.remote.GetSession(ctx, userID, remoteID)
	default:
		return nil, errRemote
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	unchanged := errors.New("unchanged")
	commitErr := b.commitLocked(false, func(next *ledger) error {
		current := next.Sessions[key]
		if current.CreateState != state || current.RemoteID != remoteID {
			return unchanged
		}
		switch {
		case err == nil:
		case state == createIntent && errors.Is(err, kagent.ErrRejected):
			current.CreateState = createNone
			return nil
		case state == createIntent && errors.Is(err, kagent.ErrUnpinned) && observed.ID != "":
			// kagent holds the session, so own it for retirement; only a
			// later read that pins the revision lets a message through.
			current.CreateState, current.RemoteID = createKnown, observed.ID
			return nil
		case state == createKnown && errors.Is(err, kagent.ErrNotFound):
			current.RemoteGone = true
			return nil
		default:
			return unchanged
		}
		gone, mismatch := observed.Deleting, observed.Revision != b.cfg.expectedRevision
		if state == createIntent {
			current.CreateState, current.RemoteID, current.Revision = createKnown, observed.ID, observed.Revision
		} else if !gone && !mismatch {
			return unchanged
		}
		current.RemoteGone = current.RemoteGone || gone
		current.RevisionMismatch = current.RevisionMismatch || mismatch
		return nil
	})
	if commitErr != nil && !errors.Is(commitErr, unchanged) {
		return nil, commitErr
	}
	current := b.ledger.Sessions[key]
	switch {
	case current.RemoteGone:
		return nil, errGone
	case current.RevisionMismatch:
		return nil, errConflict
	case err != nil || current.CreateState != createKnown:
		return nil, errRemote
	}
	copy := *current
	return &copy, nil
}

func (b *lifecycleBroker) markIntent(key, promptKey string, sequence uint64, messageID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.now()
	return b.commitLocked(false, func(next *ledger) error {
		inv := invocationFor(next, key, promptKey, sequence)
		if inv == nil || inv.State != invocationReserved {
			return errPending
		}
		inv.State, inv.MessageID, inv.SentAt = invocationIntent, messageID, now
		return nil
	})
}

// stampAttempt records the start of a send attempt as the dispatch fence base.
// A kagent v0 send is unbound until its client confirms the pinned pod.
func (b *lifecycleBroker) stampAttempt(key, promptKey string, sequence uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.now()
	return b.commitLocked(false, func(next *ledger) error {
		inv := invocationFor(next, key, promptKey, sequence)
		if inv == nil || inv.State != invocationIntent {
			return errPending
		}
		inv.SentAt, inv.Unbound = now, b.cfg.kagentV0
		return nil
	})
}

// markSent records that the send reached kagent's pinned pod, and the task
// once kagent names it.
func (b *lifecycleBroker) markSent(key, promptKey string, sequence uint64, taskID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.commitLocked(false, func(next *ledger) error {
		inv := invocationFor(next, key, promptKey, sequence)
		if inv == nil || (inv.State != invocationIntent && inv.State != invocationAccepted) {
			return nil
		}
		inv.Unbound = false
		if inv.State == invocationIntent && taskID != "" {
			inv.State, inv.TaskID = invocationAccepted, taskID
		}
		return nil
	})
}

// finish records the request's final knowledge and releases ownership. A nil
// result leaves the durable invocation as it is.
func (b *lifecycleBroker) finish(key, promptKey string, sequence uint64, result *invocation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if result != nil {
		_ = b.commitLocked(false, func(next *ledger) error {
			inv := invocationFor(next, key, promptKey, sequence)
			if inv == nil {
				return nil
			}
			switch result.State {
			case invocationCompleted:
				inv.State, inv.TaskID, inv.Terminal = invocationCompleted, result.TaskID, result.Terminal
			case invocationAccepted:
				inv.State, inv.TaskID = invocationAccepted, result.TaskID
				inv.Unbound = inv.Unbound || result.Unbound
			case invocationRejected:
				if inv.State == invocationIntent {
					inv.State = invocationRejected
				}
			}
			return nil
		})
	}
	delete(b.active, key)
}

// markUnbound records an admitted task whose pod is unknown.
func (b *lifecycleBroker) markUnbound(key, promptKey string, sequence uint64, taskID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.commitLocked(false, func(next *ledger) error {
		if inv := invocationFor(next, key, promptKey, sequence); inv != nil {
			if inv.State == invocationIntent && taskID != "" {
				inv.State, inv.TaskID = invocationAccepted, taskID
			}
			inv.Unbound = true
		}
		return nil
	})
}

func (b *lifecycleBroker) closePrompt(key, promptKey string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.commitLocked(false, func(next *ledger) error {
		if s := next.Sessions[key]; s != nil {
			if p := s.Prompts[promptKey]; p != nil {
				// Start the cleanup deadline too, so a prompt whose cleanup never
				// proves out is quarantined even without a later settle call.
				p.startClosing(b.cfg.now())
			}
		}
		return nil
	})
}

func invocationFor(l *ledger, key, promptKey string, sequence uint64) *invocation {
	s := l.Sessions[key]
	if s == nil {
		return nil
	}
	p := s.Prompts[promptKey]
	if p == nil {
		return nil
	}
	return p.Invocations[sequence]
}

// toolRelay emits observed kagent tool activity: names and status only.
// toolRelay reports each kagent tool call to Orka once, however many events
// repeat it: kagent v0 mirrors tool parts in status messages, and v1 in
// artifacts.
type toolRelay struct {
	stream   *sse
	ids      map[string]string
	finished map[string]bool
}

func newToolRelay(stream *sse) *toolRelay {
	return &toolRelay{stream: stream, ids: map[string]string{}, finished: map[string]bool{}}
}

func (t *toolRelay) relay(parts []*a2atype.Part) {
	for _, part := range parts {
		activity, ok := kagent.ToolActivity(part)
		if !ok {
			continue
		}
		name := activity.Name
		if !toolNamePattern.MatchString(name) {
			name = "tool"
		}
		id, seen := t.ids[activity.ID]
		switch activity.Kind {
		case apia2a.ToolCallKind:
			if seen || len(t.ids) >= maxToolEvents {
				continue
			}
			id = "kt_" + uuid.NewString()
			t.ids[activity.ID] = id
			_ = t.stream.event("response.kagent_tool.started", map[string]any{"type": "response.kagent_tool.started", "tool": map[string]string{"id": id, "name": name}})
		case apia2a.ToolResultKind:
			if !seen || t.finished[activity.ID] {
				continue
			}
			t.finished[activity.ID] = true
			_ = t.stream.event("response.kagent_tool.finished", map[string]any{"type": "response.kagent_tool.finished", "tool": map[string]string{"id": id, "name": name, "status": "completed"}})
		}
	}
}

// sseWriteStall bounds one lifecycle stream write. It exceeds the keepalive
// interval, so an idle but healthy stream never trips it.
const sseWriteStall = 30 * time.Second

// sse serializes server-sent events and keepalive comments. Every write and
// flush gets a rolling deadline, so a supervisor that stops reading fails the
// stream instead of blocking the turn and broker shutdown behind it.
type sse struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	control *http.ResponseController
	started bool
	failed  bool
}

func newSSE(w http.ResponseWriter) *sse {
	return &sse{w: w, control: http.NewResponseController(w)}
}

// release clears the write deadline so a reused connection does not inherit it.
func (s *sse) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.control.SetWriteDeadline(time.Time{})
}

func (s *sse) start() {
	if s.started {
		return
	}
	s.started = true
	s.w.Header().Set("Content-Type", "text/event-stream")
	s.w.WriteHeader(http.StatusOK)
}

func (s *sse) write(frame []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return errClosed
	}
	_ = s.control.SetWriteDeadline(time.Now().Add(sseWriteStall))
	s.start()
	if _, err := s.w.Write(frame); err != nil {
		s.failed = true
		return err
	}
	if err := s.control.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.failed = true
		return err
	}
	return nil
}

func (s *sse) event(name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.write([]byte("event: " + name + "\ndata: " + string(data) + "\n\n"))
}

func (s *sse) keepalive(ctx context.Context, every time.Duration) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.write([]byte(": keepalive\n\n")) != nil {
					return
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}
