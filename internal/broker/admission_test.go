package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

// An unverified kagent build gets no new turn, and a failing check is not
// remembered: the next turn after an upgrade of the adapter is admitted.
func TestUnverifiedKagentBuildRefusesNewTurns(t *testing.T) {
	remote := newFakeRemote()
	remote.versionErr = fmt.Errorf("%w: commit 0123456", kagent.ErrUnverifiedBuild)
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	recorder := h.turn(o, p)
	if !strings.Contains(recorder.Body.String(), failUnverified) || remote.count("create ") != 0 || remote.count("stream ") != 0 {
		t.Fatalf("turn on an unverified build = %d %s calls %v", recorder.Code, recorder.Body.String(), remote.snapshotCalls())
	}
	if !strings.Contains(h.logs.String(), "refusing new turns") {
		t.Fatalf("the refusal was not logged: %s", h.logs.String())
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("a refused prompt did not settle")
	}
	remote.mu.Lock()
	remote.versionErr = nil
	remote.mu.Unlock()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	if code := h.turn(o, h.prompt("prompt-2")).Code; code != http.StatusOK {
		t.Fatalf("turn once the build is accepted = %d", code)
	}
}

// The broker refuses turns while it can connect to an address the install
// should admit only kagent's controller to. A refused or dropped connection
// is the isolation working.
func TestReachableIsolatedAddressRefusesNewTurns(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := closed.Addr().String()
	_ = closed.Close()

	remote := newFakeRemote()
	h := newHarness(t, remote)
	h.b.cfg.unreachable = []string{closedAddress, listener.Addr().String()}
	o, p := testOwner(1), h.prompt("prompt-1")
	recorder := h.turn(o, p)
	if !strings.Contains(recorder.Body.String(), failIsolation) || remote.count("create ") != 0 {
		t.Fatalf("turn with a reachable router = %d %s calls %v", recorder.Code, recorder.Body.String(), remote.snapshotCalls())
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("a refused prompt did not settle")
	}
	h.b.cfg.unreachable = []string{closedAddress}
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	p = h.prompt("prompt-2")
	if code := h.turn(o, p).Code; code != http.StatusOK {
		t.Fatalf("turn once isolated = %d", code)
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("a completed prompt did not settle")
	}
	// A passing check is reused within the recheck window.
	h.b.cfg.unreachable = []string{listener.Addr().String()}
	remote.streams = append(remote.streams, completedStream(remote, "task-2", "ok"))
	p = h.prompt("prompt-3")
	if code := h.turn(o, p).Code; code != http.StatusOK {
		t.Fatalf("turn within the recheck window = %d", code)
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("a completed prompt did not settle")
	}
	h.advance(admissionRecheck)
	if recorder := h.turn(o, h.prompt("prompt-4")); !strings.Contains(recorder.Body.String(), failIsolation) {
		t.Fatalf("turn after the recheck window = %d %s", recorder.Code, recorder.Body.String())
	}
}

// When the pinned kagent v0 pod is replaced while a send is in flight, the
// turn may run on another config, on a pod the broker cannot name. It fails
// as a revision mismatch, and settles only as a fenced send: a cancel would
// reach whichever pod is current, and the task's state proves nothing.
func TestLostPinFencesTheTurn(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
		remote.setTask(task, messageID)
		return []item{{err: &kagent.PinLostError{TaskID: "task-1"}}}
	})
	h := newHarness(t, remote)
	h.kagentV0 = true
	h.restart()
	o, p := testOwner(1), h.prompt("prompt-1")
	recorder := h.turn(o, p)
	if !strings.Contains(recorder.Body.String(), failRevision) || remote.count("cancel ") != 0 {
		t.Fatalf("turn that lost its pin = %s calls %v", recorder.Body.String(), remote.snapshotCalls())
	}
	h.b.mu.Lock()
	unbound := false
	for _, s := range h.b.ledger.Sessions {
		for _, prompt := range s.Prompts {
			for _, inv := range prompt.Invocations {
				unbound = unbound || inv.Unbound
			}
		}
	}
	h.b.mu.Unlock()
	if !unbound {
		t.Fatal("the lost pin was not recorded")
	}
	// The session is fenced; an observed send settles once every pod that
	// existed at the fence is gone.
	if _, response := h.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled while the fenced pods remain")
	}
	eventually(t, "fence", func() bool { return remote.count("delete ") == 1 })
	// The fence survives a restart.
	h.restart()
	if _, response := h.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled after a restart while the fenced pods remain")
	}
	remote.setPods("pod-b")
	if !h.control(settlePath, o, p).SettlementProven || remote.count("cancel ") != 0 {
		t.Fatalf("the fenced send did not settle without a cancel: %v", remote.snapshotCalls())
	}
}

// An observed turn's model calls bypass Orka, so its usage is what kagent
// reported, once per model call.
func TestObservedTurnReportsKagentUsage(t *testing.T) {
	remote := newFakeRemote()
	call := func(prompt, candidates float64, key string) *a2atype.TaskStatusUpdateEvent {
		return &a2atype.TaskStatusUpdateEvent{TaskID: "task-1", Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking},
			Metadata: map[string]any{key: map[string]any{"promptTokenCount": prompt, "candidatesTokenCount": candidates}}}
	}
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		events := completedStream(remote, "task-1", "ok")(ctx, contextID, messageID)
		partial := call(500, 500, "adk_usage_metadata")
		partial.Metadata["adk_partial"] = true
		return append([]item{{event: call(11, 7, "adk_usage_metadata")}, {event: partial}, {event: call(20, 3, "kagent.dev/a2a/usage")}}, events...)
	})
	h := newHarness(t, remote)
	recorder := h.turn(testOwner(1), h.prompt("prompt-1"))
	events := parseSSE(t, recorder.Body.String())
	usage, _ := events[len(events)-1].data["response"].(map[string]any)["usage"].(map[string]any)
	if usage["input_tokens"] != float64(31) || usage["output_tokens"] != float64(10) {
		data, _ := json.Marshal(usage)
		t.Fatalf("reported usage = %s", data)
	}
}

// kagent v0 creates a session under the broker's ID. When every create is
// refused, as after the Agent is deleted, retirement waits until no earlier
// create can still commit, then deletes that ID and requires it gone.
func TestRetiringV0SessionWithAnUnacknowledgedCreate(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	h := newHarness(t, remote)
	h.kagentV0 = true
	h.restart()
	remote.mu.Lock()
	remote.createRefusal = errors.New("kagent session create answered 400")
	remote.mu.Unlock()
	o, p := testOwner(1), h.prompt("prompt-1")
	if code := h.turn(o, p).Code; code == http.StatusOK {
		t.Fatal("a turn ran without a session")
	}
	// Settlement waits on the create, so Orka retires the session.
	if _, response := h.controlOnce(settlePath, o, p); response.SettlementProven || !response.CreatePending {
		t.Fatalf("settled with a create pending: %+v", response)
	}
	if code, response := h.controlOnce(retirePath, o, nil); code == http.StatusOK && response.RetirementProven {
		t.Fatal("retired before an unacknowledged create could no longer commit")
	}
	creates := remote.count("create ")
	time.Sleep(3 * sweepInterval)
	if remote.count("create ") != creates || remote.count("delete ") != 0 {
		t.Fatalf("a retiring session kept creating or deleted early: %v", remote.snapshotCalls())
	}
	h.advance(v0CreateSettle)
	if !h.control(retirePath, o, nil).RetirementProven || remote.count("delete ") != 1 || remote.count("get-session ") != 1 {
		t.Fatalf("the session was not retired by its ID: %v", remote.snapshotCalls())
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("the prompt did not settle once the session was retired")
	}
}

func TestClampOutputTokens(t *testing.T) {
	for _, test := range []struct {
		name, body, want string
		ok               bool
	}{
		{"unset", `{}`, `{"max_tokens":100}`, true},
		{"larger max_tokens", `{"max_tokens":4096}`, `{"max_tokens":100}`, true},
		{"smaller max_tokens", `{"max_tokens":7}`, `{"max_tokens":7}`, true},
		{"max_completion_tokens", `{"max_completion_tokens":500}`, `{"max_completion_tokens":100}`, true},
		{"both", `{"max_tokens":50,"max_completion_tokens":80}`, `{"max_completion_tokens":50}`, true},
		{"zero", `{"max_tokens":0}`, ``, false},
		{"string", `{"max_tokens":"9"}`, ``, false},
		{"fraction", `{"max_tokens":9.5}`, ``, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal([]byte(test.body), &payload); err != nil {
				t.Fatal(err)
			}
			if ok := clampOutputTokens(payload, 100); ok != test.ok {
				t.Fatalf("clamp = %v, want %v", ok, test.ok)
			}
			if !test.ok {
				return
			}
			if got, _ := json.Marshal(payload); string(got) != test.want {
				t.Fatalf("clamped = %s, want %s", got, test.want)
			}
		})
	}
}

// A check interrupted by the turn's cancellation is not remembered as a pass:
// its later probes fail without testing their addresses.
func TestInterruptedIsolationCheckIsNotAPass(t *testing.T) {
	remote := newFakeRemote()
	h := newHarness(t, remote)
	h.b.cfg.unreachable = []string{"router.ate-system.svc:8080"}
	ctx, cancel := context.WithCancel(context.Background())
	h.b.cfg.dial = func(context.Context, string, string) (net.Conn, error) {
		cancel()
		return nil, context.Canceled
	}
	if err := h.b.admit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted admission = %v", err)
	}
	h.b.admission.mu.Lock()
	cached := !h.b.admission.checkedAt.IsZero()
	h.b.admission.mu.Unlock()
	if cached {
		t.Fatal("an interrupted check was cached")
	}
}

// A governed send whose pod is unknown may have reached a pod whose config
// never uses the turn token, so revoking the token does not settle it.
func TestLostPinOfAGovernedSendWaitsForThePods(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, messageID)
		return []item{{err: &kagent.PinLostError{TaskID: "task-1"}}}
	})
	g := newGovernedHarness(t, remote)
	g.kagentV0 = true
	g.restart()
	o, p := testOwner(1), g.prompt("prompt-1")
	if recorder := g.governedTurn(o, p); !strings.Contains(recorder.Body.String(), failRevision) {
		t.Fatalf("governed turn that lost its pin = %s", recorder.Body.String())
	}
	if _, response := g.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled on the revoked turn token alone")
	}
	eventually(t, "fence", func() bool { return remote.count("delete ") == 1 })
	if _, response := g.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled while the fenced pods remain")
	}
	remote.setPods("pod-b")
	if !g.control(settlePath, o, p).SettlementProven || remote.count("cancel ") != 0 {
		t.Fatalf("the fenced send did not settle without a cancel: %v", remote.snapshotCalls())
	}
}

// A send that failed before kagent named a task may still have reached a
// replacement pod, so it is fenced the same way.
func TestLostPinBeforeATaskFencesTheTurn(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	remote.streams = append(remote.streams, func(context.Context, string, string) []item {
		return []item{{err: &kagent.PinLostError{}}}
	})
	h := newHarness(t, remote)
	h.kagentV0 = true
	h.restart()
	o, p := testOwner(1), h.prompt("prompt-1")
	if recorder := h.turn(o, p); !strings.Contains(recorder.Body.String(), failRevision) {
		t.Fatalf("turn that lost its pin before a task = %s", recorder.Body.String())
	}
	if remote.count("stream ") != 1 {
		t.Fatalf("a possibly delivered send was retried: %v", remote.snapshotCalls())
	}
	if _, response := h.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled before the fence")
	}
	eventually(t, "fence", func() bool { return remote.count("delete ") == 1 })
	// The send stays a taskless intent, which survives a restart.
	h.restart()
	if _, response := h.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled while the fenced pods remain")
	}
	remote.setPods("pod-b")
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatalf("the fenced send did not settle: %v", remote.snapshotCalls())
	}
}

// A kagent v0 send is unbound from before it is dispatched until its client
// confirms the pinned pod. A broker that stops in between cannot know which
// pod received it, so the revoked turn token does not settle it.
func TestStopBeforeThePinIsConfirmedFencesTheSend(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, _, _ string) []item {
		close(started)
		<-ctx.Done()
		return nil
	})
	g := newGovernedHarness(t, remote)
	g.kagentV0 = true
	g.restart()
	o, p := testOwner(1), g.prompt("prompt-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.governedTurn(o, p)
	}()
	<-started
	g.b.close()
	<-done
	g.open()
	if _, response := g.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled on the revoked turn token alone")
	}
	eventually(t, "fence", func() bool { return remote.count("delete ") == 1 })
	if _, response := g.controlOnce(settlePath, o, p); response.SettlementProven {
		t.Fatal("settled while the fenced pods remain")
	}
	remote.setPods("pod-b")
	if !g.control(settlePath, o, p).SettlementProven {
		t.Fatalf("the fenced send did not settle: %v", remote.snapshotCalls())
	}
}

// A send error the client confirmed on the pinned pod keeps the governed
// proof: the turn's revoked token settles it once its task cannot appear.
func TestConfirmedSendErrorSettlesOnTheRevokedToken(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	remote.streams = append(remote.streams, func(context.Context, string, string) []item {
		return []item{{err: errors.New("connection reset")}}
	})
	g := newGovernedHarness(t, remote)
	g.kagentV0 = true
	g.restart()
	o, p := testOwner(1), g.prompt("prompt-1")
	g.governedTurn(o, p)
	g.advance(dispatchFenceWindow + time.Second)
	// pod-a still runs: the proof needs no replacement.
	if !g.control(settlePath, o, p).SettlementProven {
		t.Fatalf("the confirmed send did not settle: %v", remote.snapshotCalls())
	}
}

// kagent v0 leaves a task working when its pod dies. Once the pod that ran
// the turn is gone, the turn ends instead of waiting for its deadline, and
// settlement cancels the task.
func TestV0TurnEndsWhenItsPodIsGone(t *testing.T) {
	remote := newFakeRemote()
	remote.v0 = true
	remote.setPods("pod-a")
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
		remote.setTask(task, messageID)
		close(started)
		return []item{{event: task}, {err: errors.New("connection reset by peer")}}
	})
	h := newHarness(t, remote)
	h.kagentV0 = true
	h.restart()
	o, p := testOwner(1), h.prompt("prompt-1")
	done := make(chan string, 1)
	go func() { done <- h.turn(o, p).Body.String() }()
	<-started
	select {
	case body := <-done:
		t.Fatalf("the turn ended while its pod runs: %s", body)
	case <-time.After(2 * terminalPoll):
	}
	remote.setPods("pod-b")
	h.advance(podCheckEvery + time.Second)
	select {
	case body := <-done:
		if !strings.Contains(body, failUnknown) {
			t.Fatalf("turn whose pod is gone = %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the turn kept waiting after its pod was gone")
	}
	if !h.control(settlePath, o, p).SettlementProven || remote.count("cancel task-1") == 0 {
		t.Fatalf("the abandoned task was not settled by a cancel: %v", remote.snapshotCalls())
	}
}
