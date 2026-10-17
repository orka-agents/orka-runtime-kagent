package kagent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

type fixedPin struct {
	hash string
	err  error
}

func (p fixedPin) ConfigHash(context.Context) (string, error) { return p.hash, p.err }
func (p fixedPin) PodUIDs(context.Context) ([]string, error)  { return []string{"pod-a"}, p.err }

// rollingPin reports one pod until its first PodUIDs call, then a new one, as
// if a rollout replaced the pod during a cancel.
type rollingPin struct {
	mu    sync.Mutex
	calls int
}

func (p *rollingPin) ConfigHash(context.Context) (string, error) { return "42", nil }
func (p *rollingPin) PodUIDs(context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls == 1 {
		return []string{"pod-a"}, nil
	}
	return []string{"pod-b"}, nil
}

// fakeV0Controller answers the kagent v0.10 controller routes the client uses.
type fakeV0Controller struct {
	mu       sync.Mutex
	sessions map[string]string // session ID -> user ID
	agentID  string
	a2a      []*http.Request
	methods  []string
}

func (f *fakeV0Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	user := r.Header.Get("X-User-Id")
	reply := func(status int, data any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": status >= 400, "data": data})
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/sessions":
		var body struct {
			AgentRef string `json:"agent_ref"`
			ID       string `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch {
		case body.AgentRef != "kagent/probe-v0":
			reply(http.StatusBadRequest, nil)
		case body.ID == "dead":
			reply(http.StatusInternalServerError, nil)
		default:
			f.sessions[body.ID] = user
			reply(http.StatusCreated, map[string]string{"id": body.ID, "user_id": user, "agent_id": f.agentID})
		}
	case strings.HasPrefix(r.URL.Path, "/api/sessions/"):
		id := strings.TrimPrefix(r.URL.Path, "/api/sessions/")
		if f.sessions[id] != user {
			reply(http.StatusNotFound, nil)
			return
		}
		if r.Method == http.MethodDelete {
			delete(f.sessions, id)
			reply(http.StatusOK, nil)
			return
		}
		reply(http.StatusOK, map[string]any{"session": map[string]string{"id": id, "user_id": user, "agent_id": f.agentID}})
	case r.Method == http.MethodPost && r.URL.Path == "/api/a2a/kagent/probe-v0/":
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&rpc)
		f.a2a = append(f.a2a, r)
		f.methods = append(f.methods, rpc.Method)
		w.Header().Set("Content-Type", "application/json")
		if rpc.Method == "CancelTask" {
			// A finished task with no execution left cannot be canceled.
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
				"error": map[string]any{"code": -32002, "message": "task in non-cancelable state completed"}})
			return
		}
		task := &a2atype.Task{ID: "task-1", ContextID: "ctx", Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": task})
	default:
		http.NotFound(w, r)
	}
}

func newV0Test(t *testing.T) (*V0Client, *fakeV0Controller) {
	t.Helper()
	controller := &fakeV0Controller{sessions: map[string]string{}, agentID: "kagent__NS__probe_v0"}
	server := httptest.NewServer(controller)
	t.Cleanup(server.Close)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	client, err := NewV0(config.Kagent{ControlPlaneURL: "http://localhost:" + port, Agent: config.AgentRef{Namespace: "kagent", Name: "probe-v0"}},
		fixedPin{hash: "15270541297416117040"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, controller
}

func TestV0SessionsAreCreatedReadAndDeletedAsTheUser(t *testing.T) {
	client, _ := newV0Test(t)
	ctx := context.Background()
	session, err := client.CreateSession(ctx, "orka-user", "orka-session")
	if err != nil || session.ID != "orka-session" || session.Revision != "15270541297416117040" {
		t.Fatalf("CreateSession = %+v, %v", session, err)
	}
	if again, err := client.CreateSession(ctx, "orka-user", "orka-session"); err != nil || again.ID != session.ID {
		t.Fatalf("retried CreateSession = %+v, %v", again, err)
	}
	if _, err := client.GetSession(ctx, "someone-else", "orka-session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user read the session: %v", err)
	}
	if read, err := client.GetSession(ctx, "orka-user", "orka-session"); err != nil || read.Revision != session.Revision {
		t.Fatalf("GetSession = %+v, %v", read, err)
	}
	if err := client.DeleteSession(ctx, "orka-user", "orka-session"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSession(ctx, "orka-user", "orka-session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a deleted session read as %v", err)
	}
}

// A session kagent created is the broker's to delete even when the Agent's
// config cannot be pinned, as when the Agent has no ready pod.
func TestV0UnpinnedSessionIsStillReturned(t *testing.T) {
	client, _ := newV0Test(t)
	client.pin = fixedPin{err: ErrRollout}
	ctx := context.Background()
	session, err := client.CreateSession(ctx, "orka-user", "orka-session")
	if !errors.Is(err, ErrUnpinned) || !errors.Is(err, ErrRollout) || session.ID != "orka-session" || session.Revision != "" {
		t.Fatalf("unpinned CreateSession = %+v, %v", session, err)
	}
	if read, err := client.GetSession(ctx, "orka-user", "orka-session"); !errors.Is(err, ErrUnpinned) || read.Revision != "" {
		t.Fatalf("unpinned GetSession = %+v, %v", read, err)
	}
	if err := client.DeleteSession(ctx, "orka-user", "orka-session"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSession(ctx, "orka-user", "orka-session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a deleted unpinned session read as %v", err)
	}
}

func TestV0CreateRefusalsAreNeverDefinite(t *testing.T) {
	client, controller := newV0Test(t)
	ctx := context.Background()
	if _, err := client.CreateSession(ctx, "orka-user", "dead"); err == nil || errors.Is(err, ErrRejected) {
		t.Fatalf("a 500 was treated as definite: %v", err)
	}
	other, err := NewV0(config.Kagent{ControlPlaneURL: client.base, Agent: config.AgentRef{Namespace: "kagent", Name: "other"}}, fixedPin{hash: "1"})
	if err != nil {
		t.Fatal(err)
	}
	// v0 answers 400 for any failure to look up the Agent, transient ones
	// included, so even this refusal is not proof that nothing was created.
	if _, err := other.CreateSession(ctx, "orka-user", "orka-session"); err == nil || errors.Is(err, ErrRejected) {
		t.Fatalf("an unknown-Agent refusal was treated as definite: %v", err)
	}
	controller.agentID = "kagent__NS__someone_else"
	if _, err := client.CreateSession(ctx, "orka-user", "orka-session"); err == nil {
		t.Fatal("a session for another Agent was adopted")
	}
}

func TestV0A2AGoesToTheAgentRouteAsTheUser(t *testing.T) {
	client, controller := newV0Test(t)
	task, err := client.GetTask(context.Background(), "orka-user", "task-1")
	if err != nil || task.ID != "task-1" {
		t.Fatalf("GetTask = %+v, %v", task, err)
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if len(controller.a2a) != 1 || controller.methods[0] != "GetTask" {
		t.Fatalf("A2A calls = %v", controller.methods)
	}
	request := controller.a2a[0]
	if request.Header.Get("X-User-Id") != "orka-user" || request.Header.Get("A2A-Version") != string(a2atype.Version) {
		t.Fatalf("A2A headers = %v", request.Header)
	}
}

func TestV0PinRequiresExactlyOneReadyPod(t *testing.T) {
	decode := func(raw string) podList {
		var l podList
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	const pod = `{"metadata":{"uid":"pod-a","annotations":{"kagent.dev/config-hash":"42"}},` +
		`"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}`
	if hash, err := decode(`{"items":[` + pod + `]}`).configHash(); err != nil || hash != "42" {
		t.Fatalf("one ready pod = %q, %v", hash, err)
	}
	terminating := strings.Replace(pod, `"uid":"pod-a"`, `"uid":"pod-b","deletionTimestamp":"2026-10-16T00:00:00Z"`, 1)
	for name, raw := range map[string]string{
		"no pod":                  `{"items":[]}`,
		"a pod still terminating": `{"items":[` + pod + `,` + terminating + `]}`,
		"two replicas":            `{"items":[` + pod + `,` + strings.Replace(pod, "pod-a", "pod-b", 1) + `]}`,
		"being deleted":           `{"items":[` + terminating + `]}`,
		"not ready":               `{"items":[` + strings.Replace(pod, `"status":"True"`, `"status":"False"`, 1) + `]}`,
		"pending":                 `{"items":[` + strings.Replace(pod, `"Running"`, `"Pending"`, 1) + `]}`,
	} {
		if _, err := decode(raw).configHash(); !errors.Is(err, ErrRollout) {
			t.Fatalf("%s: err = %v, want ErrRollout", name, err)
		}
	}
	if _, err := decode(`{"items":[` + strings.Replace(pod, `"42"`, `"not-a-hash"`, 1) + `]}`).configHash(); err == nil || errors.Is(err, ErrRollout) {
		t.Fatalf("a missing hash was accepted: %v", err)
	}
}

func TestV0CancelProvesNoExecutionIsLeft(t *testing.T) {
	client, controller := newV0Test(t)
	task, err := client.CancelTask(context.Background(), "orka-user", "task-1")
	if err != nil || task.Status.State != a2atype.TaskStateCompleted {
		t.Fatalf("CancelTask of a finished task = %+v, %v", task, err)
	}
	controller.mu.Lock()
	methods := append([]string(nil), controller.methods...)
	controller.mu.Unlock()
	if len(methods) != 2 || methods[0] != "CancelTask" || methods[1] != "GetTask" {
		t.Fatalf("A2A calls = %v", methods)
	}
	client.pin = fixedPin{err: ErrRollout}
	if _, err := client.CancelTask(context.Background(), "orka-user", "task-1"); !errors.Is(err, ErrRollout) {
		t.Fatalf("a cancel during a rollout = %v, want ErrRollout", err)
	}
}

func TestV0CancelIsProofOnlyIfThePodStayedTheSame(t *testing.T) {
	client, _ := newV0Test(t)
	client.pin = &rollingPin{}
	if _, err := client.CancelTask(context.Background(), "orka-user", "task-1"); !errors.Is(err, ErrRollout) {
		t.Fatalf("a cancel across a pod change = %v, want ErrRollout", err)
	}
}
