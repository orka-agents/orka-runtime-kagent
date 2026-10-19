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
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

const testImage = "sha256:5ee9592cb7d959538d0f0fe36acf31ba3a06de084f84048b1459c3bdfafcda1f"

type fixedPin struct {
	hash    string
	image   string
	address string
	err     error
}

func (p fixedPin) Pod(context.Context) (Pod, error) {
	if p.err != nil {
		return Pod{}, p.err
	}
	image := p.image
	if image == "" {
		image = testImage
	}
	return Pod{UID: "pod-a", ConfigHash: p.hash, ImageDigest: image, Address: p.address}, nil
}
func (p fixedPin) PodUIDs(context.Context) ([]string, error) { return []string{"pod-a"}, p.err }

// rollingPin reports pod-a until its first PodUIDs call, then pod-b, as if a
// rollout replaced the pod during a cancel.
type rollingPin struct {
	mu      sync.Mutex
	calls   int
	address string
}

func (p *rollingPin) uid() string {
	if p.calls <= 1 {
		return "pod-a"
	}
	return "pod-b"
}

func (p *rollingPin) Pod(context.Context) (Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Pod{UID: p.uid(), ConfigHash: "42", ImageDigest: testImage, Address: p.address}, nil
}

func (p *rollingPin) PodUIDs(context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return []string{p.uid()}, nil
}

// fakeV0Pod answers the A2A 0.3 JSON-RPC methods an Agent pod serves.
type fakeV0Pod struct {
	mu       sync.Mutex
	requests []*http.Request
	methods  []string
	// cardURL is the URL its agent card names; empty means this Agent's.
	cardURL string
	// closeAfterCard closes the connection that served the card.
	closeAfterCard bool
	// beforeReply runs after a request is recorded, before the reply.
	beforeReply func()
}

func (f *fakeV0Pod) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/.well-known/agent-card.json" {
		f.mu.Lock()
		card, closing := f.cardURL, f.closeAfterCard
		f.mu.Unlock()
		if card == "" {
			card = "http://probe-v0.kagent:8080"
		}
		if closing {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"name": "probe_v0", "url": card, "protocolVersion": "0.3"})
		return
	}
	var rpc struct {
		ID     any             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	_ = json.NewDecoder(r.Body).Decode(&rpc)
	f.mu.Lock()
	f.requests = append(f.requests, r)
	f.methods = append(f.methods, rpc.Method)
	before := f.beforeReply
	f.mu.Unlock()
	if before != nil {
		before()
	}
	switch rpc.Method {
	case "message/stream":
		w.Header().Set("Content-Type", "text/event-stream")
		for _, state := range []string{"submitted", "completed"} {
			event := map[string]any{"kind": "status-update", "taskId": "task-1", "contextId": "ctx",
				"status": map[string]any{"state": state}, "final": state == "completed"}
			data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": event})
			_, _ = w.Write([]byte("data: " + string(data) + "\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	case "tasks/cancel":
		// A finished task with no execution left cannot be canceled.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
			"error": map[string]any{"code": -32002, "message": "task in non-cancelable state completed"}})
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
			"error": map[string]any{"code": -32601, "message": "method not found"}})
	}
}

func (f *fakeV0Pod) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.methods...)
}

// fakeV0Controller answers the kagent v0.10 controller routes the client uses.
type fakeV0Controller struct {
	mu       sync.Mutex
	sessions map[string]string // session ID -> user ID
	agentID  string
	commit   string
	a2a      []*http.Request
	methods  []string
	// event is the session's latest event, which kagent returns with the
	// session unless the read asks only for later events.
	event string
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
	case r.Method == http.MethodGet && r.URL.Path == "/version":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"kagent_version": "0.10.3", "git_commit": f.commit})
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
		data := map[string]any{"session": map[string]string{"id": id, "user_id": user, "agent_id": f.agentID}}
		if after, err := time.Parse(time.RFC3339, r.URL.Query().Get("after")); f.event != "" && (err != nil || after.Before(time.Now())) {
			data["events"] = []map[string]string{{"data": f.event}}
		}
		reply(http.StatusOK, data)
	case r.Method == http.MethodPost && r.URL.Path == "/api/a2a/kagent/probe-v0/":
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&rpc)
		f.a2a = append(f.a2a, r)
		f.methods = append(f.methods, rpc.Method)
		w.Header().Set("Content-Type", "application/json")
		task := &a2atype.Task{ID: "task-1", ContextID: "ctx", Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": task})
	default:
		http.NotFound(w, r)
	}
}

func newV0Test(t *testing.T) (*V0Client, *fakeV0Controller) {
	client, controller, _ := newV0PodTest(t)
	return client, controller
}

func newV0PodTest(t *testing.T) (*V0Client, *fakeV0Controller, *fakeV0Pod) {
	t.Helper()
	controller := &fakeV0Controller{sessions: map[string]string{}, agentID: "kagent__NS__probe_v0", commit: "8878c39"}
	server := httptest.NewServer(controller)
	t.Cleanup(server.Close)
	pod := &fakeV0Pod{}
	podServer := httptest.NewServer(pod)
	t.Cleanup(podServer.Close)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	client, err := NewV0(config.Kagent{
		ControlPlaneURL: "http://localhost:" + port, Agent: config.AgentRef{Namespace: "kagent", Name: "probe-v0"},
		ExpectedRevision: "15270541297416117040", ExpectedImageDigest: testImage,
	}, fixedPin{hash: "15270541297416117040", address: podServer.Listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, controller, pod
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

// A session read carries no events, so a large one cannot make the session
// unreadable.
func TestV0SessionReadSkipsLargeEvents(t *testing.T) {
	client, controller := newV0Test(t)
	ctx := context.Background()
	if _, err := client.CreateSession(ctx, "orka-user", "orka-session"); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	controller.event = strings.Repeat("x", maxV0ResponseBytes)
	controller.mu.Unlock()
	if read, err := client.GetSession(ctx, "orka-user", "orka-session"); err != nil || read.ID != "orka-session" {
		t.Fatalf("GetSession after a large event = %+v, %v", read, err)
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
	other, err := NewV0(config.Kagent{ControlPlaneURL: client.base, Agent: config.AgentRef{Namespace: "kagent", Name: "other"},
		ExpectedImageDigest: testImage}, fixedPin{hash: "1"})
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
		`"spec":{"containers":[{"name":"kagent","ports":[{"name":"http","containerPort":8080}]}]},` +
		`"status":{"phase":"Running","podIP":"10.244.0.69","conditions":[{"type":"Ready","status":"True"}],` +
		`"containerStatuses":[{"name":"kagent","imageID":"ghcr.io/kagent-dev/kagent/golang-adk@` + testImage + `"}]}}`
	want := Pod{UID: "pod-a", ConfigHash: "42", ImageDigest: testImage, Address: "10.244.0.69:8080"}
	if got, err := decode(`{"items":[` + pod + `]}`).only(); err != nil || got != want {
		t.Fatalf("one ready pod = %+v, %v", got, err)
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
		if _, err := decode(raw).only(); !errors.Is(err, ErrRollout) {
			t.Fatalf("%s: err = %v, want ErrRollout", name, err)
		}
	}
	for name, raw := range map[string]string{
		"no config hash":   strings.Replace(pod, `"42"`, `"not-a-hash"`, 1),
		"no address":       strings.Replace(pod, `"podIP":"10.244.0.69",`, ``, 1),
		"no A2A port":      strings.Replace(pod, `"name":"http"`, `"name":"metrics"`, 1),
		"two A2A ports":    strings.Replace(pod, `{"name":"http","containerPort":8080}`, `{"name":"http","containerPort":8080},{"name":"http","containerPort":9090}`, 1),
		"no image digest":  strings.Replace(pod, "@"+testImage, ":0.10.3", 1),
		"another status":   strings.Replace(pod, `"containerStatuses":[{"name":"kagent"`, `"containerStatuses":[{"name":"sidecar"`, 1),
		"bad port":         strings.Replace(pod, `"containerPort":8080`, `"containerPort":0`, 1),
		"image ID by hash": strings.Replace(pod, "ghcr.io/kagent-dev/kagent/golang-adk@"+testImage, "sha256:short", 1),
	} {
		if _, err := decode(`{"items":[` + raw + `]}`).only(); err == nil || errors.Is(err, ErrRollout) {
			t.Fatalf("%s: err = %v, want a pin error", name, err)
		}
	}
	bare := strings.Replace(pod, "ghcr.io/kagent-dev/kagent/golang-adk@"+testImage, testImage, 1)
	if got, err := decode(`{"items":[` + bare + `]}`).only(); err != nil || got.ImageDigest != testImage {
		t.Fatalf("a bare image ID = %+v, %v", got, err)
	}
}

func TestV0CancelProvesNoExecutionIsLeft(t *testing.T) {
	client, controller, pod := newV0PodTest(t)
	task, err := client.CancelTask(context.Background(), "orka-user", "task-1")
	if err != nil || task.Status.State != a2atype.TaskStateCompleted {
		t.Fatalf("CancelTask of a finished task = %+v, %v", task, err)
	}
	// The cancel goes to the pod; the refusal is answered from the
	// controller's committed task.
	controller.mu.Lock()
	methods := append([]string(nil), controller.methods...)
	controller.mu.Unlock()
	if calls := pod.calls(); len(calls) != 1 || calls[0] != "tasks/cancel" || len(methods) != 1 || methods[0] != "GetTask" {
		t.Fatalf("pod calls = %v, controller calls = %v", calls, methods)
	}
	pod.mu.Lock()
	user := pod.requests[0].Header.Get("X-User-Id")
	pod.mu.Unlock()
	if user != "orka-user" {
		t.Fatalf("cancel user = %q", user)
	}
	client.pin = fixedPin{err: ErrRollout}
	if _, err := client.CancelTask(context.Background(), "orka-user", "task-1"); !errors.Is(err, ErrRollout) {
		t.Fatalf("a cancel during a rollout = %v, want ErrRollout", err)
	}
}

func TestV0CancelIsProofOnlyIfThePodStayedTheSame(t *testing.T) {
	client, _ := newV0Test(t)
	client.pin = &rollingPin{address: client.pin.(fixedPin).address}
	if _, err := client.CancelTask(context.Background(), "orka-user", "task-1"); !errors.Is(err, ErrRollout) {
		t.Fatalf("a cancel across a pod change = %v, want ErrRollout", err)
	}
}

func streamEvents(client *V0Client, authorization string) ([]a2atype.Event, error) {
	var events []a2atype.Event
	for event, err := range client.Stream(context.Background(), "orka-user", "ctx", "msg-1", "hello", authorization) {
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
	return events, nil
}

// A send goes to the pinned pod, as the user, with the caller's bearer: the
// headers kagent's controller would forward.
func TestV0StreamGoesToThePinnedPod(t *testing.T) {
	client, controller, pod := newV0PodTest(t)
	events, err := streamEvents(client, "Bearer turn-token")
	if err != nil || len(events) != 2 {
		t.Fatalf("Stream = %d events, %v", len(events), err)
	}
	if update, ok := events[1].(*a2atype.TaskStatusUpdateEvent); !ok || update.Status.State != a2atype.TaskStateCompleted || update.TaskID != "task-1" {
		t.Fatalf("last event = %#v", events[1])
	}
	if calls := pod.calls(); len(calls) != 1 || calls[0] != "message/stream" {
		t.Fatalf("pod calls = %v", calls)
	}
	pod.mu.Lock()
	header := pod.requests[0].Header
	pod.mu.Unlock()
	if header.Get("X-User-Id") != "orka-user" || header.Get("Authorization") != "Bearer turn-token" {
		t.Fatalf("pod request headers = %v", header)
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if len(controller.methods) != 0 {
		t.Fatalf("the send went through the controller: %v", controller.methods)
	}
}

// Nothing is sent unless the Agent's only pod runs the pinned image, and a
// send that never reached a pod is proof that nothing was accepted.
func TestV0StreamRefusesBeforeSendingWithoutAPinnedPod(t *testing.T) {
	for name, pin := range map[string]fixedPin{
		"no single ready pod": {err: ErrRollout},
		"another image":       {hash: "15270541297416117040", image: "sha256:" + strings.Repeat("0", 64)},
		"another config":      {hash: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			client, _, pod := newV0PodTest(t)
			pin.address = client.pin.(fixedPin).address
			client.pin = pin
			if _, err := streamEvents(client, ""); !errors.Is(err, ErrNotSent) || !NotAccepted(err) || errors.Is(err, ErrPinHeld) {
				t.Fatalf("Stream = %v, want ErrNotSent", err)
			}
			if calls := pod.calls(); len(calls) != 0 {
				t.Fatalf("pod calls = %v", calls)
			}
		})
	}
}

// A pod that is gone fails the dial, before anything is written.
func TestV0StreamToAGonePodIsNotSent(t *testing.T) {
	client, _, _ := newV0PodTest(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	client.pin = fixedPin{hash: "15270541297416117040", address: address}
	if _, err := streamEvents(client, ""); !errors.Is(err, ErrNotSent) || !NotAccepted(err) {
		t.Fatalf("Stream to a closed port = %v, want ErrNotSent", err)
	}
}

// swappingPin returns the pinned pod until swap, then its replacement.
type swappingPin struct {
	mu      sync.Mutex
	address string
	swapped bool
}

func (p *swappingPin) Pod(context.Context) (Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	uid := "pod-a"
	if p.swapped {
		uid = "pod-b"
	}
	return Pod{UID: uid, ConfigHash: "15270541297416117040", ImageDigest: testImage, Address: p.address}, nil
}

func (p *swappingPin) PodUIDs(ctx context.Context) ([]string, error) {
	pod, _ := p.Pod(ctx)
	return []string{pod.UID}, nil
}

// If the pinned pod is replaced while the send is in flight, the turn may run
// on the replacement's config, so the stream ends with ErrPinLost once the
// task is known.
func TestV0StreamEndsWhenThePinnedPodIsReplaced(t *testing.T) {
	client, _, pod := newV0PodTest(t)
	pin := &swappingPin{address: client.pin.(fixedPin).address}
	client.pin = pin
	pod.beforeReply = func() {
		pin.mu.Lock()
		pin.swapped = true
		pin.mu.Unlock()
	}
	events, err := streamEvents(client, "")
	var lost *PinLostError
	if !errors.Is(err, ErrPinLost) || NotAccepted(err) || len(events) != 0 || !errors.As(err, &lost) || lost.TaskID != "task-1" {
		t.Fatalf("Stream across a pod change = %d events, %v", len(events), err)
	}
}

func TestV0VersionMustBeVerifiedOrAccepted(t *testing.T) {
	client, controller := newV0Test(t)
	ctx := context.Background()
	if err := client.CheckVersion(ctx); err != nil {
		t.Fatalf("v0.10.3 = %v", err)
	}
	controller.mu.Lock()
	controller.commit = "0123456"
	controller.mu.Unlock()
	if err := client.CheckVersion(ctx); !errors.Is(err, ErrUnverifiedBuild) {
		t.Fatalf("an unverified build = %v", err)
	}
	client.accepted = []string{"0123456789ab"}
	if err := client.CheckVersion(ctx); err != nil {
		t.Fatalf("an accepted build = %v", err)
	}
	controller.mu.Lock()
	controller.commit = ""
	controller.mu.Unlock()
	if err := client.CheckVersion(ctx); !errors.Is(err, ErrUnverifiedBuild) {
		t.Fatalf("a build without a commit = %v", err)
	}
}

// A pod running another image never matches the pinned revision.
func TestV0SessionOnAnotherImageDoesNotMatchThePin(t *testing.T) {
	client, _ := newV0Test(t)
	client.pin = fixedPin{hash: "15270541297416117040", image: "sha256:" + strings.Repeat("1", 64)}
	session, err := client.CreateSession(context.Background(), "orka-user", "orka-session")
	if err != nil || session.ID != "orka-session" || session.Revision != "" {
		t.Fatalf("CreateSession on another image = %+v, %v", session, err)
	}
}

// The listener at the pinned pod's address must serve this Agent's card, and
// the send must reuse the connection that served it.
func TestV0StreamIsBoundToAConnectionServingTheAgentsCard(t *testing.T) {
	for name, set := range map[string]func(*fakeV0Pod){
		"another Agent's card": func(p *fakeV0Pod) { p.cardURL = "http://probe-v0.other:8080" },
		"connection closed":    func(p *fakeV0Pod) { p.closeAfterCard = true },
	} {
		t.Run(name, func(t *testing.T) {
			client, _, pod := newV0PodTest(t)
			set(pod)
			if _, err := streamEvents(client, ""); !errors.Is(err, ErrNotSent) || !NotAccepted(err) || errors.Is(err, ErrPinHeld) {
				t.Fatalf("Stream = %v, want ErrNotSent", err)
			}
			for _, call := range pod.calls() {
				if call == "message/stream" {
					t.Fatal("the message was sent")
				}
			}
		})
	}
}

// failingPod accepts the send and then fails before any event, as a
// connection that drops would.
type failingPod struct{ *fakeV0Pod }

func (f failingPod) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		f.mu.Lock()
		before := f.beforeReply
		f.mu.Unlock()
		if before != nil {
			before()
		}
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	f.fakeV0Pod.ServeHTTP(w, r)
}

// A send that fails before its first event may still have reached a pod, so
// the pod is checked then too, and an error the check confirmed says so.
func TestV0StreamChecksThePodWhenTheSendFailsEarly(t *testing.T) {
	client, _, _ := newV0PodTest(t)
	pod := &fakeV0Pod{}
	server := httptest.NewServer(failingPod{pod})
	t.Cleanup(server.Close)
	pin := &swappingPin{address: server.Listener.Addr().String()}
	client.pin = pin
	if _, err := streamEvents(client, ""); !errors.Is(err, ErrPinHeld) || errors.Is(err, ErrPinLost) || NotAccepted(err) {
		t.Fatalf("an early failure on the pinned pod = %v, want a confirmed ordinary error", err)
	}
	pod.beforeReply = func() {
		pin.mu.Lock()
		pin.swapped = true
		pin.mu.Unlock()
	}
	var lost *PinLostError
	if _, err := streamEvents(client, ""); !errors.As(err, &lost) || lost.TaskID != "" || errors.Is(err, ErrPinHeld) {
		t.Fatalf("an early failure across a pod change = %v, want a PinLostError without a task", err)
	}
}
