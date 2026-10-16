package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

const (
	testBearer   = "broker-bearer-0123456789abcdef0123456789"
	testModel    = "kagent/kagent/probe"
	testRevision = "c85fb79c49c56d3720b2744f8e330d1cbe5f18fe84ad407f060043677716806f"
)

// errUnauthorized is how kagent answers task reads in a deleted session.
var errUnauthorized = errors.Join(kagent.ErrRejected, a2atype.ErrUnauthorized)

type item struct {
	event a2atype.Event
	err   error
}

// fakeRemote is an in-memory kagent. Streams are scripted per call.
type fakeRemote struct {
	mu        sync.Mutex
	revision  string
	createErr []error
	sessions  map[string]kagent.Session // by request ID
	owners    map[string]string         // session ID -> user ID
	deleted   map[string]bool
	deleting  map[string]bool
	stuck     map[a2atype.TaskID]bool // CancelTask cannot stop these
	tasks     map[a2atype.TaskID]*a2atype.Task
	messages  map[string]a2atype.TaskID // message ID -> task
	streams   []func(ctx context.Context, contextID, messageID string) []item
	calls     []string
	findErr   error
	cancelErr error    // CancelTask fails with this, as a timed-out cancel would
	pods      []string // UIDs of the pods running the Agent (kagent v0)
	onFind    func()   // runs during a task lookup
	auth      string   // the authorization of the latest stream
	// v0 models kagent v0.x: a deleted session's tasks stay readable and
	// cancellable by ID, because deletion does not stop the Agent, but they can
	// no longer be listed by session.
	v0 bool
}

func (f *fakeRemote) lastAuthorization() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auth
}

func newFakeRemote() *fakeRemote {
	return &fakeRemote{revision: testRevision, sessions: map[string]kagent.Session{}, owners: map[string]string{},
		deleted: map[string]bool{}, deleting: map[string]bool{}, stuck: map[a2atype.TaskID]bool{},
		tasks: map[a2atype.TaskID]*a2atype.Task{}, messages: map[string]a2atype.TaskID{}}
}

// deleteAll deletes every session, as kagent's idle expiry or an operator would.
func (f *fakeRemote) deleteAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := range f.owners {
		f.deleted[id] = true
	}
}

func (f *fakeRemote) markDeleting() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := range f.owners {
		f.deleting[id] = true
	}
}

func (f *fakeRemote) setRevision(revision string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revision = revision
}

func (f *fakeRemote) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeRemote) snapshotCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeRemote) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeRemote) CreateSession(_ context.Context, userID, requestID string) (kagent.Session, error) {
	f.record("create " + userID + " " + requestID)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.createErr) > 0 {
		err := f.createErr[0]
		f.createErr = f.createErr[1:]
		if err != nil {
			return kagent.Session{}, err
		}
	}
	if s, ok := f.sessions[requestID]; ok {
		return s, nil
	}
	s := kagent.Session{ID: uuid.NewString(), Revision: f.revision}
	f.sessions[requestID], f.owners[s.ID] = s, userID
	return s, nil
}

func (f *fakeRemote) GetSession(_ context.Context, userID, id string) (kagent.Session, error) {
	f.record("get-session " + id)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleted[id] || f.owners[id] != userID {
		return kagent.Session{}, kagent.ErrNotFound
	}
	return kagent.Session{ID: id, Revision: f.revision, Deleting: f.deleting[id]}, nil
}

func (f *fakeRemote) DeleteSession(_ context.Context, userID, id string) error {
	f.record("delete " + id)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.owners[id] != userID || f.deleted[id] {
		return kagent.ErrNotFound
	}
	f.deleted[id] = true
	return nil
}

func (f *fakeRemote) Stream(ctx context.Context, _, contextID, messageID, _, authorization string) iter.Seq2[a2atype.Event, error] {
	f.record("stream " + messageID)
	f.mu.Lock()
	f.auth = authorization
	var script func(context.Context, string, string) []item
	if len(f.streams) > 0 {
		script, f.streams = f.streams[0], f.streams[1:]
	}
	f.mu.Unlock()
	return func(yield func(a2atype.Event, error) bool) {
		if script == nil {
			yield(nil, errors.New("no scripted stream"))
			return
		}
		for _, it := range script(ctx, contextID, messageID) {
			if !yield(it.event, it.err) {
				return
			}
		}
	}
}

func (f *fakeRemote) GetTask(_ context.Context, _ string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	f.record("get-task " + string(taskID))
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[taskID]
	if ok && f.deleted[task.ContextID] && !f.v0 {
		return nil, errUnauthorized
	}
	if !ok {
		return nil, kagent.ErrNotFound
	}
	copy := *task
	return &copy, nil
}

func (f *fakeRemote) FindTaskByMessage(_ context.Context, _, contextID, messageID string) (*a2atype.Task, error) {
	f.record("find " + messageID)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onFind != nil {
		f.onFind()
	}
	if f.findErr != nil {
		return nil, f.findErr
	}
	if f.deleted[contextID] {
		if f.v0 {
			return nil, errors.New("get session: not found") // v0 cannot list a deleted session's tasks
		}
		return nil, errUnauthorized
	}
	id, ok := f.messages[messageID]
	if !ok {
		return nil, kagent.ErrNotFound
	}
	copy := *f.tasks[id]
	return &copy, nil
}

func (f *fakeRemote) AgentPodUIDs(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.pods...), nil
}

func (f *fakeRemote) setPods(uids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods = uids
}

func (f *fakeRemote) CancelTask(_ context.Context, _ string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	f.record("cancel " + string(taskID))
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}
	task, ok := f.tasks[taskID]
	if ok && f.deleted[task.ContextID] && !f.v0 {
		return nil, errUnauthorized
	}
	if !ok {
		return nil, kagent.ErrNotFound
	}
	if !task.Status.State.Terminal() && !f.stuck[taskID] {
		task.Status.State = a2atype.TaskStateCanceled
	}
	copy := *task
	return &copy, nil
}

// setTask registers a task and its originating message.
func (f *fakeRemote) setTask(task *a2atype.Task, messageID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[task.ID] = task
	if messageID != "" {
		f.messages[messageID] = task.ID
	}
}

type harness struct {
	t      *testing.T
	b      *lifecycleBroker
	remote *fakeRemote
	dir    string
	digest string
	mu     sync.Mutex
	clock  time.Time
	logs   lockedBuffer
	// governed, when set, opens the broker in governed mode.
	governed *governedSettings
	// kagentV0 opens the broker with kagent v0.x rules.
	kagentV0 bool
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func newHarness(t *testing.T, remote *fakeRemote) *harness {
	t.Helper()
	h := &harness{t: t, remote: remote, dir: filepath.Join(t.TempDir(), "ledger"), digest: config.Digest([]byte("kagent-config")), clock: time.Now().UTC()}
	h.open()
	return h
}

func (h *harness) now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clock
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = h.clock.Add(d)
}

func (h *harness) open() {
	h.t.Helper()
	b, err := newLifecycleBroker(context.Background(), settings{
		bearer: testBearer, configDigest: h.digest, stateDir: h.dir, model: testModel,
		expectedRevision: testRevision, userIDPrefix: "orka", operationTimeout: 5 * time.Second,
		keepalive: 50 * time.Millisecond, cancelGrace: 200 * time.Millisecond, now: h.now,
		log: slog.New(slog.NewJSONHandler(&h.logs, nil)), governed: h.governed, kagentV0: h.kagentV0,
	}, h.remote)
	if err != nil {
		h.t.Fatal(err)
	}
	h.b = b
	h.t.Cleanup(func() {
		if h.b == b {
			b.close()
		}
	})
}

func (h *harness) restart() {
	h.b.close()
	h.open()
}

func testOwner(n int) owner {
	return owner{RuntimeInstanceID: "instance", SupervisorBootID: "boot", ControllerEpoch: 1, RuntimePoolUID: "pool",
		RuntimePoolGeneration: 1, RuntimeSessionUID: fmt.Sprintf("runtime-session-%d", n), RuntimeSessionGeneration: 1,
		RuntimeProfileDigest: config.Digest([]byte("profile")), ProfileDigestSchemaVersion: 1}
}

type promptRef struct {
	id      string
	gen     uint64
	expires time.Time
}

func (h *harness) prompt(id string) *promptRef {
	return &promptRef{id: id, gen: 1, expires: h.now().Add(2 * time.Minute)}
}

// header builds the context the supervisor would stamp, with its
// path-and-identity operation ID.
func (h *harness) header(path string, o owner, p *promptRef, sequence uint64, body []byte) string {
	c := brokerContext{Protocol: brokerProtocol, Owner: o, AgentConfigurationDigest: h.digest, InvocationSequence: sequence, BodySHA256: config.Digest(body)}
	if p != nil {
		c.TaskUID, c.TaskAttempt, c.PromptID = "task-"+p.id, 1, p.id
		c.PromptRequestDigest = config.Digest([]byte(p.id))
		c.LeaseGeneration, c.LeaseExpiresAt = p.gen, p.expires.Format(time.RFC3339Nano)
	}
	identity, _ := json.Marshal(c)
	c.OperationID = "op-" + config.Digest(append([]byte(path+"\n"), identity...))[len("sha256:"):]
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (h *harness) request(ctx context.Context, path, header string, body []byte) *httptest.ResponseRecorder {
	method := http.MethodPost
	if path == statusPath {
		method = http.MethodGet
	}
	request := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+testBearer)
	if header != "" {
		request.Header.Set(brokerContextHeader, header)
	}
	recorder := httptest.NewRecorder()
	h.b.ServeHTTP(recorder, request)
	return recorder
}

func turnBody() []byte {
	return []byte(`{"model":"` + testModel + `","input":"call the probe tool","stream":true,"store":true}`)
}

func (h *harness) turn(o owner, p *promptRef) *httptest.ResponseRecorder {
	body := turnBody()
	return h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 1, body), body)
}

// control retries a settle or retire control until it is proven.
func (h *harness) control(path string, o owner, p *promptRef) controlResponse {
	h.t.Helper()
	header := h.header(path, o, p, 0, []byte("{}"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		recorder := h.request(context.Background(), path, header, []byte("{}"))
		var response controlResponse
		_ = json.Unmarshal(recorder.Body.Bytes(), &response)
		if recorder.Code == http.StatusOK {
			return response
		}
		if recorder.Code != http.StatusConflict || time.Now().After(deadline) {
			h.t.Fatalf("%s status %d body %s", path, recorder.Code, recorder.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// controlOnce issues one control request without retrying.
func (h *harness) controlOnce(path string, o owner, p *promptRef) (int, controlResponse) {
	recorder := h.request(context.Background(), path, h.header(path, o, p, 0, []byte("{}")), []byte("{}"))
	var response controlResponse
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	return recorder.Code, response
}

// eventually polls cond across the broker's sweep interval.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *harness) sessionCount() int {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	return len(h.b.ledger.Sessions)
}

type sseEvent struct {
	name string
	data map[string]any
}

func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var events []sseEvent
	var name string
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			var data map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
				t.Fatalf("event %s data is not JSON: %v", name, err)
			}
			if data["type"] != name {
				t.Fatalf("event %s carries type %v", name, data["type"])
			}
			events = append(events, sseEvent{name: name, data: data})
		}
	}
	return events
}

func names(events []sseEvent) string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.name)
	}
	return strings.Join(out, ",")
}

func completedStream(remote *fakeRemote, taskID a2atype.TaskID, text string) func(context.Context, string, string) []item {
	return func(_ context.Context, contextID, messageID string) []item {
		task := &a2atype.Task{ID: taskID, ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted},
			Artifacts: []*a2atype.Artifact{
				{ID: "tool", Parts: []*a2atype.Part{apia2a.NewToolCallPart("call-1", "probe", map[string]any{"secret": "x"})}},
				{ID: "text", Parts: []*a2atype.Part{a2atype.NewTextPart(text)}},
			}}
		remote.setTask(task, messageID)
		working := &a2atype.Task{ID: taskID, ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
		return []item{
			{event: working},
			{event: &a2atype.TaskArtifactUpdateEvent{TaskID: taskID, ContextID: contextID, Artifact: &a2atype.Artifact{ID: "a1", Parts: []*a2atype.Part{apia2a.NewToolCallPart("call-1", "probe", map[string]any{"secret": "x"})}}}},
			{event: &a2atype.TaskArtifactUpdateEvent{TaskID: taskID, ContextID: contextID, Artifact: &a2atype.Artifact{ID: "a2", Parts: []*a2atype.Part{apia2a.NewToolResultPart("call-1", "probe", map[string]any{"type": "error"})}}}},
			{event: &a2atype.TaskStatusUpdateEvent{TaskID: taskID, ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}},
		}
	}
}

func TestTurnStreamsObservedToolsAndSettlesThenRetiresWithProof(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "probe-done"))
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")

	recorder := h.turn(o, p)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("turn status %d: %s", recorder.Code, recorder.Body.String())
	}
	events := parseSSE(t, recorder.Body.String())
	got := names(events)
	want := "response.created,response.kagent_tool.started,response.kagent_tool.finished,response.output_text.delta,response.completed"
	if got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	if events[3].data["delta"] != "probe-done" {
		t.Fatalf("text = %v", events[3].data["delta"])
	}
	if strings.Contains(recorder.Body.String(), "secret") || strings.Contains(recorder.Body.String(), `"type":"error"`) {
		t.Fatal("tool arguments or results reached the stream")
	}
	userID, requestID := h.b.identity(o)
	if remote.count("create "+userID+" "+requestID) != 1 || !strings.HasPrefix(userID, "orka-") {
		t.Fatalf("session identity not derived from the owner: %v", remote.snapshotCalls())
	}
	if otherUser, _ := h.b.identity(testOwner(2)); otherUser == userID {
		t.Fatal("two Orka sessions share one kagent identity")
	}

	settled := h.control(settlePath, o, p)
	if !settled.SettlementProven || settled.State != "settled" || !config.DigestValid(settled.ProofDigest) {
		t.Fatalf("settle = %#v", settled)
	}
	retired := h.control(retirePath, o, nil)
	if !retired.RetirementProven || retired.State != "retired" || !retired.RemoteSessionCreated {
		t.Fatalf("retire = %#v", retired)
	}
	if remote.count("delete ") != 1 || remote.count("get-session ") == 0 {
		t.Fatalf("retirement did not delete and confirm: %v", remote.snapshotCalls())
	}
}

func TestSecondPromptReusesTheKagentConversation(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "one"), completedStream(remote, "task-2", "two"))
	h := newHarness(t, remote)
	o := testOwner(1)
	first, second := h.prompt("prompt-1"), h.prompt("prompt-2")
	if h.turn(o, first).Code != http.StatusOK {
		t.Fatal("first turn failed")
	}
	if code := h.turn(o, second).Code; code != http.StatusConflict {
		t.Fatalf("a new prompt was admitted before the previous one settled: %d", code)
	}
	h.control(settlePath, o, first)
	recorder := h.turn(o, second)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "two") {
		t.Fatalf("second turn: %d %s", recorder.Code, recorder.Body.String())
	}
	if remote.count("create ") != 1 {
		t.Fatalf("conversation was recreated: %v", remote.snapshotCalls())
	}
}

func TestUnapprovedRevisionIsRefusedBeforeDispatch(t *testing.T) {
	remote := newFakeRemote()
	remote.revision = strings.Repeat("0", 64)
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	recorder := h.turn(o, p)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), failRevision) {
		t.Fatalf("turn = %d %s", recorder.Code, recorder.Body.String())
	}
	if remote.count("stream ") != 0 {
		t.Fatal("a message was dispatched to an unapproved revision")
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("undispatched prompt did not settle")
	}
	if !h.control(retirePath, o, nil).RetirementProven || remote.count("delete ") != 1 {
		t.Fatal("refused session was not deleted on retirement")
	}
}

func TestOnlyKagentNotAcceptedProofIsRetried(t *testing.T) {
	remote := newFakeRemote()
	notAccepted := a2atype.NewError(a2atype.ErrUnsupportedOperation, "busy").
		WithErrorInfoMeta(map[string]string{"reason": "KAGENT_SEND_NOT_ACCEPTED", "retryAfterMs": "100"})
	remote.streams = append(remote.streams,
		func(context.Context, string, string) []item { return []item{{err: notAccepted}} },
		completedStream(remote, "task-1", "after-retry"))
	h := newHarness(t, remote)
	recorder := h.turn(testOwner(1), h.prompt("prompt-1"))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "after-retry") {
		t.Fatalf("turn = %d %s", recorder.Code, recorder.Body.String())
	}
	streams := []string{}
	for _, call := range remote.snapshotCalls() {
		if strings.HasPrefix(call, "stream ") {
			streams = append(streams, call)
		}
	}
	if len(streams) != 2 || streams[0] != streams[1] {
		t.Fatalf("retry did not reuse the message identity: %v", streams)
	}
}

func TestAmbiguousSendSettlesOnlyWithKagentEvidence(t *testing.T) {
	for _, test := range []struct {
		name  string
		found bool
	}{{"task found terminal", true}, {"no task, session fenced by deletion", false}} {
		t.Run(test.name, func(t *testing.T) {
			remote := newFakeRemote()
			remote.streams = append(remote.streams, func(context.Context, string, string) []item {
				return []item{{err: errors.New("connection reset")}}
			})
			h := newHarness(t, remote)
			o, p := testOwner(1), h.prompt("prompt-1")
			recorder := h.turn(o, p)
			if !strings.Contains(recorder.Body.String(), failUnknown) {
				t.Fatalf("ambiguous send reported %s", recorder.Body.String())
			}
			code, response := h.controlOnce(settlePath, o, p)
			if code != http.StatusConflict || response.SettlementProven {
				t.Fatalf("ambiguous prompt settled without evidence: %d %#v", code, response)
			}
			var messageID string
			for _, call := range remote.snapshotCalls() {
				if strings.HasPrefix(call, "stream ") {
					messageID = strings.TrimPrefix(call, "stream ")
				}
			}
			if test.found {
				remote.setTask(&a2atype.Task{ID: "late-task", Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}, messageID)
			} else {
				time.Sleep(1100 * time.Millisecond) // reconcile backoff
				if code, _ := h.controlOnce(settlePath, o, p); code != http.StatusConflict {
					t.Fatal("not-found settled before kagent's dispatch fence expired")
				}
				h.advance(dispatchFenceWindow + time.Second)
			}
			if !h.control(settlePath, o, p).SettlementProven {
				t.Fatal("prompt did not settle")
			}
			if remote.count("stream ") != 1 {
				t.Fatalf("an ambiguous message was resent: %v", remote.snapshotCalls())
			}
			if deleted := remote.count("delete "); (deleted == 1) == test.found {
				t.Fatalf("session deletion = %d, want only when no task appeared: %v", deleted, remote.snapshotCalls())
			}
		})
	}
}

func TestInputRequiredIsCancelledAndFailsTheTurn(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateInputRequired}}
		remote.setTask(task, messageID)
		return []item{{event: task}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	recorder := h.turn(o, p)
	if !strings.Contains(recorder.Body.String(), failInputRequired) || remote.count("cancel task-1") != 1 {
		t.Fatalf("input-required turn = %s calls %v", recorder.Body.String(), remote.snapshotCalls())
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("cancelled waiting task did not settle")
	}
}

func TestDisconnectCancelsTheRunningKagentTaskBeforeSettlement(t *testing.T) {
	remote := newFakeRemote()
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
		remote.setTask(task, messageID)
		close(started)
		<-ctx.Done()
		return []item{{event: task}, {err: ctx.Err()}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := turnBody()
		h.request(ctx, responsesPath, h.header(responsesPath, o, p, 1, body), body)
	}()
	<-started
	cancel()
	<-done
	if !h.control(settlePath, o, p).SettlementProven || remote.count("cancel task-1") == 0 {
		t.Fatalf("disconnected task was not cancelled before settlement: %v", remote.snapshotCalls())
	}
}

func TestRestartClosesUnsettledPromptsAndRecoversFromKagent(t *testing.T) {
	remote := newFakeRemote()
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, messageID)
		close(started)
		<-ctx.Done()
		return []item{{err: ctx.Err()}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	go func() {
		body := turnBody()
		h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 1, body), body)
	}()
	<-started
	h.restart()
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("recovered prompt did not settle")
	}
	if remote.count("find ") == 0 || remote.count("cancel task-1") == 0 || remote.count("stream ") != 1 {
		t.Fatalf("recovery did not resolve from kagent history without resending: %v", remote.snapshotCalls())
	}
}

func TestUnacknowledgedCreateIsRecoveredByIdempotentRetry(t *testing.T) {
	remote := newFakeRemote()
	remote.createErr = []error{errors.New("unavailable")}
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	if code := h.turn(o, p).Code; code != http.StatusServiceUnavailable {
		t.Fatalf("ambiguous create status = %d", code)
	}
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("prompt with a recovered create did not settle")
	}
	if remote.count("create ") != 2 || remote.count("stream ") != 0 {
		t.Fatalf("create was not retried idempotently: %v", remote.snapshotCalls())
	}
}

func TestRequestsMustCarryAuthorityAndExactContext(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	body := turnBody()
	header := h.header(responsesPath, o, p, 1, body)

	request := httptest.NewRequest(http.MethodPost, responsesPath, strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer wrong")
	request.Header.Set(brokerContextHeader, header)
	recorder := httptest.NewRecorder()
	h.b.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer = %d", recorder.Code)
	}
	if code := h.request(context.Background(), responsesPath, "", body).Code; code != http.StatusBadRequest {
		t.Fatalf("missing context = %d", code)
	}
	if code := h.request(context.Background(), responsesPath, header, []byte(strings.Replace(string(body), "probe tool", "other tool", 1))).Code; code != http.StatusBadRequest {
		t.Fatalf("body digest mismatch = %d", code)
	}
	forged := []byte(`{"model":"other","input":"x","stream":true,"store":true}`)
	if code := h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 1, forged), forged).Code; code != http.StatusBadRequest {
		t.Fatalf("model mismatch = %d", code)
	}
	expired := &promptRef{id: "prompt-2", gen: 1, expires: h.now().Add(-time.Second)}
	if code := h.request(context.Background(), responsesPath, h.header(responsesPath, o, expired, 1, body), body).Code; code != http.StatusGone {
		t.Fatalf("expired lease = %d", code)
	}
	if code := h.request(context.Background(), responsesPath, header, body).Code; code != http.StatusOK {
		t.Fatalf("valid turn = %d", code)
	}
	if code := h.request(context.Background(), responsesPath, header, body).Code; code != http.StatusConflict {
		t.Fatalf("replayed operation = %d", code)
	}
	if code := h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 2, body), body).Code; code != http.StatusConflict {
		t.Fatalf("second round = %d", code)
	}
}

func TestLedgerSurvivesRestartAndRejectsTampering(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	h.turn(o, p)
	h.restart()
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("completed prompt did not settle after restart")
	}
	h.b.mu.Lock()
	for _, s := range h.b.ledger.Sessions {
		s.CreateState = "bogus"
	}
	if err := h.b.commitLocked(false, func(*ledger) error { return nil }); err != nil {
		h.b.mu.Unlock()
		t.Fatal(err)
	}
	h.b.mu.Unlock()
	h.b.close()
	if _, err := newLifecycleBroker(context.Background(), settings{
		bearer: testBearer, configDigest: h.digest, stateDir: h.dir, model: testModel, expectedRevision: testRevision, userIDPrefix: "orka",
	}, remote); err == nil {
		t.Fatal("tampered ledger was accepted")
	}
	h.b = nil
}

func TestLedgerRejectsEmptyState(t *testing.T) {
	for _, state := range []string{"null", "{}"} {
		t.Run(state, func(t *testing.T) {
			remote := newFakeRemote()
			h := newHarness(t, remote)
			h.b.close()
			h.b = nil
			if err := os.WriteFile(filepath.Join(h.dir, "state.json"), []byte(state), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := newLifecycleBroker(context.Background(), settings{
				bearer: testBearer, configDigest: h.digest, stateDir: h.dir, model: testModel, expectedRevision: testRevision, userIDPrefix: "orka",
			}, remote); err == nil {
				t.Fatalf("state %s was accepted as a valid ledger", state)
			}
		})
	}
}

func TestDispatchRequiresTheSessionToStillExist(t *testing.T) {
	for _, test := range []struct {
		name   string
		remove func(*fakeRemote)
	}{{"expired", (*fakeRemote).deleteAll}, {"deleting", (*fakeRemote).markDeleting}} {
		t.Run(test.name, func(t *testing.T) {
			remote := newFakeRemote()
			remote.streams = append(remote.streams, completedStream(remote, "task-1", "one"))
			h := newHarness(t, remote)
			o, first, second := testOwner(1), h.prompt("prompt-1"), h.prompt("prompt-2")
			if h.turn(o, first).Code != http.StatusOK {
				t.Fatal("first turn failed")
			}
			h.control(settlePath, o, first)
			test.remove(remote)
			recorder := h.turn(o, second)
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), failSessionGone) {
				t.Fatalf("turn on a %s session = %d %s", test.name, recorder.Code, recorder.Body.String())
			}
			if !h.control(settlePath, o, second).SettlementProven {
				t.Fatal("refused prompt did not settle")
			}
			// A gone session stays gone across restarts and is never recreated.
			h.restart()
			third := h.prompt("prompt-3")
			if recorder := h.turn(o, third); !strings.Contains(recorder.Body.String(), failSessionGone) {
				t.Fatalf("turn after restart = %d %s", recorder.Code, recorder.Body.String())
			}
			if remote.count("stream ") != 1 || remote.count("create ") != 1 {
				t.Fatalf("a gone session was dispatched to or recreated: %v", remote.snapshotCalls())
			}
			h.control(settlePath, o, third)
			if test.name == "deleting" {
				remote.deleteAll()
			}
			if !h.control(retirePath, o, nil).RetirementProven {
				t.Fatal("gone session did not retire")
			}
		})
	}
}

func TestRevisionIsCheckedBeforeEveryDispatch(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "one"), completedStream(remote, "task-2", "two"))
	h := newHarness(t, remote)
	o, first, second := testOwner(1), h.prompt("prompt-1"), h.prompt("prompt-2")
	if h.turn(o, first).Code != http.StatusOK {
		t.Fatal("first turn failed")
	}
	h.control(settlePath, o, first)
	remote.setRevision(strings.Repeat("1", 64))
	recorder := h.turn(o, second)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), failRevision) {
		t.Fatalf("turn after revision drift = %d %s", recorder.Code, recorder.Body.String())
	}
	if remote.count("stream ") != 1 {
		t.Fatal("a message was dispatched to an unapproved revision")
	}
}

func TestStuckCleanupQuarantinesUntilKagentDeletesTheSession(t *testing.T) {
	remote := newFakeRemote()
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, messageID)
		remote.mu.Lock()
		remote.stuck["task-1"] = true // the agent crashed before settlement
		remote.mu.Unlock()
		close(started)
		<-ctx.Done()
		return []item{{err: ctx.Err()}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := turnBody()
		h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 1, body), body)
	}()
	<-started
	if code, response := h.controlOnce(settlePath, o, p); code != http.StatusConflict || response.SettlementProven {
		t.Fatalf("stuck task settled: %d %#v", code, response)
	}
	<-done
	eventually(t, "cleanup to find the task", func() bool {
		h.controlOnce(settlePath, o, p)
		return remote.count("cancel task-1") > 0
	})
	h.advance(defaultCleanupDeadline + time.Second)
	eventually(t, "quarantine", func() bool {
		_, response := h.controlOnce(settlePath, o, p)
		return response.State == "quarantined" && !response.SettlementProven
	})
	logs := h.logs.String()
	if !strings.Contains(logs, "kagent session quarantined") || !strings.Contains(logs, "task-1") || strings.Contains(logs, "probe tool") {
		t.Fatalf("quarantine log = %s", logs)
	}
	if strings.Count(logs, "kagent session quarantined") != 1 {
		t.Fatalf("quarantine logged more than once: %s", logs)
	}
	if code := h.turn(o, h.prompt("prompt-2")).Code; code != http.StatusGone {
		t.Fatalf("quarantined session admitted a prompt: %d", code)
	}

	// The runbook resolution: deleting the kagent session proves the task
	// can no longer change.
	remote.deleteAll()
	h.advance(quarantineBackoff + time.Second)
	settled := h.control(settlePath, o, p)
	if !settled.SettlementProven || settled.State != "settled" {
		t.Fatalf("settle after deletion = %#v", settled)
	}
	if retired := h.control(retirePath, o, nil); !retired.RetirementProven || retired.State != "retired" {
		t.Fatalf("retire after deletion = %#v", retired)
	}
}

func TestDisconnectedPromptQuarantinesWithoutASettleCall(t *testing.T) {
	remote := newFakeRemote()
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, messageID)
		remote.mu.Lock()
		remote.stuck["task-1"] = true
		remote.mu.Unlock()
		close(started)
		<-ctx.Done()
		return []item{{err: ctx.Err()}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := turnBody()
		h.request(ctx, responsesPath, h.header(responsesPath, o, p, 1, body), body)
	}()
	<-started
	cancel()
	<-done
	// Orka never sends a settle call; the disconnect alone must start the
	// cleanup deadline.
	h.advance(defaultCleanupDeadline + time.Second)
	eventually(t, "quarantine after a disconnect", func() bool { return strings.Contains(h.logs.String(), "kagent session quarantined") })
}

func TestQuarantineEndsWhenCleanupIsProven(t *testing.T) {
	remote := newFakeRemote()
	started := make(chan struct{})
	remote.streams = append(remote.streams, func(ctx context.Context, contextID, messageID string) []item {
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, messageID)
		remote.mu.Lock()
		remote.stuck["task-1"] = true
		remote.mu.Unlock()
		close(started)
		<-ctx.Done()
		return []item{{err: ctx.Err()}}
	}, completedStream(remote, "task-2", "ok"))
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := turnBody()
		h.request(context.Background(), responsesPath, h.header(responsesPath, o, p, 1, body), body)
	}()
	<-started
	h.controlOnce(settlePath, o, p)
	<-done
	h.advance(defaultCleanupDeadline + time.Second)
	eventually(t, "quarantine", func() bool {
		_, response := h.controlOnce(settlePath, o, p)
		return response.State == "quarantined"
	})
	// kagent finally lets the task stop: cleanup proves it and the session
	// takes prompts again.
	remote.mu.Lock()
	remote.stuck["task-1"] = false
	remote.mu.Unlock()
	h.advance(quarantineBackoff + time.Second)
	eventually(t, "settlement", func() bool {
		_, response := h.controlOnce(settlePath, o, p)
		return response.SettlementProven
	})
	if code := h.turn(o, h.prompt("prompt-2")).Code; code != http.StatusOK {
		t.Fatalf("a session whose cleanup was proven refused a prompt: %d", code)
	}
}

func TestRetiredRecordsArePrunedAfterRetention(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	h.turn(o, p)
	h.control(settlePath, o, p)
	h.control(retirePath, o, nil)
	h.advance(defaultRetiredRetention - time.Minute)
	time.Sleep(3 * sweepInterval)
	if h.sessionCount() != 1 {
		t.Fatal("retired record was pruned before its retention ended")
	}
	h.advance(2 * time.Minute)
	eventually(t, "pruning", func() bool { return h.sessionCount() == 0 })
	// A late retried retire still proves retirement and creates nothing.
	if !h.control(retirePath, o, nil).RetirementProven || remote.count("create ") != 1 {
		t.Fatalf("late retire after pruning: %v", remote.snapshotCalls())
	}
}

func TestAbsentTaskIsFencedByDeletionNotTrusted(t *testing.T) {
	remote := newFakeRemote()
	h := newHarness(t, remote)
	userID, requestID := h.b.identity(testOwner(1))
	remoteSession, _ := remote.CreateSession(context.Background(), userID, requestID)
	s := &session{UserID: userID, RemoteID: remoteSession.ID}
	sentAt := h.now().Add(-dispatchFenceWindow + time.Second)
	// The lookup starts inside the fence and finishes after it expires.
	remote.onFind = func() { h.advance(2 * time.Second) }
	inv := &invocation{State: invocationIntent, MessageID: "orka-message", SentAt: sentAt}
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok || remote.count("delete ") != 0 {
		t.Fatalf("a lookup started before the window acted: %s", kind)
	}
	remote.onFind = nil
	inv = &invocation{State: invocationIntent, MessageID: "orka-message", SentAt: sentAt}
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); !ok || kind != proofSessionDeleted || remote.count("delete ") != 1 {
		t.Fatalf("after the window = %s %v, calls %v", kind, ok, remote.snapshotCalls())
	}
}

func newV0Harness(t *testing.T, remote *fakeRemote) *harness {
	t.Helper()
	h := &harness{t: t, remote: remote, dir: filepath.Join(t.TempDir(), "ledger"), digest: config.Digest([]byte("kagent-config")),
		clock: time.Now().UTC(), kagentV0: true}
	remote.v0 = true
	h.open()
	return h
}

// newV0Resolver opens a v0 broker and a kagent session for resolving
// invocations directly.
func newV0Resolver(t *testing.T, governed bool) (*harness, *fakeRemote, *session) {
	t.Helper()
	remote := newFakeRemote()
	remote.v0 = true
	h := &harness{t: t, remote: remote, dir: filepath.Join(t.TempDir(), "ledger"), digest: config.Digest([]byte("kagent-config")),
		clock: time.Now().UTC(), kagentV0: true}
	if governed {
		h.governed = &governedSettings{UpstreamBaseURL: "http://model.orka.svc/v1", UpstreamModel: "gpt-test", UpstreamToken: testUpstreamToken}
	}
	h.open()
	userID, requestID := h.b.identity(testOwner(1))
	remoteSession, _ := remote.CreateSession(context.Background(), userID, requestID)
	return h, remote, &session{UserID: userID, RemoteID: remoteSession.ID}
}

// A v0 listing is only a snapshot, and deleting a v0 session does not stop a
// call already in flight. So an observed send whose task cannot be found
// settles only once every Agent pod that existed at the deletion is gone.
func TestV0ObservedAmbiguousSendSettlesOncePodsAreReplaced(t *testing.T) {
	h, remote, s := newV0Resolver(t, false)
	remote.setPods("pod-a")
	inv := &invocation{State: invocationIntent, MessageID: "orka-message", SentAt: h.now().Add(-dispatchFenceWindow - time.Second)}
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok || inv.FencedAt.IsZero() || remote.count("delete ") != 1 {
		t.Fatalf("first pass = %s %v, fenced %v %v, calls %v", kind, ok, inv.FencedAt, inv.FencedPods, remote.snapshotCalls())
	}
	h.advance(24 * time.Hour)
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok {
		t.Fatalf("settled while the pod that could run the turn remains: %s", kind)
	}
	remote.setPods("pod-a", "pod-b") // a restart in progress: pod-a is terminating
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok {
		t.Fatalf("settled while the old pod terminates: %s", kind)
	}
	remote.setPods("pod-b")
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); !ok || kind != proofPodsReplaced {
		t.Fatalf("after the restart = %s %v", kind, ok)
	}
}

// A governed turn's token dies with the turn, so a late execution can reach
// neither Orka's tools nor its model: deleting the session settles the send.
func TestV0GovernedAmbiguousSendSettlesOnTheRevokedToken(t *testing.T) {
	h, remote, s := newV0Resolver(t, true)
	inv := &invocation{State: invocationIntent, MessageID: "orka-message", SentAt: h.now().Add(-dispatchFenceWindow - time.Second)}
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); !ok || kind != proofTurnTokenRevoked || remote.count("delete ") != 1 {
		t.Fatalf("governed fence = %s %v, calls %v", kind, ok, remote.snapshotCalls())
	}
}

// A session someone else deleted can no longer be listed, so the fence
// records the pods that exist when the broker sees it gone.
func TestV0ExternallyDeletedSessionIsFencedOnItsPods(t *testing.T) {
	h, remote, s := newV0Resolver(t, false)
	remote.setPods("pod-a")
	remote.deleteAll()
	inv := &invocation{State: invocationIntent, MessageID: "orka-message", SentAt: h.now().Add(-dispatchFenceWindow - time.Second)}
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok || len(inv.FencedPods) != 1 {
		t.Fatalf("an externally deleted session = %s %v, fenced pods %v", kind, ok, inv.FencedPods)
	}
	remote.setPods("pod-b")
	if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); !ok || kind != proofPodsReplaced {
		t.Fatalf("after the restart = %s %v", kind, ok)
	}
}

// kagent v0 records a cancellation before its execution has unwound, so a
// canceled v0 task settles only once a cancel call returns.
func TestV0TaskSettlesOnlyWhenACancelReturns(t *testing.T) {
	for _, v0 := range []bool{false, true} {
		remote := newFakeRemote()
		newBroker := newHarness
		if v0 {
			newBroker = newV0Harness
		}
		h := newBroker(t, remote)
		userID, requestID := h.b.identity(testOwner(1))
		remoteSession, _ := remote.CreateSession(context.Background(), userID, requestID)
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: remoteSession.ID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCanceled}}, "orka-message")
		remote.cancelErr = context.DeadlineExceeded
		s := &session{UserID: userID, RemoteID: remoteSession.ID}
		inv := &invocation{State: invocationAccepted, MessageID: "orka-message", TaskID: "task-1"}
		if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); ok == v0 {
			t.Fatalf("v0=%v: a canceled row with a failed cancel settled = %v (%s)", v0, ok, kind)
		}
		remote.cancelErr = nil
		if kind, ok := h.b.resolveInvocation(context.Background(), s, inv); !ok || kind != proofTaskTerminal {
			t.Fatalf("v0=%v: after the cancel returned = %s %v", v0, kind, ok)
		}
	}
}

// kagent v0.10's controller answers a cancel of a finished task with an
// internal error, so a completed v0 turn must settle without one.
func TestV0CompletedTurnSettlesWithoutACancel(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "ok"))
	remote.cancelErr = errors.New("failed to decode response")
	h := newV0Harness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	if recorder := h.turn(o, p); recorder.Code != http.StatusOK {
		t.Fatalf("turn = %d", recorder.Code)
	}
	if !h.control(settlePath, o, p).SettlementProven || remote.count("cancel task-1") != 0 {
		t.Fatalf("a completed v0 turn did not settle on its own: %v", remote.snapshotCalls())
	}
}

// kagent v0 marks tool parts with the ADK's adk_type key, not v1's part type.
func TestV0TurnStreamsToolActivity(t *testing.T) {
	v0Tool := func(kind string, data map[string]any) *a2atype.Part {
		part := a2atype.NewDataPart(data)
		part.SetMeta("adk_type", kind)
		return part
	}
	remote := newFakeRemote()
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		call := v0Tool("function_call", map[string]any{"id": "call-1", "name": "probe", "args": map[string]any{"secret": "x"}})
		result := v0Tool("function_response", map[string]any{"id": "call-1", "name": "probe", "response": map[string]any{"secret": "y"}})
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted},
			Artifacts: []*a2atype.Artifact{{ID: "text", Parts: []*a2atype.Part{call, a2atype.NewTextPart("probe-done")}}}}
		remote.setTask(task, messageID)
		return []item{
			{event: &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}},
			{event: &a2atype.TaskStatusUpdateEvent{TaskID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking,
				Message: a2atype.NewMessage(a2atype.MessageRoleAgent, call)}}},
			{event: &a2atype.TaskStatusUpdateEvent{TaskID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking,
				Message: a2atype.NewMessage(a2atype.MessageRoleAgent, result)}}},
			{event: &a2atype.TaskStatusUpdateEvent{TaskID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}},
		}
	})
	h := newV0Harness(t, remote)
	recorder := h.turn(testOwner(1), h.prompt("prompt-1"))
	got := names(parseSSE(t, recorder.Body.String()))
	want := "response.created,response.kagent_tool.started,response.kagent_tool.finished,response.output_text.delta,response.completed"
	if got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	if strings.Contains(recorder.Body.String(), "secret") {
		t.Fatal("tool arguments or results reached the stream")
	}
}

func TestV0DeletedSessionNeverProvesSettlement(t *testing.T) {
	for _, v0 := range []bool{false, true} {
		remote := newFakeRemote()
		newBroker := newHarness
		if v0 {
			newBroker = newV0Harness
		}
		h := newBroker(t, remote)
		userID, requestID := h.b.identity(testOwner(1))
		remoteSession, _ := remote.CreateSession(context.Background(), userID, requestID)
		remote.setTask(&a2atype.Task{ID: "task-1", ContextID: remoteSession.ID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, "orka-message")
		remote.mu.Lock()
		remote.stuck["task-1"] = true
		remote.mu.Unlock()
		remote.deleteAll()
		s := &session{UserID: userID, RemoteID: remoteSession.ID}
		inv := &invocation{State: invocationAccepted, MessageID: "orka-message", TaskID: "task-1"}
		kind, ok := h.b.resolveInvocation(context.Background(), s, inv)
		if ok == v0 {
			t.Fatalf("v0=%v: a deleted session settled = %v (%s)", v0, ok, kind)
		}
	}
}

func TestEveryRetriedSendMovesTheDispatchFenceBase(t *testing.T) {
	remote := newFakeRemote()
	var h *harness
	notAccepted := a2atype.NewError(a2atype.ErrUnsupportedOperation, "busy").
		WithErrorInfoMeta(map[string]string{"reason": "KAGENT_SEND_NOT_ACCEPTED"})
	var secondAttempt time.Time
	remote.streams = append(remote.streams,
		func(context.Context, string, string) []item {
			h.advance(20 * time.Second) // a slow refusal
			return []item{{err: notAccepted}}
		},
		func(context.Context, string, string) []item {
			secondAttempt = h.now()
			return []item{{err: errors.New("connection reset")}}
		})
	h = newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	if recorder := h.turn(o, p); !strings.Contains(recorder.Body.String(), failUnknown) {
		t.Fatalf("ambiguous retry reported %s", recorder.Body.String())
	}
	h.b.mu.Lock()
	var sentAt time.Time
	for _, s := range h.b.ledger.Sessions {
		for _, prompt := range s.Prompts {
			for _, inv := range prompt.Invocations {
				sentAt = inv.SentAt
			}
		}
	}
	h.b.mu.Unlock()
	if sentAt.IsZero() || sentAt.Before(secondAttempt) {
		t.Fatalf("fence base %s predates the last attempt at %s", sentAt, secondAttempt)
	}
}

func (h *harness) recovery(method, path string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+testBearer)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.b.ServeHTTP(recorder, request)
	return recorder
}

func (h *harness) ledgerIdentity() identityResponse {
	h.t.Helper()
	recorder := h.recovery(http.MethodGet, identityPath, nil)
	var identity identityResponse
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &identity) != nil {
		h.t.Fatalf("identity = %d %s", recorder.Code, recorder.Body.String())
	}
	return identity
}

func bootOf(o owner) owner {
	o.RuntimeSessionUID, o.RuntimeSessionGeneration = "", 0
	return o
}

func TestLedgerIdentityIsStableAcrossRestarts(t *testing.T) {
	h := newHarness(t, newFakeRemote())
	identity := h.ledgerIdentity()
	if identity.Protocol != brokerProtocol || !config.DigestValid(identity.LedgerIdentityDigest) || identity.AgentConfigurationDigest != h.digest {
		t.Fatalf("identity = %#v", identity)
	}
	h.restart()
	if h.ledgerIdentity() != identity {
		t.Fatal("ledger identity changed across a restart")
	}
	if other := newHarness(t, newFakeRemote()); other.ledgerIdentity().LedgerIdentityDigest == identity.LedgerIdentityDigest {
		t.Fatal("two ledgers share an identity")
	}
}

func TestRetireBootSealsAndRetiresOnlyThatBoot(t *testing.T) {
	remote := newFakeRemote()
	remote.streams = append(remote.streams, completedStream(remote, "task-1", "one"), completedStream(remote, "task-2", "two"),
		completedStream(remote, "task-3", "three"))
	h := newHarness(t, remote)
	dead1, dead2 := testOwner(1), testOwner(2)
	live := testOwner(3)
	live.SupervisorBootID = "boot-2"
	for _, o := range []owner{dead1, dead2, live} {
		if h.turn(o, h.prompt("prompt-"+o.RuntimeSessionUID)).Code != http.StatusOK {
			t.Fatal("turn failed")
		}
	}
	identity := h.ledgerIdentity()
	body := func(operation string) []byte {
		raw, _ := json.Marshal(retireBootRequest{Protocol: brokerProtocol, LedgerIdentityDigest: identity.LedgerIdentityDigest,
			AgentConfigurationDigest: identity.AgentConfigurationDigest, RetiredFence: bootOf(dead1), OperationID: operation})
		return raw
	}
	var proof bootRetirementProof
	eventually(t, "boot retirement", func() bool {
		recorder := h.recovery(http.MethodPost, retireBootPath, body("op-1"))
		_ = json.Unmarshal(recorder.Body.Bytes(), &proof)
		return recorder.Code == http.StatusOK
	})
	keys := []string{config.JSONDigest(dead1), config.JSONDigest(dead2)}
	slices.Sort(keys)
	if proof.State != "retired" || !proof.Sealed || !proof.SettlementProven || !proof.RetirementProven || proof.OwnerCount != 2 ||
		proof.OwnerSetDigest != config.JSONDigest(keys) || proof.RetiredFenceDigest != config.JSONDigest(bootOf(dead1)) ||
		proof.ContextSHA256 != config.Digest(body("op-1")) || proof.ProofDigest != proof.canonicalDigest() {
		t.Fatalf("proof = %#v", proof)
	}
	if remote.count("delete ") != 2 {
		t.Fatalf("retirement did not delete exactly the dead boot's sessions: %v", remote.snapshotCalls())
	}
	// The live boot keeps working; the sealed boot admits nothing new.
	h.control(settlePath, live, h.prompt("prompt-"+live.RuntimeSessionUID))
	if h.turn(live, h.prompt("prompt-live-2")).Code != http.StatusOK {
		t.Fatal("the live boot lost admission")
	}
	if code := h.turn(testOwner(4), h.prompt("prompt-late")).Code; code != http.StatusGone {
		t.Fatalf("a new session under the sealed boot was admitted: %d", code)
	}
	// A fresh retry proves the same evidence under its own operation.
	retry := h.recovery(http.MethodPost, retireBootPath, body("op-2"))
	var again bootRetirementProof
	if retry.Code != http.StatusOK || json.Unmarshal(retry.Body.Bytes(), &again) != nil || again.OperationID != "op-2" {
		t.Fatalf("retry = %d %s", retry.Code, retry.Body.String())
	}
	// A refused request leaves no record, so the evidence is unchanged.
	if again.ProofDigest != proof.ProofDigest || again.ContextSHA256 == proof.ContextSHA256 {
		t.Fatalf("retry proof = %#v", again)
	}
	wrong := strings.Replace(string(body("op-3")), identity.LedgerIdentityDigest, config.Digest([]byte("other-ledger")), 1)
	if code := h.recovery(http.MethodPost, retireBootPath, []byte(wrong)).Code; code != http.StatusBadRequest {
		t.Fatalf("a different ledger's request = %d", code)
	}
}

func TestCancelledTurnLearnsItsTaskAndCancelsItByID(t *testing.T) {
	remote := newFakeRemote()
	sent, release := make(chan struct{}), make(chan struct{})
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		close(sent)
		<-release // kagent names the task only after Orka cancels the turn
		task := &a2atype.Task{ID: "task-1", ContextID: contextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
		remote.setTask(task, messageID)
		return []item{{event: task}}
	})
	h := newHarness(t, remote)
	o, p := testOwner(1), h.prompt("prompt-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.turn(o, p)
	}()
	<-sent
	if code, _ := h.controlOnce(settlePath, o, p); code != http.StatusConflict {
		t.Fatalf("settle during the send = %d", code)
	}
	close(release)
	<-done
	if !h.control(settlePath, o, p).SettlementProven {
		t.Fatal("cancelled turn did not settle")
	}
	if remote.count("cancel task-1") == 0 || remote.count("delete ") != 0 {
		t.Fatalf("settlement did not cancel the named task: %v", remote.snapshotCalls())
	}
}
