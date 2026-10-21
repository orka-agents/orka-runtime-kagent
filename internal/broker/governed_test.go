package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

const testUpstreamToken = "orka-model-credential-0123456789"

// fakeOrkaMCP stands in for the supervisor's session MCP server.
type fakeOrkaMCP struct {
	mu    sync.Mutex
	calls []map[string]any
	auth  []string
	hold  chan struct{} // when set, tool calls wait for it to close
}

func (f *fakeOrkaMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var request map[string]any
	_ = json.NewDecoder(r.Body).Decode(&request)
	f.mu.Lock()
	f.calls = append(f.calls, request)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	hold := f.hold
	f.mu.Unlock()
	if hold != nil && request["method"] == "tools/call" {
		<-hold
	}
	w.Header().Set("Content-Type", "application/json")
	result := map[string]any{"tools": []any{map[string]any{"name": "create-work-order", "inputSchema": map[string]any{"type": "object"}}}}
	if request["method"] == "tools/call" {
		result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "work order created"}}, "isError": false}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": result})
}

func (f *fakeOrkaMCP) snapshot() ([]map[string]any, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.calls...), append([]string(nil), f.auth...)
}

// fakeModel stands in for Orka's OpenAI-compatible model endpoint.
type fakeModel struct {
	mu      sync.Mutex
	bodies  []map[string]any
	auth    []string
	holdEOF bool // keep the body open after [DONE] until the request ends
}

func (f *fakeModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	hold := f.holdEOF
	f.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":8,\"prompt_tokens_details\":{\"cached_tokens\":10}}}\n\n"+
		"data: [DONE]\n\n")
	if hold {
		http.NewResponseController(w).Flush() //nolint:errcheck
		<-r.Context().Done()
	}
}

type governedHarness struct {
	*harness
	mcp    *fakeOrkaMCP
	model  *fakeModel
	mcpURL string
}

func newGovernedHarness(t *testing.T, remote *fakeRemote) *governedHarness {
	t.Helper()
	mcp, model := &fakeOrkaMCP{}, &fakeModel{}
	mcpServer, modelServer := httptest.NewServer(mcp), httptest.NewServer(model)
	t.Cleanup(mcpServer.Close)
	t.Cleanup(modelServer.Close)
	h := &harness{t: t, remote: remote, dir: filepath.Join(t.TempDir(), "ledger"), digest: config.Digest([]byte("kagent-config")), clock: time.Now().UTC(),
		governed: &governedSettings{UpstreamBaseURL: modelServer.URL + "/v1", UpstreamModel: "gpt-test", UpstreamToken: testUpstreamToken}}
	h.open()
	return &governedHarness{harness: h, mcp: mcp, model: model, mcpURL: mcpServer.URL + "/mcp/route"}
}

func (g *governedHarness) governedTurn(o owner, p *promptRef) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]any{"model": testModel, "input": "file a work order", "stream": true, "store": true,
		"mcp": map[string]string{"url": g.mcpURL, "authorization": "Bearer orka-session-credential"}})
	return g.request(context.Background(), responsesPath, g.header(responsesPath, o, p, 1, body), body)
}

// relay sends one request to the relay as kagent would, with the turn bearer.
func (g *governedHarness) relay(method, path, bearer, session string, body any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	request := httptest.NewRequest(method, path, strings.NewReader(string(data)))
	if bearer != "" {
		request.Header.Set("Authorization", bearer)
	}
	if session != "" {
		request.Header.Set(mcpSessionHeader, session)
	}
	recorder := httptest.NewRecorder()
	g.b.relayHandler().ServeHTTP(recorder, request)
	return recorder
}

func TestGovernedTurnRunsToolsAndModelThroughOrkaWhileLive(t *testing.T) {
	remote := newFakeRemote()
	var g *governedHarness
	var bearer string
	var toolReply, retryReply, modelReply, foreignUser *httptest.ResponseRecorder
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		bearer = remote.lastAuthorization()
		initialize := g.relay(http.MethodPost, "/mcp", bearer, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize",
			"params": map[string]any{"protocolVersion": "2025-06-18"}})
		session := initialize.Header().Get(mcpSessionHeader)
		call := map[string]any{"jsonrpc": "2.0", "id": 7, "method": "tools/call",
			"params": map[string]any{"name": "create-work-order", "arguments": map[string]any{"asset": "pump-1"}}}
		toolReply = g.relay(http.MethodPost, "/mcp", bearer, session, call)
		retryReply = g.relay(http.MethodPost, "/mcp", bearer, session, call)
		modelReply = g.relay(http.MethodPost, "/v1/chat/completions", bearer, "", map[string]any{"model": "whatever", "stream": true,
			"messages": []any{map[string]any{"role": "user", "content": "hi"}}})
		request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"ping"}`))
		request.Header.Set("Authorization", bearer)
		request.Header.Set(userIDHeader, "someone-else")
		foreignUser = httptest.NewRecorder()
		g.b.relayHandler().ServeHTTP(foreignUser, request)
		return completedStream(remote, "task-1", "done")(context.Background(), contextID, messageID)
	})
	g = newGovernedHarness(t, remote)
	o, p := testOwner(1), g.prompt("prompt-1")
	recorder := g.governedTurn(o, p)
	if recorder.Code != http.StatusOK {
		t.Fatalf("governed turn = %d %s", recorder.Code, recorder.Body.String())
	}
	if !strings.HasPrefix(bearer, "Bearer ") || strings.Contains(recorder.Body.String(), strings.TrimPrefix(bearer, "Bearer ")) {
		t.Fatal("the turn bearer was missing or leaked into the stream")
	}
	if !strings.Contains(toolReply.Body.String(), "work order created") || retryReply.Code != http.StatusOK {
		t.Fatalf("tool reply = %d %s", toolReply.Code, toolReply.Body.String())
	}
	calls, auth := g.mcp.snapshot()
	if len(calls) != 2 || calls[0]["id"] != calls[1]["id"] || !strings.HasPrefix(calls[0]["id"].(string), "kr-") ||
		auth[0] != "Bearer orka-session-credential" {
		t.Fatalf("Orka saw %v with %v", calls, auth)
	}
	if foreignUser.Code != http.StatusUnauthorized {
		t.Fatalf("a request for another kagent user = %d", foreignUser.Code)
	}
	if modelReply.Code != http.StatusOK || !strings.Contains(modelReply.Body.String(), "[DONE]") {
		t.Fatalf("model reply = %d %s", modelReply.Code, modelReply.Body.String())
	}
	g.model.mu.Lock()
	model, modelAuth := g.model.bodies[0], g.model.auth[0]
	g.model.mu.Unlock()
	if model["model"] != "gpt-test" || modelAuth != "Bearer "+testUpstreamToken ||
		model["stream_options"].(map[string]any)["include_usage"] != true {
		t.Fatalf("model upstream saw %v with %q", model, modelAuth)
	}
	events := parseSSE(t, recorder.Body.String())
	usage, _ := events[len(events)-1].data["response"].(map[string]any)["usage"].(map[string]any)
	if usage["input_tokens"] != float64(40) || usage["output_tokens"] != float64(8) {
		t.Fatalf("reported usage = %v", usage)
	}
	// The token dies with the turn.
	if late := g.relay(http.MethodPost, "/mcp", bearer, "", map[string]any{"jsonrpc": "2.0", "id": 9, "method": "ping"}); late.Code != http.StatusUnauthorized {
		t.Fatalf("a finished turn's token = %d", late.Code)
	}
}

func TestGovernedModeRequiresTheOrkaMCPServer(t *testing.T) {
	g := newGovernedHarness(t, newFakeRemote())
	if code := g.turn(testOwner(1), g.prompt("prompt-1")).Code; code != http.StatusBadRequest {
		t.Fatalf("a governed turn without the Orka MCP server = %d", code)
	}
	body, _ := json.Marshal(map[string]any{"model": testModel, "input": "x", "stream": true, "store": true,
		"mcp": map[string]string{"url": "http://orka.example.com/mcp", "authorization": "Bearer x"}})
	if code := g.request(context.Background(), responsesPath, g.header(responsesPath, testOwner(2), g.prompt("prompt-2"), 1, body), body).Code; code != http.StatusBadRequest {
		t.Fatalf("a non-loopback MCP server = %d", code)
	}
	observed := newHarness(t, newFakeRemote())
	body, _ = json.Marshal(map[string]any{"model": testModel, "input": "x", "stream": true, "store": true,
		"mcp": map[string]string{"url": "http://127.0.0.1:1/mcp", "authorization": "Bearer x"}})
	if code := observed.request(context.Background(), responsesPath, observed.header(responsesPath, testOwner(3), observed.prompt("prompt-3"), 1, body), body).Code; code != http.StatusBadRequest {
		t.Fatalf("observed mode accepted an MCP server = %d", code)
	}
	if code := g.relay(http.MethodPost, "/mcp", "Bearer "+strings.Repeat("A", 43), "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}).Code; code != http.StatusUnauthorized {
		t.Fatalf("an unknown token = %d", code)
	}
}

// kagent's Go runtime keeps one MCP session per process and reuses it across
// turns, so a session opened in one turn must work in the next, under that
// turn's own token. Requests without a well-formed session are refused.
func TestRelayMCPSessionsCarryNoAuthority(t *testing.T) {
	remote := newFakeRemote()
	var g *governedHarness
	codes := map[string]int{}
	var session string
	call := map[string]any{"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": map[string]any{"name": "create-work-order"}}
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		bearer := remote.lastAuthorization()
		codes["no session"] = g.relay(http.MethodPost, "/mcp", bearer, "", call).Code
		codes["malformed session"] = g.relay(http.MethodPost, "/mcp", bearer, "not-a-session", call).Code
		codes["stream"] = g.relay(http.MethodGet, "/mcp", bearer, "", nil).Code
		session = g.relay(http.MethodPost, "/mcp", bearer, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize"}).Header().Get(mcpSessionHeader)
		codes["first turn"] = g.relay(http.MethodPost, "/mcp", bearer, session, call).Code
		return completedStream(remote, "task-1", "done")(context.Background(), contextID, messageID)
	}, func(_ context.Context, contextID, messageID string) []item {
		codes["next turn, same session"] = g.relay(http.MethodPost, "/mcp", remote.lastAuthorization(), session, call).Code
		return completedStream(remote, "task-2", "done")(context.Background(), contextID, messageID)
	})
	g = newGovernedHarness(t, remote)
	o := testOwner(1)
	for _, prompt := range []string{"prompt-1", "prompt-2"} {
		p := g.prompt(prompt)
		if recorder := g.governedTurn(o, p); recorder.Code != http.StatusOK {
			t.Fatalf("governed turn %s = %d", prompt, recorder.Code)
		}
		g.control(settlePath, o, p)
	}
	want := map[string]int{"no session": http.StatusBadRequest, "malformed session": http.StatusBadRequest,
		"stream": http.StatusMethodNotAllowed, "first turn": http.StatusOK, "next turn, same session": http.StatusOK}
	for name, code := range want {
		if codes[name] != code {
			t.Fatalf("%s = %d, want %d (all %v)", name, codes[name], code, codes)
		}
	}
	calls, _ := g.mcp.snapshot()
	if len(calls) != 2 || calls[0]["id"] == calls[1]["id"] {
		t.Fatalf("the two turns' calls reached Orka as %v", calls)
	}
}

func TestRelayRefusesRequestsBeyondTheTurnLimit(t *testing.T) {
	remote := newFakeRemote()
	var g *governedHarness
	var refused int
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		bearer := remote.lastAuthorization()
		session := g.relay(http.MethodPost, "/mcp", bearer, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize"}).Header().Get(mcpSessionHeader)
		var wg sync.WaitGroup
		for i := range maxTurnRelayInflight {
			wg.Go(func() {
				g.relay(http.MethodPost, "/mcp", bearer, session, map[string]any{"jsonrpc": "2.0", "id": i + 10, "method": "tools/call",
					"params": map[string]any{"name": "create-work-order"}})
			})
		}
		eventually(t, "held calls", func() bool { calls, _ := g.mcp.snapshot(); return len(calls) == maxTurnRelayInflight })
		refused = g.relay(http.MethodPost, "/v1/chat/completions", bearer, "", map[string]any{"model": "m"}).Code
		close(g.mcp.hold)
		wg.Wait()
		return completedStream(remote, "task-1", "done")(context.Background(), contextID, messageID)
	})
	g = newGovernedHarness(t, remote)
	g.mcp.hold = make(chan struct{})
	if recorder := g.governedTurn(testOwner(1), g.prompt("prompt-1")); recorder.Code != http.StatusOK {
		t.Fatalf("governed turn = %d", recorder.Code)
	}
	if refused != http.StatusTooManyRequests {
		t.Fatalf("a request past the turn limit = %d", refused)
	}
}

func TestRelayAnswersRefusalsOverARealConnection(t *testing.T) {
	g := newGovernedHarness(t, newFakeRemote())
	server := httptest.NewServer(g.b.relayHandler())
	t.Cleanup(server.Close)
	for path, want := range map[string]int{"/mcp": http.StatusUnauthorized, "/v1/chat/completions": http.StatusUnauthorized, "/other": http.StatusNotFound} {
		response, err := http.Post(server.URL+path, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("%s = %d, want %d", path, response.StatusCode, want)
		}
	}
}

func TestCanonicalJSONKeepsEveryIntegerIDDistinct(t *testing.T) {
	first, err := canonicalJSON(json.RawMessage("9007199254740992"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := canonicalJSON(json.RawMessage("9007199254740993"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatalf("distinct IDs share the canonical form %s", first)
	}
	if number, _ := canonicalJSON(json.RawMessage("7")); string(number) != "7" {
		t.Fatalf("canonical 7 = %s", number)
	}
	if text, _ := canonicalJSON(json.RawMessage(`"7"`)); string(text) != `"7"` {
		t.Fatalf(`canonical "7" = %s`, text)
	}
	for _, id := range []string{"1.5", "1e3", "9223372036854775808", "null", "true", "{}"} {
		if _, err := canonicalJSON(json.RawMessage(id)); err == nil {
			t.Fatalf("ID %s was accepted", id)
		}
	}
}

func TestModelRelayHandlesNullAndInvalidStreamOptions(t *testing.T) {
	remote := newFakeRemote()
	var g *governedHarness
	var nullOptions, badOptions *httptest.ResponseRecorder
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		bearer := remote.lastAuthorization()
		messages := []any{map[string]any{"role": "user", "content": "hi"}}
		nullOptions = g.relay(http.MethodPost, "/v1/chat/completions", bearer, "", map[string]any{"stream": true, "stream_options": nil, "messages": messages})
		badOptions = g.relay(http.MethodPost, "/v1/chat/completions", bearer, "", map[string]any{"stream": true, "stream_options": 5, "messages": messages})
		return completedStream(remote, "task-1", "done")(context.Background(), contextID, messageID)
	})
	g = newGovernedHarness(t, remote)
	if recorder := g.governedTurn(testOwner(1), g.prompt("prompt-1")); recorder.Code != http.StatusOK {
		t.Fatalf("governed turn = %d %s", recorder.Code, recorder.Body.String())
	}
	if nullOptions.Code != http.StatusOK || badOptions.Code != http.StatusBadRequest {
		t.Fatalf("null stream_options = %d, invalid stream_options = %d", nullOptions.Code, badOptions.Code)
	}
	g.model.mu.Lock()
	defer g.model.mu.Unlock()
	if len(g.model.bodies) != 1 || g.model.bodies[0]["stream_options"].(map[string]any)["include_usage"] != true {
		t.Fatalf("model upstream saw %v", g.model.bodies)
	}
}

// failingBody returns its data, then fails as a reset connection would.
type failingBody struct{ data *strings.Reader }

func (f failingBody) Read(p []byte) (int, error) {
	if f.data.Len() == 0 {
		return 0, errors.New("connection reset by peer")
	}
	return f.data.Read(p)
}

// endlessLines is an upstream body that never ends.
type endlessLines struct{}

func (endlessLines) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
		if i%1024 == 1023 {
			p[i] = '\n'
		}
	}
	return len(p), nil
}

func TestRelayCopyReportsTruncatedUpstreamBodies(t *testing.T) {
	complete := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n"
	recorder := httptest.NewRecorder()
	if usage, err := relayCopy(recorder, strings.NewReader(complete), true); err != nil || usage.Input != 4 || recorder.Body.String() != complete {
		t.Fatalf("complete body: usage %+v, err %v, forwarded %q", usage, err, recorder.Body.String())
	}
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	if _, err := relayCopy(httptest.NewRecorder(), failingBody{strings.NewReader(partial)}, true); err == nil {
		t.Fatal("an upstream error between complete events looked like a complete response")
	}
	recorder = httptest.NewRecorder()
	if _, err := relayCopy(recorder, endlessLines{}, true); !errors.Is(err, errRelayResponseTooLarge) || recorder.Body.Len() > relayMaxResponse {
		t.Fatalf("oversized body: err %v after forwarding %d bytes", err, recorder.Body.Len())
	}
}

// kagent can finish a turn as soon as it reads [DONE], while the relay still
// waits for the upstream body to end. The turn must still count that call.
func TestTurnCountsUsageOfAModelCallOpenAtItsEnd(t *testing.T) {
	remote := newFakeRemote()
	var relayURL string
	var open []io.Closer
	t.Cleanup(func() {
		for _, body := range open {
			_ = body.Close()
		}
	})
	remote.streams = append(remote.streams, func(_ context.Context, contextID, messageID string) []item {
		request, _ := http.NewRequest(http.MethodPost, relayURL+"/v1/chat/completions",
			strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
		request.Header.Set("Authorization", remote.lastAuthorization())
		if response, err := http.DefaultClient.Do(request); err == nil {
			open = append(open, response.Body)
			for reader := bufio.NewReader(response.Body); ; {
				line, err := reader.ReadString('\n')
				if strings.Contains(line, "[DONE]") || err != nil {
					break
				}
			}
		}
		return completedStream(remote, "task-1", "done")(context.Background(), contextID, messageID)
	})
	g := newGovernedHarness(t, remote)
	g.model.mu.Lock()
	g.model.holdEOF = true
	g.model.mu.Unlock()
	server := httptest.NewServer(g.b.relayHandler())
	t.Cleanup(server.Close)
	relayURL = server.URL
	recorder := g.governedTurn(testOwner(1), g.prompt("prompt-1"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("governed turn = %d %s", recorder.Code, recorder.Body.String())
	}
	events := parseSSE(t, recorder.Body.String())
	usage, _ := events[len(events)-1].data["response"].(map[string]any)["usage"].(map[string]any)
	if usage["input_tokens"] != float64(40) || usage["output_tokens"] != float64(8) {
		t.Fatalf("reported usage = %v", usage)
	}
}
