package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

const (
	testModel  = "kagent/kagent/probe"
	testToken  = "test-only-proxy-token"
	testConfig = `{"model":"kagent/kagent/probe","kagent":{"controlPlaneURL":"http://kagent-controller.kagent.svc:8083","agent":{"namespace":"kagent","name":"probe"},"expectedRevision":"c85fb79c49c56d3720b2744f8e330d1cbe5f18fe84ad407f060043677716806f","userIDPrefix":"orka"}}`
)

type peer struct {
	t       *testing.T
	in      *io.PipeWriter
	decoder *json.Decoder
	done    chan error
	events  []map[string]any
	mu      sync.Mutex
	session string
	id      int
}

func newPeer(t *testing.T, provider http.HandlerFunc, mcpURL string) *peer {
	t.Helper()
	return newPeerWithConfig(t, provider, mcpURL, testConfig)
}

func newPeerWithConfig(t *testing.T, provider http.HandlerFunc, mcpURL, agentConfig string) *peer {
	t.Helper()
	server := httptest.NewServer(provider)
	t.Cleanup(server.Close)
	path := filepath.Join(t.TempDir(), "kagent.json")
	if err := os.WriteFile(path, []byte(agentConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		config.ModelEnv: testModel, config.ConfigDigestEnv: config.Digest([]byte(agentConfig)),
		providerBaseEnv: server.URL + "/_orka/provider/route/v1", providerTokenEnv: testToken,
	}
	cfg, err := loadConfiguration(path, func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	in, input := io.Pipe()
	output, out := io.Pipe()
	p := &peer{t: t, in: input, decoder: json.NewDecoder(output), done: make(chan error, 1)}
	go func() { p.done <- serveACP(context.Background(), cfg, in, out) }()
	t.Cleanup(func() {
		_ = input.Close()
		_ = output.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("ACP child did not exit after transport close")
		}
	})
	result := p.call("initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})["result"].(map[string]any)
	caps := result["agentCapabilities"].(map[string]any)
	if caps["loadSession"] != false || caps["promptCapabilities"].(map[string]any)["image"] != false {
		t.Fatal("ACP capabilities claim unsupported functionality")
	}
	if mcpURL == "" {
		mcpURL = "http://127.0.0.1:1/_orka/mcp/route"
	}
	cwd, _ := os.Getwd()
	response := p.call("session/new", map[string]any{"cwd": cwd, "mcpServers": []map[string]any{{
		"type": "http", "name": "orka", "url": mcpURL,
		"headers": []map[string]string{{"name": "Authorization", "value": "Bearer mcp-token"}},
	}}})
	if result, ok := response["result"].(map[string]any); ok {
		p.session, _ = result["sessionId"].(string)
	}
	return p
}

func (p *peer) send(message any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := json.NewEncoder(p.in).Encode(message); err != nil {
		p.t.Fatal("ACP request write failed")
	}
}

func (p *peer) read() map[string]any {
	p.t.Helper()
	done := make(chan map[string]any, 1)
	go func() {
		var value map[string]any
		if p.decoder.Decode(&value) != nil {
			value = nil
		}
		done <- value
	}()
	select {
	case value := <-done:
		if value == nil {
			p.t.Fatal("ACP response read failed")
		}
		if value["method"] == "session/update" {
			params := value["params"].(map[string]any)
			p.events = append(p.events, params["update"].(map[string]any))
		}
		return value
	case <-time.After(5 * time.Second):
		p.t.Fatal("ACP response did not arrive")
		return nil
	}
}

func (p *peer) start(method string, params any) int {
	p.id++
	p.send(map[string]any{"jsonrpc": "2.0", "id": p.id, "method": method, "params": params})
	return p.id
}

func (p *peer) reply(id int) map[string]any {
	p.t.Helper()
	for {
		message := p.read()
		if message["id"] == float64(id) {
			return message
		}
		if message["method"] != "session/update" {
			p.t.Fatalf("unexpected ACP message %v", message)
		}
	}
}

func (p *peer) call(method string, params any) map[string]any {
	p.t.Helper()
	return p.reply(p.start(method, params))
}

func (p *peer) prompt(text string) int {
	return p.start("session/prompt", map[string]any{"sessionId": p.session, "prompt": []map[string]string{{"type": "text", "text": text}}})
}

func sse(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		var probe struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal([]byte(event), &probe)
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", probe.Type, event)
	}
}

const (
	created   = `{"type":"response.created","response":{"id":"kr_1","status":"in_progress"}}`
	completed = `{"type":"response.completed","response":{"id":"kr_1","status":"completed"}}`
	toolID    = "kt_123e4567-e89b-12d3-a456-426614174000"
)

func TestPromptRelaysObservedToolsAndTextThenEndsTurn(t *testing.T) {
	var body map[string]any
	p := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_orka/provider/route/v1/responses" || r.Header.Get("Authorization") != "Bearer "+testToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		sse(w, created,
			`{"type":"response.kagent_tool.started","tool":{"id":"`+toolID+`","name":"probe"}}`,
			`{"type":"response.kagent_tool.finished","tool":{"id":"`+toolID+`","name":"probe","status":"completed"}}`,
			`{"type":"response.output_text.delta","delta":"probe-done"}`,
			completed)
	}, "")
	if !strings.HasPrefix(p.session, "kagent-") {
		t.Fatalf("session id %q", p.session)
	}
	response := p.reply(p.prompt("hello"))
	if response["result"].(map[string]any)["stopReason"] != "end_turn" {
		t.Fatalf("prompt = %v", response)
	}
	if body["model"] != testModel || body["input"] != "hello" || body["stream"] != true || body["store"] != true || len(body) != 4 {
		t.Fatalf("request body = %v", body)
	}
	got := []string{}
	for _, event := range p.events {
		got = append(got, fmt.Sprint(event["sessionUpdate"], ":", event["status"]))
	}
	if strings.Join(got, ",") != "tool_call:in_progress,tool_call_update:completed,agent_message_chunk:<nil>" {
		t.Fatalf("updates = %v", got)
	}
}

func TestBrokerFailureCodeSurfacesAsPromptError(t *testing.T) {
	p := newPeer(t, func(w http.ResponseWriter, _ *http.Request) {
		sse(w, created, `{"type":"response.failed","response":{"id":"kr_1","status":"failed","error":{"code":"kagent_input_required","message":"paused"}}}`)
	}, "")
	response := p.reply(p.prompt("hello"))
	failure, _ := response["error"].(map[string]any)
	data, _ := failure["data"].(map[string]any)
	if failure == nil || failure["message"] != "kagent ACP prompt failed: kagent_input_required" ||
		data["service"] != "kagent" || data["errorName"] != "kagent_input_required" {
		t.Fatalf("prompt = %v", response)
	}
	// A failed turn does not poison the session: kagent owns the conversation.
	if response := p.reply(p.prompt("again")); response["error"] == nil {
		t.Fatal("second prompt unexpectedly succeeded against a failing provider")
	} else if response["error"].(map[string]any)["code"] == float64(-32602) {
		t.Fatal("session was poisoned by a failed turn")
	}
}

func TestMalformedOrTruncatedStreamsFail(t *testing.T) {
	for name, events := range map[string][]string{
		"no terminal event":   {created, `{"type":"response.output_text.delta","delta":"partial"}`},
		"unknown event":       {created, `{"type":"response.function_call","name":"x"}`},
		"unstarted tool":      {created, `{"type":"response.kagent_tool.finished","tool":{"id":"` + toolID + `","name":"probe","status":"completed"}}`, completed},
		"unsafe tool name":    {created, `{"type":"response.kagent_tool.started","tool":{"id":"` + toolID + `","name":"bad name"}}`, completed},
		"duplicate field":     {created, `{"type":"response.output_text.delta","delta":"a","delta":"b"}`, completed},
		"unsafe failure code": {created, `{"type":"response.failed","response":{"id":"kr_1","status":"failed","error":{"code":"Bad Code","message":"x"}}}`},
	} {
		t.Run(name, func(t *testing.T) {
			p := newPeer(t, func(w http.ResponseWriter, _ *http.Request) { sse(w, events...) }, "")
			response := p.reply(p.prompt("hello"))
			if response["error"] == nil {
				t.Fatalf("%s was accepted: %v", name, response)
			}
		})
	}
}

func TestCancelEndsTheTurnAndClosesTheRequest(t *testing.T) {
	closed := make(chan struct{})
	p := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, created)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}, "")
	id := p.prompt("hello")
	time.Sleep(100 * time.Millisecond)
	p.send(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": p.session}})
	response := p.reply(id)
	if response["result"].(map[string]any)["stopReason"] != "cancelled" {
		t.Fatalf("cancel = %v", response)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("provider request stayed open after cancel")
	}
}

func TestSessionRequiresOrkaLoopbackMCPDescriptor(t *testing.T) {
	p := newPeer(t, func(http.ResponseWriter, *http.Request) {}, "http://example.com/mcp")
	if p.session != "" {
		t.Fatal("non-loopback MCP descriptor was accepted")
	}
}

func TestGovernedPromptHandsTheOrkaMCPServerToTheBroker(t *testing.T) {
	var body map[string]any
	governed := strings.Replace(testConfig, `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc:8080/v1","upstreamModel":"gpt-test"},"kagent":{`, 1)
	p := newPeerWithConfig(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		sse(w, created, `{"type":"response.output_text.delta","delta":"done"}`,
			`{"type":"response.completed","response":{"id":"kr_1","status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}`)
	}, "http://127.0.0.1:9/_orka/mcp/route", governed)
	response := p.reply(p.prompt("hello"))
	if response["result"].(map[string]any)["stopReason"] != "end_turn" {
		t.Fatalf("prompt = %v", response)
	}
	mcp, _ := body["mcp"].(map[string]any)
	if mcp["url"] != "http://127.0.0.1:9/_orka/mcp/route" || mcp["authorization"] != "Bearer mcp-token" {
		t.Fatalf("governed request body = %v", body)
	}
}

func TestGovernedSessionRequiresWhatTheBrokerRelays(t *testing.T) {
	governed := strings.Replace(testConfig, `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc:8080/v1","upstreamModel":"gpt-test"},"kagent":{`, 1)
	for _, url := range []string{"http://localhost:9/_orka/mcp/route", "https://127.0.0.1:9/_orka/mcp/route", "http://127.0.0.1/_orka/mcp/route"} {
		if p := newPeerWithConfig(t, func(http.ResponseWriter, *http.Request) {}, url, governed); p.session != "" {
			t.Fatalf("governed session accepted MCP descriptor %s, which the broker refuses", url)
		}
	}
	// Observed sessions never hand the server to the broker.
	if p := newPeer(t, func(http.ResponseWriter, *http.Request) {}, "http://localhost:9/_orka/mcp/route"); p.session == "" {
		t.Fatal("observed session refused a loopback host name")
	}
}

// JSON escaping can make a prompt within the text limit encode past the
// broker's request limit; the child refuses it as invalid instead of letting
// the broker answer an anonymous 400.
func TestOversizedEncodedPromptIsInvalid(t *testing.T) {
	called := false
	p := newPeer(t, func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusBadRequest)
	}, "")
	failure, _ := p.reply(p.prompt(strings.Repeat("<", 1<<20)))["error"].(map[string]any)
	if failure["code"] != float64(-32602) || called {
		t.Fatalf("oversized prompt = %v, broker called %v", failure, called)
	}
}

func TestBrokerRefusalSurfacesItsCode(t *testing.T) {
	p := newPeer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"kagent_revision_mismatch"}`))
	}, "")
	failure, _ := p.reply(p.prompt("hello"))["error"].(map[string]any)
	data, _ := failure["data"].(map[string]any)
	if data["errorName"] != "kagent_revision_mismatch" {
		t.Fatalf("refusal = %v", failure)
	}
	p2 := newPeer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"Free text with spaces"}`))
	}, "")
	if failure, _ := p2.reply(p2.prompt("hello"))["error"].(map[string]any); failure["data"] != nil {
		t.Fatalf("free text reached the error data: %v", failure)
	}
}
