package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

// Governed mode: the approved revision points every MCP binding and model
// endpoint at this relay. kagent forwards the A2A caller's bearer to both,
// and the broker issues that bearer per turn, so a relayed call is accepted
// only while the turn it belongs to is live.
const (
	relayMaxBody     = 8 << 20
	relayMaxResponse = 32 << 20
	// Orka holds an approval-gated tool call for up to 900 seconds; the relay
	// waits longer, and the RemoteMCPServer timeout must be longer still.
	relayToolTimeout  = 20 * time.Minute
	relayModelTimeout = 10 * time.Minute
	maxRelaySessions  = 32
	// In-flight relay requests are bounded before any body is read; excess
	// requests are refused, never queued.
	maxRelayInflight     = 64
	maxTurnRelayInflight = 8
	// A downstream write that stalls this long fails; the turn's end fails
	// it at once. Approval waits happen before any write.
	relayWriteStall  = 30 * time.Second
	turnTokenBytes   = 32
	mcpSessionHeader = "Mcp-Session-Id"
	userIDHeader     = "X-User-Id"
	jsonRPCVersion   = "2.0"
	fieldName        = "name"
)

var mcpProtocolVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

// turnBinding is the live authority behind one turn token. It exists only in
// memory: a restarted broker invalidates every token along with every turn.
type turnBinding struct {
	promptKey string
	userID    string
	ctx       context.Context
	mcp       relayMCP

	slots chan struct{}

	mu       sync.Mutex
	sessions map[string]bool
	usage    tokenUsage
}

func newTurnBinding(promptKey, userID string, ctx context.Context, mcp relayMCP) *turnBinding {
	return &turnBinding{promptKey: promptKey, userID: userID, ctx: ctx, mcp: mcp,
		slots: make(chan struct{}, maxTurnRelayInflight), sessions: map[string]bool{}}
}

// admitRelay reserves a turn slot and a broker slot, or refuses the request.
func (b *lifecycleBroker) admitRelay(turn *turnBinding) (func(), bool) {
	select {
	case turn.slots <- struct{}{}:
	default:
		return nil, false
	}
	select {
	case b.relaySlots <- struct{}{}:
	default:
		<-turn.slots
		return nil, false
	}
	return func() { <-b.relaySlots; <-turn.slots }, true
}

// relayWriter bounds every downstream write and fails a blocked one as soon
// as the turn ends.
type relayWriter struct {
	http.ResponseWriter
	control *http.ResponseController
}

func guardWrites(w http.ResponseWriter, turn context.Context) (*relayWriter, func()) {
	guarded := &relayWriter{ResponseWriter: w, control: http.NewResponseController(w)}
	stop := context.AfterFunc(turn, func() { _ = guarded.control.SetWriteDeadline(time.Now()) })
	return guarded, func() { stop() }
}

func (w *relayWriter) WriteHeader(status int) {
	_ = w.control.SetWriteDeadline(time.Now().Add(relayWriteStall))
	w.ResponseWriter.WriteHeader(status)
}

func (w *relayWriter) Write(p []byte) (int, error) {
	_ = w.control.SetWriteDeadline(time.Now().Add(relayWriteStall))
	return w.ResponseWriter.Write(p)
}

func (w *relayWriter) Flush() {
	_ = w.control.SetWriteDeadline(time.Now().Add(relayWriteStall))
	_ = w.control.Flush()
}

// relayMCP is the Orka session MCP server the ACP child was given. The child
// passes it with each turn; it never leaves the pod.
type relayMCP struct {
	URL           string `json:"url"`
	Authorization string `json:"authorization"`
}

type tokenUsage struct {
	Input, Output, Cached uint64
	reported, cached      bool
}

// valid accepts only the pod-local Orka MCP server with a bearer header.
func (m relayMCP) valid() bool { return config.SessionMCPServer(m.URL, m.Authorization) }

func newTurnToken() (token, digest string, err error) {
	raw := make([]byte, turnTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	return token, config.Digest([]byte(token)), nil
}

func (b *lifecycleBroker) bindTurn(digest string, binding *turnBinding) {
	b.turnMu.Lock()
	defer b.turnMu.Unlock()
	b.turns[digest] = binding
}

func (b *lifecycleBroker) unbindTurn(digest string) {
	b.turnMu.Lock()
	defer b.turnMu.Unlock()
	delete(b.turns, digest)
}

// turnFor authenticates a relayed request by its turn token, and by kagent's
// user header when present.
func (b *lifecycleBroker) turnFor(r *http.Request) *turnBinding {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return nil
	}
	token, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok || len(token) != base64.RawURLEncoding.EncodedLen(turnTokenBytes) {
		return nil
	}
	b.turnMu.Lock()
	binding := b.turns[config.Digest([]byte(token))]
	b.turnMu.Unlock()
	if binding == nil || binding.ctx.Err() != nil {
		return nil
	}
	if users := r.Header.Values(userIDHeader); len(users) > 1 || (len(users) == 1 && users[0] != binding.userID) {
		return nil
	}
	return binding
}

func (t *turnBinding) addSession() (string, bool) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.sessions) >= maxRelaySessions {
		return "", false
	}
	id := hex.EncodeToString(raw)
	t.sessions[id] = true
	return id, true
}

func (t *turnBinding) hasSession(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return id != "" && t.sessions[id]
}

func (t *turnBinding) closeSession(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.sessions, id)
}

func (t *turnBinding) addUsage(u tokenUsage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usage.Input += u.Input
	t.usage.Output += u.Output
	t.usage.Cached += u.Cached
	t.usage.reported = t.usage.reported || u.reported
	t.usage.cached = t.usage.cached || u.cached
}

func (t *turnBinding) totalUsage() tokenUsage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.usage
}

// relayContext bounds a relayed call by the request, the turn, and limit.
func relayContext(r *http.Request, turn context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), limit)
	stop := context.AfterFunc(turn, cancel)
	return ctx, func() { stop(); cancel() }
}

func (b *lifecycleBroker) relayHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", b.serveMCPRelay)
	mux.HandleFunc("/v1/chat/completions", b.serveModelRelay)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every reply, refusals included, gets a bounded write. Authenticated
		// calls renew the deadline before each write, after any approval wait.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(relayWriteStall))
		mux.ServeHTTP(w, r)
	})
}

type relayRPC struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (b *lifecycleBroker) serveMCPRelay(response http.ResponseWriter, r *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	turn := b.turnFor(r)
	if turn == nil {
		response.WriteHeader(http.StatusUnauthorized)
		return
	}
	release, ok := b.admitRelay(turn)
	if !ok {
		response.WriteHeader(http.StatusTooManyRequests)
		return
	}
	defer release()
	w, stop := guardWrites(response, turn.ctx)
	defer stop()
	switch r.Method {
	case http.MethodPost:
	case http.MethodDelete:
		turn.closeSession(r.Header.Get(mcpSessionHeader))
		w.WriteHeader(http.StatusNoContent)
		return
	default:
		// No server-initiated stream: every response is a JSON body.
		w.Header().Set("Allow", "POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, relayMaxBody+1))
	var rpc relayRPC
	if err != nil || len(body) > relayMaxBody || strictjson.Decode(body, &rpc, false) != nil || rpc.JSONRPC != jsonRPCVersion || rpc.Method == "" {
		writeRPC(w, nil, nil, rpcError(-32600, "invalid MCP request"))
		return
	}
	if len(rpc.ID) == 0 || bytes.Equal(rpc.ID, []byte("null")) {
		// Notifications need no answer.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	switch rpc.Method {
	case "initialize":
		b.initializeRelay(w, turn, rpc)
	case "ping":
		writeRPC(w, rpc.ID, map[string]any{}, nil)
	case "tools/list", "tools/call":
		if !turn.hasSession(r.Header.Get(mcpSessionHeader)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx, cancel := relayContext(r, turn.ctx, relayToolTimeout)
		defer cancel()
		result, rpcErr := b.forwardMCP(ctx, turn, r.Header.Get(mcpSessionHeader), rpc)
		writeRPC(w, rpc.ID, result, rpcErr)
	default:
		writeRPC(w, rpc.ID, nil, rpcError(-32601, "MCP method is not supported"))
	}
}

func (b *lifecycleBroker) initializeRelay(w http.ResponseWriter, turn *turnBinding, rpc relayRPC) {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(rpc.Params, &params)
	version := mcpProtocolVersions[0]
	if slices.Contains(mcpProtocolVersions, params.ProtocolVersion) {
		version = params.ProtocolVersion
	}
	id, ok := turn.addSession()
	if !ok {
		writeRPC(w, rpc.ID, nil, rpcError(-32003, "MCP relay session limit reached"))
		return
	}
	w.Header().Set(mcpSessionHeader, id)
	writeRPC(w, rpc.ID, map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
		"serverInfo":      map[string]string{fieldName: "orka-kagent-relay", "version": "v1"},
	}, nil)
}

// forwardMCP sends one call to the Orka session MCP server. A tool call's ID
// is derived from the turn, relay session, and kagent request ID, so a
// retried request maps to the same Orka operation and never runs twice.
func (b *lifecycleBroker) forwardMCP(ctx context.Context, turn *turnBinding, session string, rpc relayRPC) (json.RawMessage, *rpcErr) {
	request := map[string]any{"jsonrpc": jsonRPCVersion, "method": rpc.Method}
	if rpc.Method == "tools/call" {
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments,omitempty"`
		}
		if json.Unmarshal(rpc.Params, &params) != nil || !config.SafeString(params.Name, 256) {
			return nil, rpcError(-32602, "invalid MCP tool call parameters")
		}
		canonical, err := canonicalJSON(rpc.ID)
		if err != nil {
			return nil, rpcError(-32600, "invalid MCP request ID")
		}
		sum := config.Digest([]byte(turn.promptKey + "\n" + session + "\n" + string(canonical)))
		request["id"] = "kr-" + sum[len("sha256:"):][:40]
		call := map[string]any{fieldName: params.Name}
		if len(params.Arguments) != 0 {
			call["arguments"] = params.Arguments
		}
		request["params"] = call
	} else {
		request["id"] = "kr-list"
	}
	data, _ := json.Marshal(request)
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, turn.mcp.URL, bytes.NewReader(data))
	if err != nil {
		return nil, rpcError(-32603, "Orka tool broker is unavailable")
	}
	upstream.Header.Set("Authorization", turn.mcp.Authorization)
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("Accept", "application/json")
	response, err := b.relayClient.Do(upstream)
	if err != nil {
		return nil, rpcError(-32603, "Orka tool broker is unavailable")
	}
	defer response.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(io.LimitReader(response.Body, relayMaxResponse+1))
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err != nil || len(raw) > relayMaxResponse || response.StatusCode != http.StatusOK || json.Unmarshal(raw, &reply) != nil ||
		(reply.Error == nil) == (len(reply.Result) == 0) {
		return nil, rpcError(-32603, "Orka tool broker is unavailable")
	}
	return reply.Result, reply.Error
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func rpcError(code int, message string) *rpcErr { return &rpcErr{Code: code, Message: message} }

func writeRPC(w http.ResponseWriter, id json.RawMessage, result any, failure *rpcErr) {
	response := map[string]any{"jsonrpc": jsonRPCVersion, "id": json.RawMessage("null")}
	if len(id) != 0 {
		response["id"] = id
	}
	if failure != nil {
		response["error"] = failure
	} else {
		response["result"] = result
	}
	writeJSON(w, http.StatusOK, response)
}

// serveModelRelay forwards an OpenAI chat completion to Orka's model
// endpoint with Orka's credential, pins the model, and totals the usage.
func (b *lifecycleBroker) serveModelRelay(downstream http.ResponseWriter, r *http.Request) {
	downstream.Header().Set("Cache-Control", "no-store")
	turn := b.turnFor(r)
	if turn == nil {
		downstream.WriteHeader(http.StatusUnauthorized)
		return
	}
	release, ok := b.admitRelay(turn)
	if !ok {
		downstream.WriteHeader(http.StatusTooManyRequests)
		return
	}
	defer release()
	w, stop := guardWrites(downstream, turn.ctx)
	defer stop()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, relayMaxBody+1))
	var payload map[string]json.RawMessage
	if err != nil || len(body) > relayMaxBody || strictjson.Decode(body, &payload, false) != nil || payload == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	payload["model"], _ = json.Marshal(b.cfg.governed.UpstreamModel)
	var stream bool
	_ = json.Unmarshal(payload["stream"], &stream)
	if stream {
		var options map[string]json.RawMessage
		if raw, ok := payload["stream_options"]; ok && json.Unmarshal(raw, &options) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if options == nil {
			options = map[string]json.RawMessage{}
		}
		options["include_usage"] = json.RawMessage("true")
		payload["stream_options"], _ = json.Marshal(options)
	}
	data, _ := json.Marshal(payload)
	ctx, cancel := relayContext(r, turn.ctx, relayModelTimeout)
	defer cancel()
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.governed.UpstreamBaseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	upstream.Header.Set("Authorization", "Bearer "+b.cfg.governed.UpstreamToken)
	upstream.Header.Set("Content-Type", "application/json")
	response, err := b.relayClient.Do(upstream)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer response.Body.Close() //nolint:errcheck
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(response.StatusCode)
	usage := relayCopy(w, response.Body, response.StatusCode == http.StatusOK)
	turn.addUsage(usage)
}

// relayCopy streams the upstream body and reads usage from each complete
// JSON chunk or body. It never forwards more than relayMaxResponse bytes.
func relayCopy(w http.ResponseWriter, body io.Reader, success bool) tokenUsage {
	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReaderSize(io.LimitReader(body, relayMaxResponse), 64<<10)
	var usage tokenUsage
	var whole bytes.Buffer
	for {
		line, err := reader.ReadSlice('\n')
		if len(line) > 0 {
			if _, writeErr := w.Write(line); writeErr != nil {
				return usage
			}
			if flusher != nil && bytes.HasSuffix(line, []byte("\n")) {
				flusher.Flush()
			}
			if success {
				if data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte("data:")); ok {
					if u, ok := chunkUsage(bytes.TrimSpace(data)); ok {
						usage = u // the final usage chunk carries the totals
					}
				} else if whole.Len()+len(line) <= relayMaxBody {
					whole.Write(line)
				}
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			break
		}
	}
	if !usage.reported && success {
		if u, ok := chunkUsage(bytes.TrimSpace(whole.Bytes())); ok {
			usage = u
		}
	}
	return usage
}

func chunkUsage(data []byte) (tokenUsage, bool) {
	var chunk struct {
		Usage *struct {
			PromptTokens        uint64 `json:"prompt_tokens"`
			CompletionTokens    uint64 `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens *uint64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &chunk) != nil || chunk.Usage == nil {
		return tokenUsage{}, false
	}
	u := tokenUsage{Input: chunk.Usage.PromptTokens, Output: chunk.Usage.CompletionTokens, reported: true}
	if details := chunk.Usage.PromptTokensDetails; details != nil && details.CachedTokens != nil && *details.CachedTokens <= u.Input {
		u.Cached, u.cached = *details.CachedTokens, true
	}
	return u, true
}

// canonicalJSON re-encodes a JSON-RPC ID so equal IDs digest equally.
// Numeric IDs must be integers that fit in int64: decoding them as float64
// would give distinct IDs above 2^53 the same digest.
func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	switch id := value.(type) {
	case string:
		return json.Marshal(id)
	case json.Number:
		n, err := strconv.ParseInt(id.String(), 10, 64)
		if err != nil {
			return nil, errors.New("JSON-RPC numeric ID must be an int64 integer")
		}
		return strconv.AppendInt(nil, n, 10), nil
	default:
		return nil, errors.New("JSON-RPC ID must be a string or number")
	}
}
