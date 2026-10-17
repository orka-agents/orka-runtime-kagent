package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

const (
	// The broker sends at most 4 MiB of text, and JSON may spell each byte of
	// it as a six-byte \u escape. The rest covers the stream's other events.
	maxStreamBytes = 32 << 20
	maxEventBytes  = 1 << 20
	maxToolCalls   = 256
)

var (
	errStream = errors.New("kagent response stream is invalid")
	// Broker failure codes are fixed lowercase identifiers.
	failureCodePattern = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
	toolIDPattern      = regexp.MustCompile(`^kt_[0-9a-f-]{36}$`)
	toolNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
)

type streamEvent struct {
	Type     string `json:"type"`
	Delta    string `json:"delta,omitempty"`
	Response *struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
		// Usage is read by the Orka supervisor, not by the child.
		Usage json.RawMessage `json:"usage,omitempty"`
	} `json:"response,omitempty"`
	Tool *struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Status string `json:"status,omitempty"`
	} `json:"tool,omitempty"`
}

// runPrompt sends one prompt through the supervisor proxy to the broker and
// relays the resulting stream as ACP session updates.
func (s *acpServer) runPrompt(ctx context.Context, session *acpSession, text string) error {
	payload := map[string]any{"model": s.cfg.agent.Model, "input": text, "stream": true, "store": true}
	if session.mcp != nil {
		payload["mcp"] = session.mcp
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(body) > maxProviderRequestBytes {
		return acpInvalidParams
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.providerURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.cfg.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close() //nolint:errcheck
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK {
		// The broker refuses before streaming with a fixed failure code.
		var refusal struct {
			Error string `json:"error"`
		}
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if mediaType == "application/json" && strictjson.Decode(data, &refusal, true) == nil && failureCodePattern.MatchString(refusal.Error) {
			return kagentFailure(refusal.Error)
		}
		return errStream
	}
	if err != nil || mediaType != "text/event-stream" {
		return errStream
	}
	watchdog := newIdleWatchdog(cancel, streamIdleTimeout)
	defer watchdog.stop()
	return s.consumeStream(ctx, session, io.TeeReader(io.LimitReader(response.Body, maxStreamBytes), watchdog))
}

func (s *acpServer) consumeStream(ctx context.Context, session *acpSession, body io.Reader) error {
	reader := bufio.NewReaderSize(body, 32<<10)
	tools := map[string]string{}
	var name, data string
	for {
		line, err := readStreamLine(reader)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errStream // EOF before a terminal event is never success.
		}
		switch {
		case line == "":
			if name == "" && data == "" {
				continue
			}
			done, err := s.handleEvent(ctx, session, tools, name, data)
			if err != nil || done {
				return err
			}
			name, data = "", ""
		case strings.HasPrefix(line, ":"):
			// Keepalive comment.
		case strings.HasPrefix(line, "event: ") && name == "":
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: ") && data == "":
			data = strings.TrimPrefix(line, "data: ")
		default:
			return errStream
		}
	}
}

func readStreamLine(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > maxEventBytes {
			return "", errStream
		}
		line = append(line, part...)
		if err == nil {
			return strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r"), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}

func (s *acpServer) handleEvent(ctx context.Context, session *acpSession, tools map[string]string, name, data string) (bool, error) {
	var event streamEvent
	if strictjson.Decode([]byte(data), &event, true) != nil || event.Type != name {
		return false, errStream
	}
	switch event.Type {
	case "response.created":
		return false, nil
	case "response.output_text.delta":
		if event.Delta == "" || !utf8.ValidString(event.Delta) {
			return false, errStream
		}
		return false, s.outputText(ctx, session.id, event.Delta)
	case "response.kagent_tool.started", "response.kagent_tool.finished":
		if event.Tool == nil || !toolIDPattern.MatchString(event.Tool.ID) || !toolNamePattern.MatchString(event.Tool.Name) {
			return false, errStream
		}
		if event.Type == "response.kagent_tool.started" {
			if _, seen := tools[event.Tool.ID]; seen || len(tools) >= maxToolCalls || event.Tool.Status != "" {
				return false, errStream
			}
			tools[event.Tool.ID] = event.Tool.Name
			return false, s.toolEvent(session.id, event.Tool.ID, event.Tool.Name, "in_progress")
		}
		if started, seen := tools[event.Tool.ID]; !seen || started != event.Tool.Name || (event.Tool.Status != "completed" && event.Tool.Status != "failed") {
			return false, errStream
		}
		return false, s.toolEvent(session.id, event.Tool.ID, event.Tool.Name, event.Tool.Status)
	case "response.completed":
		if event.Response == nil || event.Response.Status != "completed" {
			return false, errStream
		}
		return true, nil
	case "response.failed":
		if event.Response == nil || event.Response.Error == nil || !failureCodePattern.MatchString(event.Response.Error.Code) {
			return false, errStream
		}
		return true, kagentFailure(event.Response.Error.Code)
	default:
		return false, errStream
	}
}

func (s *acpServer) toolEvent(sessionID, id, name, status string) error {
	update := "tool_call_update"
	if status == "in_progress" {
		update = "tool_call"
	}
	return s.update(sessionID, map[string]string{
		"sessionUpdate": update, "toolCallId": id, "title": name, "kind": "other", "status": status,
	})
}

// idleWatchdog cancels the request when the stream goes silent.
type idleWatchdog struct {
	timer *time.Timer
	idle  time.Duration
}

func newIdleWatchdog(cancel context.CancelFunc, idle time.Duration) *idleWatchdog {
	return &idleWatchdog{timer: time.AfterFunc(idle, cancel), idle: idle}
}

func (w *idleWatchdog) Write(p []byte) (int, error) {
	w.timer.Reset(w.idle)
	return len(p), nil
}

func (w *idleWatchdog) stop() { w.timer.Stop() }
