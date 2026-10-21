package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

type acpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// Data carries identifier-shaped diagnostics the Orka supervisor shows;
	// it never copies free text.
	Data *acpErrorData `json:"data,omitempty"`
}

type acpErrorData struct {
	Service   string `json:"service"`
	ErrorName string `json:"errorName"`
}

func (e *acpRPCError) Error() string { return e.Message }

// kagentFailure reports one of the broker's fixed failure codes.
func kagentFailure(code string) *acpRPCError {
	return &acpRPCError{Code: -32603, Message: "kagent ACP prompt failed: " + code, Data: &acpErrorData{Service: "kagent", ErrorName: code}}
}

var (
	acpInvalidRequest = &acpRPCError{Code: -32600, Message: "invalid ACP request"}
	acpInvalidParams  = &acpRPCError{Code: -32602, Message: "invalid ACP parameters"}
	acpInternalError  = &acpRPCError{Code: -32603, Message: "kagent ACP prompt failed"}
)

type acpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type acpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *acpRPCError    `json:"error,omitempty"`
}

type acpMCPServer struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Headers []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"headers"`
	Meta json.RawMessage `json:"_meta,omitempty"`
}

type acpNewSession struct {
	CWD                   string          `json:"cwd"`
	AdditionalDirectories []string        `json:"additionalDirectories,omitempty"`
	MCPServers            []acpMCPServer  `json:"mcpServers"`
	Meta                  json.RawMessage `json:"_meta,omitempty"`
}

type acpPrompt struct {
	SessionID string `json:"sessionId"`
	Prompt    []struct {
		Type     string          `json:"type"`
		Text     *string         `json:"text,omitempty"`
		Name     string          `json:"name,omitempty"`
		URI      string          `json:"uri,omitempty"`
		MIMEType string          `json:"mimeType,omitempty"`
		Meta     json.RawMessage `json:"_meta,omitempty"`
	} `json:"prompt"`
	Meta json.RawMessage `json:"_meta,omitempty"`
}

func (p acpPrompt) text() (string, error) {
	if !config.SafeString(p.SessionID, 512) || len(p.Prompt) == 0 {
		return "", acpInvalidParams
	}
	var blocks []string
	for _, block := range p.Prompt {
		switch block.Type {
		case "text":
			if block.Text == nil || block.Name != "" || block.URI != "" || block.MIMEType != "" {
				return "", acpInvalidParams
			}
			blocks = append(blocks, *block.Text)
		case "resource_link":
			if block.Text != nil || !config.SafeString(block.Name, 1024) || !config.SafeString(block.URI, 8<<10) {
				return "", acpInvalidParams
			}
			text := "Resource link: " + block.Name + "\nURI: " + block.URI
			if block.MIMEType != "" {
				if !config.SafeString(block.MIMEType, 256) {
					return "", acpInvalidParams
				}
				text += "\nMIME type: " + block.MIMEType
			}
			blocks = append(blocks, text)
		default:
			return "", acpInvalidParams
		}
	}
	text := strings.Join(blocks, "\n")
	if len(text) > maxPromptBytes {
		return "", acpInvalidParams
	}
	return text, nil
}

func acpRequestKey(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || len(raw) > 1024 {
		return "", acpInvalidRequest
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil || !config.SafeString(value, 512) {
			return "", acpInvalidRequest
		}
		return "s:" + value, nil
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return "", acpInvalidRequest
	}
	return "n:" + strconv.FormatInt(value, 10), nil
}

func acpReadLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > acpMaxMessageBytes {
			return nil, acpInvalidRequest
		}
		line = append(line, part...)
		if err == nil {
			return bytes.TrimSpace(line), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(line) != 0 {
			return nil, acpInvalidRequest
		}
		return nil, err
	}
}
