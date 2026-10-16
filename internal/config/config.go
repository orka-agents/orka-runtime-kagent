// Package config decodes the frozen agent configuration shared by the ACP
// child and the lifecycle broker. Both processes verify the exact file bytes
// against the digest Orka registered in the runtime profile.
package config

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

const (
	// AgentConfigPath is the configuration baked into the configured image.
	AgentConfigPath = "/agent/kagent.json"
	// ModelEnv and ConfigDigestEnv are set by the Orka supervisor for the
	// child, and by the deployment for the broker sidecar.
	ModelEnv        = "ORKA_KAGENT_ACP_MODEL"
	ConfigDigestEnv = "ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST"
	// MaxConfigBytes bounds the configuration file read.
	MaxConfigBytes = 64 << 10
)

// ErrConfig is the only error returned for configuration problems so that
// startup failures never echo configuration content.
var ErrConfig = errors.New("kagent agent configuration is invalid")

var (
	dnsLabel     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	revision     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	configHash   = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
	userIDPrefix = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?$`)
)

// Agent is the frozen, digest-pinned description of one wrapped kagent Agent.
type Agent struct {
	// Model is the profile model string Orka pins. kagent owns the real model.
	Model  string `json:"model"`
	Kagent Kagent `json:"kagent"`
	// Governed, when set, makes Orka execute the Agent's tools and serve its
	// model through the broker's relay. The approved revision must point
	// every MCP binding and model endpoint at that relay.
	Governed *Governed `json:"governed,omitempty"`
}

// Governed names the model endpoint the relay calls with Orka's credential.
type Governed struct {
	// UpstreamBaseURL is an OpenAI-compatible base URL, such as ".../v1".
	UpstreamBaseURL string `json:"upstreamBaseURL"`
	// UpstreamModel replaces whatever model the kagent Agent asks for.
	UpstreamModel string `json:"upstreamModel"`
}

// Kagent versions the adapter supports.
const (
	// KagentV1 is kagent v1, the Agent Substrate design (api.kagent.dev/v1alpha3).
	KagentV1 = "v1"
	// KagentV0 is kagent v0.x, which runs each Agent as a Deployment
	// (kagent.dev/v1alpha2).
	KagentV0 = "v0"
)

// Kagent identifies the kagent control plane and the exact Agent revision.
type Kagent struct {
	// Version selects the kagent API generation. Empty means KagentV1.
	Version string `json:"version,omitempty"`
	// ControlPlaneURL is the kagent controller's public API base URL.
	ControlPlaneURL string   `json:"controlPlaneURL"`
	Agent           AgentRef `json:"agent"`
	// ExpectedRevision pins what the Agent runs. For v1 it is the approved
	// prepared revision, which kagent pins each session to. For v0 it is the
	// kagent.dev/config-hash of the Agent's Deployment: a hash of its compiled
	// config, agent card, and secrets. Anything else is refused before dispatch.
	ExpectedRevision string `json:"expectedRevision"`
	// UserIDPrefix prefixes the per-Orka-session kagent user identity.
	UserIDPrefix string `json:"userIDPrefix"`
}

// KagentVersion returns the kagent API generation, defaulting to KagentV1.
func (k Kagent) KagentVersion() string {
	if k.Version == "" {
		return KagentV1
	}
	return k.Version
}

// AgentRef names a kagent Agent.
type AgentRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// Load reads path, verifies it against digest, and decodes it.
func Load(path, digest, model string) (Agent, []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return Agent{}, nil, ErrConfig
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, MaxConfigBytes+1))
	if err != nil || len(data) > MaxConfigBytes {
		return Agent{}, nil, ErrConfig
	}
	agent, err := Decode(data, digest, model)
	return agent, data, err
}

// Decode verifies data against digest before interpreting it.
func Decode(data []byte, digest, model string) (Agent, error) {
	if !DigestValid(digest) || subtle.ConstantTimeCompare([]byte(Digest(data)), []byte(digest)) != 1 {
		return Agent{}, ErrConfig
	}
	var agent Agent
	if strictjson.Decode(data, &agent, true) != nil {
		return Agent{}, ErrConfig
	}
	if agent.Model != model || !SafeString(agent.Model, 256) || strings.TrimSpace(agent.Model) != agent.Model {
		return Agent{}, ErrConfig
	}
	k := agent.Kagent
	pin := revision
	switch k.Version {
	case "", KagentV1:
	case KagentV0:
		pin = configHash
	default:
		return Agent{}, ErrConfig
	}
	if _, err := ControlPlaneURL(k.ControlPlaneURL); err != nil ||
		!dnsLabel.MatchString(k.Agent.Namespace) || len(k.Agent.Namespace) > 63 ||
		!dnsSubdomain.MatchString(k.Agent.Name) || len(k.Agent.Name) > 253 ||
		!pin.MatchString(k.ExpectedRevision) || !userIDPrefix.MatchString(k.UserIDPrefix) {
		return Agent{}, ErrConfig
	}
	if g := agent.Governed; g != nil {
		if _, err := UpstreamBaseURL(g.UpstreamBaseURL); err != nil || !SafeString(g.UpstreamModel, 256) ||
			strings.TrimSpace(g.UpstreamModel) != g.UpstreamModel {
			return Agent{}, ErrConfig
		}
	}
	return agent, nil
}

// UpstreamBaseURL validates an http(s) base URL that may carry a path but no
// credentials, query, or fragment. Its trailing slash is removed.
func UpstreamBaseURL(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || !SafeString(value, 2048) || strings.TrimSpace(value) != value || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || u.RawPath != "" {
		return "", ErrConfig
	}
	return strings.TrimSuffix(value, "/"), nil
}

// SessionMCPServer accepts only the pod-local Orka session MCP server that a
// governed broker relays to: plain http to a literal loopback IP with an
// explicit port, and a bounded bearer. The ACP child and the broker both use
// it, so a descriptor the broker would refuse fails at session creation.
func SessionMCPServer(rawURL, authorization string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	return ip != nil && ip.IsLoopback() && u.Port() != "" && ok && SafeString(token, 512) && !strings.ContainsAny(token, " \t")
}

// ControlPlaneURL validates the kagent controller base URL: plain http(s) with
// a DNS host, no credentials, query, fragment, or path.
func ControlPlaneURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || !SafeString(value, 2048) || strings.TrimSpace(value) != value || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return nil, ErrConfig
	}
	if net.ParseIP(u.Hostname()) != nil || !dnsSubdomain.MatchString(u.Hostname()) {
		// Substrate egress rules match DNS names; literal IPs are unsupported.
		return nil, ErrConfig
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, ErrConfig
		}
	}
	u.Path = ""
	return u, nil
}

// SafeString reports whether value is nonempty, at most limit bytes, and free
// of control characters.
func SafeString(value string, limit int) bool {
	if value == "" || len(value) > limit {
		return false
	}
	for _, ch := range value {
		if unicode.IsControl(ch) {
			return false
		}
	}
	return true
}

// Digest returns the "sha256:<hex>" digest of data.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// JSONDigest digests the JSON encoding of a concrete, JSON-safe value.
func JSONDigest(value any) string {
	data, _ := json.Marshal(value)
	return Digest(data)
}

// DigestValid reports whether value is a lowercase "sha256:<64 hex>" digest.
func DigestValid(value string) bool {
	encoded, ok := strings.CutPrefix(value, "sha256:")
	if !ok || len(encoded) != 64 {
		return false
	}
	for _, ch := range encoded {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
