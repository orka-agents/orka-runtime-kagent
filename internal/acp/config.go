// Package acp implements the ACP child that Orka's supervisor launches for a
// wrapped kagent Agent. Its only network authority is the supervisor-owned
// loopback provider proxy; it holds no kagent credentials.
package acp

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

const (
	providerBaseEnv    = "ORKA_KAGENT_ACP_PROVIDER_BASE_URL"
	providerTokenEnv   = "ORKA_KAGENT_ACP_PROVIDER_TOKEN"
	acpMaxMessageBytes = 8 << 20
	maxPromptBytes     = 4 << 20
	// maxProviderRequestBytes is the broker's request limit. Escaping and the
	// MCP descriptor make the encoded request longer than the prompt text.
	maxProviderRequestBytes = 4 << 20
	acpTextChunkBytes       = 32 << 10
	// The broker sends a keepalive every 15s; a longer silence means the
	// relay is gone.
	streamIdleTimeout     = 90 * time.Second
	responseHeaderTimeout = 130 * time.Second
)

var errACPTransport = errors.New("kagent ACP transport failed")

type acpConfiguration struct {
	agent       config.Agent
	providerURL string
	token       string
}

// MaybeServe runs the ACP child when --protocol acp is selected.
func MaybeServe(args []string, getenv func(string) string, input io.ReadCloser, output io.WriteCloser) error {
	flags := flag.NewFlagSet("kagent-acp", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	protocol := flags.String("protocol", "", "")
	path := flags.String("config", config.AgentConfigPath, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != "acp" {
		return config.ErrConfig
	}
	cfg, err := loadConfiguration(*path, getenv)
	if err != nil {
		return err
	}
	return serveACP(context.Background(), cfg, input, output)
}

func loadConfiguration(path string, getenv func(string) string) (acpConfiguration, error) {
	agent, _, err := config.Load(path, getenv(config.ConfigDigestEnv), getenv(config.ModelEnv))
	if err != nil {
		return acpConfiguration{}, err
	}
	base, err := loopbackURL(getenv(providerBaseEnv))
	token := getenv(providerTokenEnv)
	if err != nil || !config.SafeString(token, 16<<10) || strings.ContainsAny(token, " \t") {
		return acpConfiguration{}, config.ErrConfig
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/responses"
	return acpConfiguration{agent: agent, providerURL: base.String(), token: token}, nil
}

func loopbackURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || !config.SafeString(value, 8<<10) || strings.TrimSpace(value) != value || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || u.RawPath != "" {
		return nil, config.ErrConfig
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, config.ErrConfig
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, config.ErrConfig
		}
	}
	return u, nil
}

func newHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		// No overall timeout: a kagent turn streams for as long as it runs. The
		// idle watchdog and the prompt context bound it instead.
		CheckRedirect: func(*http.Request, []*http.Request) error { return errACPTransport },
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, errACPTransport
				}
				if host == "localhost" {
					host = "127.0.0.1"
				}
				if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
					return nil, errACPTransport
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
			},
			MaxIdleConns: 2, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 2,
			IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: responseHeaderTimeout,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

// Main is the binary entry point for ACP mode.
func Main(args []string) error {
	return MaybeServe(args, os.Getenv, os.Stdin, os.Stdout)
}
