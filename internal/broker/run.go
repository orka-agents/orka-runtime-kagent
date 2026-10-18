package broker

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

const (
	addrEnv     = "ORKA_KAGENT_BROKER_ADDR"
	stateDirEnv = "ORKA_KAGENT_BROKER_STATE_DIR"
	bearerEnv   = "ORKA_KAGENT_BROKER_BEARER_TOKEN"
	defaultAddr = "127.0.0.1:8091"
	// Governed mode only: the relay kagent Agents call, and Orka's model
	// credential, which the relay alone holds.
	relayAddrEnv     = "ORKA_KAGENT_RELAY_ADDR"
	defaultRelayAddr = ":8092"
	upstreamTokenEnv = "ORKA_KAGENT_MODEL_UPSTREAM_TOKEN"
)

var errUsage = errors.New("usage: orka-runtime-kagent --protocol broker --config <path> [--health-check]")

// Run serves the lifecycle broker until SIGTERM, or performs a health check.
func Run(args []string, getenv func(string) string) error {
	flags := flag.NewFlagSet("kagent-broker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	protocol := flags.String("protocol", "", "")
	path := flags.String("config", config.AgentConfigPath, "")
	healthCheck := flags.Bool("health-check", false, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *protocol != "broker" {
		return errUsage
	}
	addr, err := loopbackAddr(getenv(addrEnv))
	if err != nil {
		return err
	}
	if *healthCheck {
		return checkHealth(addr)
	}
	agent, _, err := config.Load(*path, getenv(config.ConfigDigestEnv), getenv(config.ModelEnv))
	if err != nil {
		return err
	}
	bearer := getenv(bearerEnv)
	stateDir := getenv(stateDirEnv)
	if len(bearer) < 32 || strings.ContainsFunc(bearer, func(r rune) bool { return r <= ' ' || r == 0x7f }) || stateDir == "" {
		return config.ErrConfig
	}
	client, err := newRemote(agent.Kagent, getenv)
	if err != nil {
		return err
	}
	defer client.Close() //nolint:errcheck
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	cfg := settings{
		bearer: bearer, configDigest: getenv(config.ConfigDigestEnv), stateDir: stateDir, model: agent.Model,
		expectedRevision: agent.Kagent.ExpectedRevision, userIDPrefix: agent.Kagent.UserIDPrefix,
		kagentV0:    agent.Kagent.KagentVersion() == config.KagentV0,
		unreachable: agent.Unreachable,
	}
	relayAddr := ""
	if g := agent.Governed; g != nil {
		token := getenv(upstreamTokenEnv)
		base, err := config.UpstreamBaseURL(g.UpstreamBaseURL)
		if err != nil || len(token) < 16 || strings.ContainsFunc(token, func(r rune) bool { return r <= ' ' || r == 0x7f }) {
			return config.ErrConfig
		}
		cfg.governed = &governedSettings{UpstreamBaseURL: base, UpstreamModel: g.UpstreamModel, UpstreamToken: token,
			MaxOutputTokens: g.MaxOutputTokens}
		if relayAddr = getenv(relayAddrEnv); relayAddr == "" {
			relayAddr = defaultRelayAddr
		}
		if _, _, err := net.SplitHostPort(relayAddr); err != nil {
			return config.ErrConfig
		}
	}
	// Bind the relay before admitting turns: a governed broker without its
	// relay would accept turns whose every tool and model call fails.
	var relayListener net.Listener
	if relayAddr != "" {
		if relayListener, err = net.Listen("tcp", relayAddr); err != nil {
			return err
		}
		defer relayListener.Close() //nolint:errcheck
	}
	b, err := newLifecycleBroker(ctx, cfg, client)
	if err != nil {
		return err
	}
	defer b.close()
	// Report an unsupported install at startup, not only at the first turn.
	go func() { _ = b.admit(ctx) }()
	errs := make(chan error, 2)
	if relayListener != nil {
		relay := &http.Server{
			Handler: b.relayHandler(),
			// Tool calls may wait for an Orka approval, so there is no write deadline.
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			MaxHeaderBytes: 32 << 10,
		}
		go func() { errs <- relay.Serve(relayListener) }()
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = relay.Shutdown(shutdown)
		}()
	}
	server := &http.Server{
		Addr: addr, Handler: b,
		// Responses stream for a whole turn, so there is no write deadline.
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	go func() { errs <- server.ListenAndServe() }()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

// closableRemote is a kagent client the broker owns for its lifetime.
type closableRemote interface {
	remote
	Close() error
}

// newRemote builds the client for the configured kagent version. A v0 Agent
// is pinned by the config hash on its only pod, which the broker reads with its
// pod's service account.
func newRemote(cfg config.Kagent, getenv func(string) string) (closableRemote, error) {
	if cfg.KagentVersion() != config.KagentV0 {
		return kagent.New(cfg)
	}
	pin, err := kagent.NewAgentPods(cfg.Agent, getenv)
	if err != nil {
		return nil, err
	}
	return kagent.NewV0(cfg, pin)
}

func loopbackAddr(value string) (string, error) {
	if value == "" {
		value = defaultAddr
	}
	host, port, err := net.SplitHostPort(value)
	if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() || port == "" {
		return "", config.ErrConfig
	}
	return value, nil
}

func checkHealth(addr string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return err
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		return errors.New("kagent broker is not healthy")
	}
	return nil
}
