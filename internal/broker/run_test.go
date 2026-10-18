package broker

import (
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

func TestRunFailsWhenTheRelayCannotBind(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close() //nolint:errcheck
	dir := t.TempDir()
	data := []byte(`{"model":"kagent/kagent/probe","kagent":{"controlPlaneURL":"http://kagent-controller.kagent.svc:8083",` +
		`"agent":{"namespace":"kagent","name":"probe"},"expectedRevision":"` + testRevision + `","userIDPrefix":"orka"},` +
		`"governed":{"upstreamBaseURL":"http://model.orka-system.svc/v1","upstreamModel":"gpt-test"}}`)
	path := filepath.Join(dir, "kagent.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		config.ConfigDigestEnv: config.Digest(data), config.ModelEnv: "kagent/kagent/probe",
		addrEnv: "127.0.0.1:0", bearerEnv: testBearer, stateDirEnv: filepath.Join(dir, "ledger"),
		relayAddrEnv: taken.Addr().String(), upstreamTokenEnv: testUpstreamToken,
	}
	done := make(chan error, 1)
	go func() {
		done <- Run([]string{"--protocol", "broker", "--config", path}, func(key string) string { return env[key] })
	}()
	select {
	case err := <-done:
		if bind := (*net.OpError)(nil); !errors.As(err, &bind) || bind.Op != "listen" {
			t.Fatalf("Run = %v, want the relay's listen error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept serving although the relay address was taken")
	}
}

// The relay closes every connection, so a client whose Service moved to a new
// runtime reaches it on its next request.
func TestRelayClosesEveryConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelayServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	go func() { _ = relay.Serve(listener) }()
	t.Cleanup(func() { _ = relay.Close() })
	response, err := http.Get("http://" + listener.Addr().String() + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	// The client reads the server's Connection: close into Close.
	if !response.Close {
		t.Fatal("the relay kept its connection open")
	}
}
