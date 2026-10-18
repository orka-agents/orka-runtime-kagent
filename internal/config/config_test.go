package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validConfig = `{"model":"kagent/kagent/probe","kagent":{"controlPlaneURL":"http://kagent-controller.kagent.svc:8083","agent":{"namespace":"kagent","name":"probe"},"expectedRevision":"c85fb79c49c56d3720b2744f8e330d1cbe5f18fe84ad407f060043677716806f","userIDPrefix":"orka"}}`

func TestDecodeVerifiesDigestModelAndFields(t *testing.T) {
	data := []byte(validConfig)
	agent, err := Decode(data, Digest(data), "kagent/kagent/probe")
	if err != nil {
		t.Fatal(err)
	}
	if agent.Kagent.Agent.Name != "probe" || agent.Kagent.UserIDPrefix != "orka" {
		t.Fatalf("decoded %#v", agent)
	}
	if _, err := Decode(data, Digest([]byte("other")), "kagent/kagent/probe"); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	if _, err := Decode(data, Digest(data), "other-model"); err == nil {
		t.Fatal("model mismatch accepted")
	}
	for name, mutate := range map[string]func(string) string{
		"unknown field": func(s string) string { return strings.Replace(s, `"userIDPrefix"`, `"extra":1,"userIDPrefix"`, 1) },
		"duplicate field": func(s string) string {
			return strings.Replace(s, `"userIDPrefix":"orka"`, `"userIDPrefix":"orka","userIDPrefix":"x"`, 1)
		},
		"short revision":     func(s string) string { return strings.Replace(s, "c85fb79c", "", 1) },
		"ip control plane":   func(s string) string { return strings.Replace(s, "kagent-controller.kagent.svc", "10.0.0.1", 1) },
		"control plane path": func(s string) string { return strings.Replace(s, ":8083", ":8083/api", 1) },
		"bad namespace":      func(s string) string { return strings.Replace(s, `"namespace":"kagent"`, `"namespace":"Kagent"`, 1) },
		"bad user prefix":    func(s string) string { return strings.Replace(s, `"orka"`, `"Orka!"`, 1) },
		"credentials in url": func(s string) string { return strings.Replace(s, "http://", "http://user:pass@", 1) },
		"governed without model": func(s string) string {
			return strings.Replace(s, `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc/v1","upstreamModel":""},"kagent":{`, 1)
		},
		"governed upstream query": func(s string) string {
			return strings.Replace(s, `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc/v1?x=1","upstreamModel":"m"},"kagent":{`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := []byte(mutate(validConfig))
			if _, err := Decode(changed, Digest(changed), "kagent/kagent/probe"); err == nil {
				t.Fatalf("%s accepted", name)
			}
		})
	}
}

func TestLoadBoundsAndVerifiesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kagent.json")
	if err := os.WriteFile(path, []byte(validConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, data, err := Load(path, Digest([]byte(validConfig)), "kagent/kagent/probe"); err != nil || string(data) != validConfig {
		t.Fatalf("Load = %v", err)
	}
	large := filepath.Join(dir, "large.json")
	if err := os.WriteFile(large, make([]byte, MaxConfigBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(large, Digest(make([]byte, MaxConfigBytes+1)), "x"); err == nil {
		t.Fatal("oversized configuration accepted")
	}
}

func TestDecodeAcceptsGovernedUpstream(t *testing.T) {
	data := []byte(strings.Replace(validConfig, `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc:8080/v1/","upstreamModel":"gpt-test"},"kagent":{`, 1))
	agent, err := Decode(data, Digest(data), "kagent/kagent/probe")
	if err != nil || agent.Governed == nil || agent.Governed.UpstreamModel != "gpt-test" {
		t.Fatalf("governed config = %#v %v", agent.Governed, err)
	}
	if base, err := UpstreamBaseURL(agent.Governed.UpstreamBaseURL); err != nil || base != "http://model.orka.svc:8080/v1" {
		t.Fatalf("upstream base = %q %v", base, err)
	}
}

func TestDecodePinsEachKagentVersionItsOwnWay(t *testing.T) {
	v1Revision := "c85fb79c49c56d3720b2744f8e330d1cbe5f18fe84ad407f060043677716806f"
	const v0 = `"version":"v0","expectedImageDigest":"sha256:5ee9592cb7d959538d0f0fe36acf31ba3a06de084f84048b1459c3bdfafcda1f",`
	for _, test := range []struct {
		name, version, revision string
		ok                      bool
		want                    string
	}{
		{"v1 by default", "", v1Revision, true, KagentV1},
		{"explicit v1", `"version":"v1",`, v1Revision, true, KagentV1},
		{"v0 config hash", v0, "15270541297416117040", true, KagentV0},
		{"v0 with a v1 revision", v0, v1Revision, false, ""},
		{"v1 with a config hash", "", "15270541297416117040", false, ""},
		{"v0 with a leading zero", v0, "0123", false, ""},
		{"v0 without an image", `"version":"v0",`, "15270541297416117040", false, ""},
		{"v0 with a bad image", strings.Replace(v0, "sha256:5ee9", "5ee9", 1), "15270541297416117040", false, ""},
		{"v1 with an image", `"expectedImageDigest":"sha256:5ee9592cb7d959538d0f0fe36acf31ba3a06de084f84048b1459c3bdfafcda1f",`, v1Revision, false, ""},
		{"unknown version", `"version":"v2",`, v1Revision, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(strings.Replace(strings.Replace(validConfig, v1Revision, test.revision, 1), `"kagent":{`, `"kagent":{`+test.version, 1))
			agent, err := Decode(data, Digest(data), "kagent/kagent/probe")
			if (err == nil) != test.ok {
				t.Fatalf("Decode = %v, want ok %v", err, test.ok)
			}
			if test.ok && agent.Kagent.KagentVersion() != test.want {
				t.Fatalf("version = %q, want %q", agent.Kagent.KagentVersion(), test.want)
			}
		})
	}
}

func TestDecodeChecksGuardsAndLimits(t *testing.T) {
	for _, test := range []struct {
		name, replace, with string
		ok                  bool
	}{
		{"accepted commits", `"userIDPrefix":"orka"`, `"userIDPrefix":"orka","acceptedCommits":["1eb89fb","8878c39d0c1e"]`, true},
		{"short commit", `"userIDPrefix":"orka"`, `"userIDPrefix":"orka","acceptedCommits":["1eb89f"]`, false},
		{"uppercase commit", `"userIDPrefix":"orka"`, `"userIDPrefix":"orka","acceptedCommits":["1EB89FB"]`, false},
		{"unreachable addresses", `"kagent":{`, `"unreachable":["kagent-postgresql.kagent.svc:5432","10.0.0.7:8080"],"kagent":{`, true},
		{"unreachable without a port", `"kagent":{`, `"unreachable":["kagent-postgresql.kagent.svc"],"kagent":{`, false},
		{"unreachable bad port", `"kagent":{`, `"unreachable":["router:0"],"kagent":{`, false},
		{"unreachable padded port", `"kagent":{`, `"unreachable":["router:080"],"kagent":{`, false},
		{"output limit", `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc/v1","upstreamModel":"m","maxOutputTokens":4096},"kagent":{`, true},
		{"negative output limit", `"kagent":{`, `"governed":{"upstreamBaseURL":"http://model.orka.svc/v1","upstreamModel":"m","maxOutputTokens":-1},"kagent":{`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(strings.Replace(validConfig, test.replace, test.with, 1))
			if _, err := Decode(data, Digest(data), "kagent/kagent/probe"); (err == nil) != test.ok {
				t.Fatalf("Decode = %v, want ok %v", err, test.ok)
			}
		})
	}
}
