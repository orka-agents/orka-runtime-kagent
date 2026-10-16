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
