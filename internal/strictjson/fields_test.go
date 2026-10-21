package strictjson_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/orka-agents/orka-runtime-kagent/internal/strictjson"
)

// outputItem mirrors the shape of a Responses output item: a named type plus
// opaque JSON arguments whose member names must stay case-sensitive.
type outputItem struct {
	ID        string          `json:"id,omitempty"`
	Type      string          `json:"type"`
	CallID    string          `json:"call_id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func TestACPStructFieldsKeepOpaqueValuesCaseSensitive(t *testing.T) {
	var value struct {
		outputItem
		Status string         `json:"status"`
		Map    map[string]any `json:"map"`
		Any    any            `json:"any"`
	}
	data := []byte(`{"TyPe":"function_call","ſtatus":"completed","ArGuMeNtS":{"Key":1,"key":2},"Map":{"Key":3,"key":4},"Any":{"Key":5,"key":6},"unknown":{"Key":7,"key":8},"Unknown":null}`)
	if err := strictjson.DecodeStruct(data, &value, false); err != nil {
		t.Fatal("single aliases or opaque case-sensitive values rejected")
	}
	if value.Type != "function_call" || value.Status != "completed" ||
		string(value.Arguments) != `{"Key":1,"key":2}` || len(value.Map) != 2 || len(value.Any.(map[string]any)) != 2 {
		t.Fatal("structured decode changed an opaque value")
	}
	for _, data := range []string{
		`{"arguments":{"Key":1,"Key":2}}`,
		`{"Map":{"Key":1,"\u004bey":2}}`,
		`{"Any":{"nested":{"same":1,"same":2}}}`,
		`{"unknown":{"same":1,"same":2}}`,
		`{"arguments":"\ud800"}`,
	} {
		if strictjson.DecodeStruct([]byte(data), &value, false) == nil {
			t.Error("opaque value bypassed existing exact duplicate or Unicode validation")
		}
	}
}

func TestACPStructFieldsRejectAliasesBeforeApplyingValues(t *testing.T) {
	type nested struct {
		Status string `json:"status"`
	}
	for _, data := range []string{
		`{"status":"active","Status":"idle"}`,
		`{"status":"active","ſtatus":"idle"}`,
		`{"status":"active","\u0053tatus":"idle"}`,
		`{"status":null,"Status":"idle"}`,
		`{"values":[{"status":"active","Status":"idle"}]}`,
		`{"pointer":{"status":"active","Status":"idle"}}`,
		`{"pointer":{"status":"active"},"Pointer":{"status":"idle"}}`,
	} {
		var value struct {
			nested
			Values  []nested `json:"values"`
			Pointer *nested  `json:"pointer"`
		}
		value.Status = "untouched"
		if strictjson.DecodeStruct([]byte(data), &value, false) == nil || value.Status != "untouched" || value.Pointer != nil || value.Values != nil {
			t.Error("folded alias was accepted or applied before rejection")
		}
	}
}

func TestACPStructFieldsPreserveDecoderCompatibility(t *testing.T) {
	type embedded struct {
		ID string `json:"id"`
	}
	for _, data := range []string{
		`{"ID":"first","id":"second","Status":"completed"}`,
		`{"Id":"folded","status":"completed"}`,
		`{"id":"canonical","unknown":{"Status":"active","status":"idle"}}`,
	} {
		var ordinary, checked struct {
			embedded
			ID     string `json:"ID"`
			Status string `json:"status"`
		}
		if strictjson.Decode([]byte(data), &ordinary, false) != nil || strictjson.DecodeStruct([]byte(data), &checked, false) != nil || ordinary != checked {
			t.Error("unambiguous fields differ from encoding/json")
		}
	}
	var object struct {
		Value json.RawMessage `json:"value"`
	}
	if strictjson.DecodeStruct([]byte(`{"unknown":true}`), &object, false) != nil || strictjson.DecodeStruct([]byte(`{"unknown":true}`), &object, true) == nil {
		t.Error("unknown-field policy changed")
	}
	if strictjson.DecodeStruct([]byte(`{"value":`+strings.Repeat("[", 65)+strings.Repeat("]", 65)+`}`), &object, false) == nil {
		t.Error("structured decoder lost the depth bound")
	}
}
