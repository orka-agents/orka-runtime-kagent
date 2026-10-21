package kagent

import (
	"testing"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
)

func TestToolActivityReadsBothKagentEncodings(t *testing.T) {
	v0 := func(key, kind string) *a2atype.Part {
		part := a2atype.NewDataPart(map[string]any{"id": "call-1", "name": "probe", "args": map[string]any{}})
		part.SetMeta(key, kind)
		return part
	}
	for name, test := range map[string]struct {
		part *a2atype.Part
		kind apia2a.ToolActivityKind
	}{
		"v1 call":             {apia2a.NewToolCallPart("call-1", "probe", nil), apia2a.ToolCallKind},
		"v1 result":           {apia2a.NewToolResultPart("call-1", "probe", nil), apia2a.ToolResultKind},
		"v0 kagent_type call": {v0("kagent_type", "function_call"), apia2a.ToolCallKind},
		"v0 adk_type result":  {v0("adk_type", "function_response"), apia2a.ToolResultKind},
	} {
		activity, ok := ToolActivity(test.part)
		if !ok || activity.Kind != test.kind || activity.ID != "call-1" || activity.Name != "probe" {
			t.Fatalf("%s: ToolActivity = %+v, %v", name, activity, ok)
		}
	}
	text := a2atype.NewTextPart("hello")
	if _, ok := ToolActivity(text); ok {
		t.Fatal("a text part was read as a tool")
	}
	if _, ok := ToolActivity(v0("adk_type", "code_execution")); ok {
		t.Fatal("another ADK part type was read as a tool")
	}
}
