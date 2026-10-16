package kagent

import (
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
)

// kagent v0 marks a tool part with the ADK's metadata key or its own prefixed
// copy of it, instead of v1's part-type key.
var v0ToolTypeKeys = []string{"kagent_type", "adk_type"}

// ToolActivity reports whether part is a tool call or tool result, in either
// kagent version's encoding. Both carry the call's id, name, and args or
// response as the part's data.
func ToolActivity(part *a2atype.Part) (apia2a.ToolActivity, bool) {
	if activity, ok := apia2a.ParseToolActivity(part); ok || part == nil {
		return activity, ok
	}
	for _, key := range v0ToolTypeKeys {
		kind, _ := part.Metadata[key].(string)
		if kind != string(apia2a.ToolCallKind) && kind != string(apia2a.ToolResultKind) {
			continue
		}
		data, ok := part.Data().(map[string]any)
		if !ok {
			return apia2a.ToolActivity{}, false
		}
		activity := apia2a.ToolActivity{Kind: apia2a.ToolActivityKind(kind), Args: data["args"], Response: data["response"]}
		activity.ID, _ = data["id"].(string)
		activity.Name, _ = data["name"].(string)
		return activity, true
	}
	return apia2a.ToolActivity{}, false
}
