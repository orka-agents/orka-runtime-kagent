package kagent

import (
	"encoding/json"
	"math"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
)

// v0UsageMetadataKey is where kagent v0.10 reports a model call's usage.
const v0UsageMetadataKey = "adk_usage_metadata"

// Usage is the token usage kagent reported for one model call.
type Usage struct {
	Input, Output, Cached uint64
	HasCached             bool
}

// EventUsage returns the usage kagent reported on event, once per model
// call: on the event's metadata, or else on its status message's. kagent
// reports Gemini usage fields (v1 under kagent.dev/a2a/usage, v0 under
// adk_usage_metadata); partial stream events carry none. kagent, not Orka,
// measured these numbers.
func EventUsage(event a2atype.Event) (Usage, bool) {
	var metadata []map[string]any
	switch e := event.(type) {
	case *a2atype.TaskStatusUpdateEvent:
		metadata = append(metadata, e.Metadata)
		if e.Status.Message != nil {
			metadata = append(metadata, e.Status.Message.Metadata)
		}
	case *a2atype.TaskArtifactUpdateEvent:
		metadata = append(metadata, e.Metadata)
	case *a2atype.Message:
		metadata = append(metadata, e.Metadata)
	}
	for _, m := range metadata {
		if partial, _ := m["adk_partial"].(bool); partial {
			return Usage{}, false
		}
		raw, ok := m[apia2a.UsageMetadataKey].(map[string]any)
		if !ok {
			raw, ok = m[v0UsageMetadataKey].(map[string]any)
		}
		if ok {
			return parseUsage(raw)
		}
	}
	return Usage{}, false
}

// ArtifactUsage returns the usage kagent v1 reports on the artifacts of
// event, keyed by artifact ID: v1 records each model call's usage on the
// artifact the call produced, which artifact updates and task snapshots may
// both repeat, so a caller counts each key once.
func ArtifactUsage(event a2atype.Event) map[a2atype.ArtifactID]Usage {
	var artifacts []*a2atype.Artifact
	switch e := event.(type) {
	case *a2atype.TaskArtifactUpdateEvent:
		artifacts = append(artifacts, e.Artifact)
	case *a2atype.Task:
		artifacts = e.Artifacts
	}
	usage := map[a2atype.ArtifactID]Usage{}
	for _, artifact := range artifacts {
		if artifact == nil || artifact.ID == "" {
			continue
		}
		raw, ok := artifact.Metadata[apia2a.UsageMetadataKey].(map[string]any)
		if !ok {
			continue
		}
		if parsed, ok := parseUsage(raw); ok {
			usage[artifact.ID] = parsed
		}
	}
	return usage
}

func parseUsage(raw map[string]any) (Usage, bool) {
	prompt, okPrompt := count(raw["promptTokenCount"])
	candidates, okCandidates := count(raw["candidatesTokenCount"])
	if !okPrompt && !okCandidates {
		return Usage{}, false
	}
	toolPrompt, _ := count(raw["toolUsePromptTokenCount"])
	thoughts, _ := count(raw["thoughtsTokenCount"])
	cached, hasCached := count(raw["cachedContentTokenCount"])
	return Usage{Input: prompt + toolPrompt, Output: candidates + thoughts, Cached: cached, HasCached: hasCached}, true
}

// count reads a non-negative integral JSON number.
func count(value any) (uint64, bool) {
	var f float64
	switch v := value.(type) {
	case float64:
		f = v
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case json.Number:
		parsed, err := v.Float64()
		if err != nil {
			return 0, false
		}
		f = parsed
	default:
		return 0, false
	}
	if f < 0 || f > 1<<53 || f != math.Trunc(f) {
		return 0, false
	}
	return uint64(f), true
}
