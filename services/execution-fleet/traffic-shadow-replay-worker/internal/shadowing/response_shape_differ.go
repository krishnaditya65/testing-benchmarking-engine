package shadowing

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// DriftKind classifies one field-level difference SchemaDrift found between
// a baseline (originally captured) response body and a replayed response
// body, both parsed as JSON.
type DriftKind string

const (
	// FieldAdded means actual's response body has a JSON field at this path
	// that baseline's did not — the target environment is returning more
	// than it used to.
	FieldAdded DriftKind = "field-added"

	// FieldRemoved means baseline had a field at this path that actual's
	// response no longer has — usually the more dangerous direction for a
	// consumer that depends on the field, and typically the one worth
	// paging someone over.
	FieldRemoved DriftKind = "field-removed"

	// FieldTypeChanged means the field exists in both but its JSON value
	// type differs (e.g. a numeric ID that became a string, or a scalar
	// that became an object) — a classic silent-breaking-change shape.
	FieldTypeChanged DriftKind = "field-type-changed"
)

// SchemaDriftFinding is one flagged field-level difference.
type SchemaDriftFinding struct {
	FieldPath    string    `json:"fieldPath"`
	Kind         DriftKind `json:"kind"`
	BaselineType string    `json:"baselineType,omitempty"`
	ActualType   string    `json:"actualType,omitempty"`
}

// LatencyRegressionFinding flags that a replayed exchange's actual latency
// exceeded its captured baseline by more than ResponseShapeDiffer's
// configured threshold.
type LatencyRegressionFinding struct {
	BaselineLatency time.Duration `json:"baselineLatency"`
	ActualLatency   time.Duration `json:"actualLatency"`
	// RegressionRatio is ActualLatency / BaselineLatency; a value of 2.0
	// means the replay took twice as long as the original capture did.
	RegressionRatio float64 `json:"regressionRatio"`
}

// DiffFinding is the full result of comparing one CapturedExchange's
// originally-captured response against one ReplayResult's actual response.
type DiffFinding struct {
	ExchangePath       string                    `json:"exchangePath"`
	SchemaDrift        []SchemaDriftFinding      `json:"schemaDrift,omitempty"`
	LatencyRegression  *LatencyRegressionFinding `json:"latencyRegression,omitempty"`
	BaselineParseError string                    `json:"baselineParseError,omitempty"`
	ActualParseError   string                    `json:"actualParseError,omitempty"`
}

// HasFindings reports whether this diff flagged anything at all — schema
// drift, a latency regression, or a body that couldn't even be parsed as
// JSON for comparison (itself worth surfacing, since a baseline that used to
// be valid JSON and no longer is is its own kind of drift).
func (d DiffFinding) HasFindings() bool {
	return len(d.SchemaDrift) > 0 || d.LatencyRegression != nil || d.BaselineParseError != "" || d.ActualParseError != ""
}

// ResponseShapeDiffer implements this service's other half of "Production
// Traffic Shadowing": after TrafficMultiplierReplayer replays a fixture
// against a target environment, ResponseShapeDiffer compares what actually
// came back against what was originally captured, flagging exactly two
// classes of problem — response *schema* drift and response *latency*
// regression — since those are the two things a target environment can
// silently get wrong under shadow load without ever returning a
// non-2xx status the replayer itself would already notice.
type ResponseShapeDiffer struct {
	// LatencyRegressionThreshold is the ActualLatency/BaselineLatency ratio
	// at or above which Diff reports a LatencyRegressionFinding. Defaults
	// to 1.5 (50% slower than baseline) if <= 1.0.
	LatencyRegressionThreshold float64
}

// Diff compares baseline (the exchange as originally captured, including
// its recorded response body/latency) against actual (what the replay of
// that same exchange returned) and returns every finding.
func (d ResponseShapeDiffer) Diff(baseline CapturedExchange, actual ReplayResult) DiffFinding {
	finding := DiffFinding{ExchangePath: baseline.Path}

	threshold := d.LatencyRegressionThreshold
	if threshold <= 1.0 {
		threshold = 1.5
	}

	if actual.Err == nil && baseline.Latency > 0 && actual.Latency > 0 {
		ratio := float64(actual.Latency) / float64(baseline.Latency)
		if ratio >= threshold {
			finding.LatencyRegression = &LatencyRegressionFinding{
				BaselineLatency: baseline.Latency,
				ActualLatency:   actual.Latency,
				RegressionRatio: ratio,
			}
		}
	}

	// Schema drift only makes sense to compute when both bodies are
	// present and the replay didn't hard-fail — an Err'd replay has no
	// meaningful "actual" body to diff against.
	if actual.Err != nil || len(baseline.ResponseBody) == 0 || len(actual.Body) == 0 {
		return finding
	}

	baselineFields, err := flattenJSONFieldTypes(baseline.ResponseBody)
	if err != nil {
		finding.BaselineParseError = err.Error()
		return finding
	}
	actualFields, err := flattenJSONFieldTypes(actual.Body)
	if err != nil {
		finding.ActualParseError = err.Error()
		return finding
	}

	finding.SchemaDrift = diffFieldTypes(baselineFields, actualFields)
	return finding
}

// flattenJSONFieldTypes parses body as JSON and flattens it into a
// path -> JSON-value-kind map (e.g. "user.id" -> "number",
// "items[].sku" -> "string"), so structurally different bodies can be
// compared field by field regardless of key ordering. Arrays flatten every
// element under the same "[]" path segment rather than by index, since
// replay traffic legitimately varies array length/order call to call and
// this differ cares about *shape*, not exact content.
func flattenJSONFieldTypes(body []byte) (map[string]string, error) {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("shadowing: response body is not valid JSON: %w", err)
	}
	fields := make(map[string]string)
	flattenInto(fields, "", decoded)
	return fields, nil
}

func flattenInto(fields map[string]string, path string, value any) {
	switch v := value.(type) {
	case map[string]any:
		if path != "" {
			fields[path] = "object"
		}
		for key, child := range v {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			flattenInto(fields, childPath, child)
		}
	case []any:
		if path != "" {
			fields[path] = "array"
		}
		arrayPath := path + "[]"
		for _, elem := range v {
			flattenInto(fields, arrayPath, elem)
		}
	case string:
		fields[path] = "string"
	case float64:
		fields[path] = "number"
	case bool:
		fields[path] = "boolean"
	case nil:
		fields[path] = "null"
	}
}

// diffFieldTypes compares two flattened field-type maps and returns every
// added/removed/type-changed field, sorted by path for stable output.
func diffFieldTypes(baseline, actual map[string]string) []SchemaDriftFinding {
	var findings []SchemaDriftFinding

	for path, baselineType := range baseline {
		actualType, present := actual[path]
		switch {
		case !present:
			findings = append(findings, SchemaDriftFinding{FieldPath: path, Kind: FieldRemoved, BaselineType: baselineType})
		case actualType != baselineType:
			findings = append(findings, SchemaDriftFinding{FieldPath: path, Kind: FieldTypeChanged, BaselineType: baselineType, ActualType: actualType})
		}
	}
	for path, actualType := range actual {
		if _, present := baseline[path]; !present {
			findings = append(findings, SchemaDriftFinding{FieldPath: path, Kind: FieldAdded, ActualType: actualType})
		}
	}

	sort.Slice(findings, func(i, j int) bool { return findings[i].FieldPath < findings[j].FieldPath })
	return findings
}
