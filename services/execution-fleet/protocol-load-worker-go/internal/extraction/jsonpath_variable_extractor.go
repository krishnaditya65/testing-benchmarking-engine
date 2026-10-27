// Package extraction pulls named values out of a protocol response body
// (via a JSONPath-like expression) and stores them in a VariableContext
// that later pipeline steps can inject into their own requests — the
// mechanism behind "REST JWT fetch -> extract token -> inject into gRPC
// stream" pipelines.
package extraction

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// VariableContext is a thread-safe named-value store shared across the
// steps of one MultiProtocolPipelineExecutor run. Earlier steps write
// extracted values (e.g. "authToken"); later steps read them back for
// header/body interpolation.
type VariableContext struct {
	mu     sync.RWMutex
	values map[string]any
}

// NewVariableContext returns an empty VariableContext ready to use.
func NewVariableContext() *VariableContext {
	return &VariableContext{values: make(map[string]any)}
}

// Set stores value under name, overwriting any previous value.
func (c *VariableContext) Set(name string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[name] = value
}

// Get returns the value stored under name and whether it was present.
func (c *VariableContext) Get(name string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[name]
	return v, ok
}

// GetString returns the value stored under name formatted as a string,
// which is the common case for injecting an extracted token into a header
// or query parameter.
func (c *VariableContext) GetString(name string) (string, bool) {
	v, ok := c.Get(name)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%v", v), true
}

// Interpolate replaces every "{{name}}" placeholder in template with the
// string form of the matching variable. Placeholders with no matching
// variable are left untouched so missing-variable bugs are visible in the
// output rather than silently producing empty strings.
func (c *VariableContext) Interpolate(template string) string {
	var b strings.Builder
	remaining := template
	for {
		start := strings.Index(remaining, "{{")
		if start == -1 {
			b.WriteString(remaining)
			break
		}
		end := strings.Index(remaining[start:], "}}")
		if end == -1 {
			b.WriteString(remaining)
			break
		}
		end += start

		b.WriteString(remaining[:start])
		name := strings.TrimSpace(remaining[start+2 : end])
		if value, ok := c.GetString(name); ok {
			b.WriteString(value)
		} else {
			b.WriteString(remaining[start : end+2])
		}
		remaining = remaining[end+2:]
	}
	return b.String()
}

// JsonPathVariableExtractor extracts a single value from a JSON response
// body using a restricted JSONPath dialect and stores it into a
// VariableContext under a caller-chosen name.
//
// Supported expression syntax (a hand-rolled subset covering the common
// "grab one field/array element from an API response" case):
//
//	$.field.nested.field   dot-separated object field access
//	$.items[2].id          numeric array index
//	$.items[-1].id          negative index counts from the end
//	$['field with spaces']  bracket access with a quoted key
//
// NOTE: this intentionally does not implement the full JSONPath spec
// (no wildcards, filters, or recursive descent). A production build that
// needs those should swap this for github.com/PaesslerAG/jsonpath (already
// listed in go.mod) behind the same ExtractInto method signature.
type JsonPathVariableExtractor struct{}

// NewJsonPathVariableExtractor returns a ready-to-use extractor. It holds
// no state, so a single instance can be shared across every pipeline step
// and goroutine.
func NewJsonPathVariableExtractor() *JsonPathVariableExtractor {
	return &JsonPathVariableExtractor{}
}

// ExtractInto evaluates expression against body, and on success stores the
// resulting value into ctx under variableName. It returns the extracted
// value as well, so callers that need it immediately (without a round trip
// through the VariableContext) can use it directly.
func (e *JsonPathVariableExtractor) ExtractInto(body []byte, expression, variableName string, ctx *VariableContext) (any, error) {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("extraction: response body is not valid JSON for expression %q: %w", expression, err)
	}

	segments, err := parseJSONPathSegments(expression)
	if err != nil {
		return nil, fmt.Errorf("extraction: parsing JSONPath %q: %w", expression, err)
	}

	value, err := evaluateSegments(decoded, segments)
	if err != nil {
		return nil, fmt.Errorf("extraction: evaluating JSONPath %q: %w", expression, err)
	}

	ctx.Set(variableName, value)
	return value, nil
}

// pathSegment is one hop of a parsed JSONPath expression: either a field
// name or a (possibly negative) array index.
type pathSegment struct {
	field   string
	isIndex bool
	index   int
}

// parseJSONPathSegments turns "$.a.b[2].c" into
// [{field:"a"} {field:"b"} {isIndex:true index:2} {field:"c"}].
func parseJSONPathSegments(expression string) ([]pathSegment, error) {
	trimmed := strings.TrimSpace(expression)
	trimmed = strings.TrimPrefix(trimmed, "$")
	trimmed = strings.TrimPrefix(trimmed, ".")

	var segments []pathSegment
	var current strings.Builder

	flushField := func() {
		if current.Len() > 0 {
			segments = append(segments, pathSegment{field: current.String()})
			current.Reset()
		}
	}

	runes := []rune(trimmed)
	for i := 0; i < len(runes); i++ {
		switch r := runes[i]; r {
		case '.':
			flushField()
		case '[':
			flushField()
			close := strings.IndexRune(string(runes[i:]), ']')
			if close == -1 {
				return nil, fmt.Errorf("unterminated '[' in expression")
			}
			inner := strings.TrimSpace(string(runes[i+1 : i+close]))
			inner = strings.Trim(inner, `'"`)
			if idx, err := strconv.Atoi(inner); err == nil {
				segments = append(segments, pathSegment{isIndex: true, index: idx})
			} else {
				segments = append(segments, pathSegment{field: inner})
			}
			i += close
		default:
			current.WriteRune(r)
		}
	}
	flushField()

	if len(segments) == 0 {
		return nil, fmt.Errorf("expression resolves to zero path segments")
	}
	return segments, nil
}

// evaluateSegments walks decoded following segments, returning the value
// found at the end of the path or an error describing where the walk
// failed.
func evaluateSegments(decoded any, segments []pathSegment) (any, error) {
	current := decoded
	for _, seg := range segments {
		switch {
		case seg.isIndex:
			arr, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array before index [%d], got %T", seg.index, current)
			}
			idx := seg.index
			if idx < 0 {
				idx += len(arr)
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("index %d out of range for array of length %d", seg.index, len(arr))
			}
			current = arr[idx]
		default:
			obj, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected object before field %q, got %T", seg.field, current)
			}
			value, present := obj[seg.field]
			if !present {
				return nil, fmt.Errorf("field %q not present in object", seg.field)
			}
			current = value
		}
	}
	return current, nil
}
