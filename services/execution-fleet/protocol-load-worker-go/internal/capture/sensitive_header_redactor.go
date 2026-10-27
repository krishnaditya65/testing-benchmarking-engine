package capture

import "strings"

// redactedValuePlaceholder replaces a sensitive header's value in a captured
// fixture. Kept as a recognizable sentinel (not empty string, not the real
// value) so a reader of a fixture file can tell redaction ran versus a
// header that was simply absent.
const redactedValuePlaceholder = "[REDACTED]"

// sensitiveHeaderExactNames are header names redacted regardless of casing,
// matched exactly (after lowercasing both sides).
var sensitiveHeaderExactNames = map[string]struct{}{
	"authorization":       {},
	"cookie":              {},
	"set-cookie":          {},
	"proxy-authorization": {},
	"x-csrf-token":        {},
}

// sensitiveHeaderNamePatterns catches the long tail of vendor-specific
// API-key header conventions that don't share one exact name — e.g.
// "X-Api-Key", "X-Auth-Token", "X-Stripe-Signature", "X-Github-Token". Every
// pattern here is a lowercase substring match against the lowercased header
// name, so this list is deliberately permissive (false positives redact a
// harmless header; false negatives leak a secret into a persisted fixture —
// the asymmetry means this list should stay biased toward over-matching).
var sensitiveHeaderNamePatterns = []string{
	"api-key",
	"apikey",
	"auth-token",
	"access-token",
	"secret",
	"signature",
	"session-id",
	"session-token",
}

// SensitiveHeaderRedactor strips auth tokens, cookies, and API-key-shaped
// headers from captured request/response headers before a
// ReplayableTrafficFixture is ever persisted to disk. It exists because
// LiveTrafficCaptureRecorder observes real production traffic in passive
// sidecar mode — that traffic legitimately contains bearer tokens, session
// cookies, and vendor API keys, and a fixture file is a plain JSON artifact
// that may get committed to a repo, attached to a bug report, or shared
// across teams. Redaction happens unconditionally in
// LiveTrafficCaptureRecorder.Observe; there is no "capture raw" mode.
//
// NOTE: this redacts *headers* only. Request/response *bodies* may also
// carry PII or secrets (e.g. a JSON body with an "access_token" field or a
// user's email address) — body-level redaction needs schema-aware or
// pattern-based scanning that is a materially bigger scope than header
// redaction (false positives corrupt replay fidelity by mangling legitimate
// business data, not just secrets) and is intentionally left as follow-up
// work. Teams enabling --capture-mode against traffic with sensitive body
// content should pair it with test-data-factory-service's synthetic/PII-masked
// data generation on the target side, or restrict capture to non-production
// environments, until body redaction lands.
type SensitiveHeaderRedactor struct{}

// Redact returns a copy of headers with every sensitive header's value
// replaced by redactedValuePlaceholder, plus the list of header names (as
// they originally appeared, not lowercased) that were actually redacted —
// the second return feeds ReplayableTrafficFixture.RedactedHeaderNames so a
// fixture is self-documenting about what it scrubbed.
func (SensitiveHeaderRedactor) Redact(headers map[string]string) (redacted map[string]string, redactedNames []string) {
	if headers == nil {
		return nil, nil
	}
	redacted = make(map[string]string, len(headers))
	for name, value := range headers {
		if isSensitiveHeaderName(name) {
			redacted[name] = redactedValuePlaceholder
			redactedNames = append(redactedNames, name)
			continue
		}
		redacted[name] = value
	}
	return redacted, redactedNames
}

func isSensitiveHeaderName(name string) bool {
	lower := strings.ToLower(name)
	if _, ok := sensitiveHeaderExactNames[lower]; ok {
		return true
	}
	for _, pattern := range sensitiveHeaderNamePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
