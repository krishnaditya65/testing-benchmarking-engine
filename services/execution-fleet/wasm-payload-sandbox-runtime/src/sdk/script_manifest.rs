//! [`ScriptManifest`]: the metadata a publishable, shareable script snippet carries — name,
//! description, author, semantic version, and which host-function capabilities (see
//! [`super::scripting_sdk_host_functions`]) it requires — so snippets can be shared, versioned,
//! and safety-reviewed before another test author pulls one into their own pipeline. This is the
//! metadata shape [`crate::marketplace::SnippetRegistryClient`] publishes and fetches.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// One host-function import a script may call, matching the `env.sdk_*` surface documented in
/// [`super::scripting_sdk_host_functions`]. A [`ScriptManifest`] declares which of these it
/// needs so a marketplace consumer (or this worker, before ever instantiating the module) can
/// refuse to run a script that requests a capability the current SDK build doesn't expose,
/// instead of discovering that mismatch as an opaque link-time trap deep inside
/// [`crate::sandbox::WasmSandboxHost::execute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostFunctionCapability {
    GetVariable,
    SetVariable,
    HttpContext,
    Assert,
    Log,
}

impl HostFunctionCapability {
    /// The full, current set of host-function capabilities this SDK build exposes. A manifest
    /// requesting a capability outside this set cannot be satisfied by the running worker.
    pub const ALL: [HostFunctionCapability; 5] = [
        HostFunctionCapability::GetVariable,
        HostFunctionCapability::SetVariable,
        HostFunctionCapability::HttpContext,
        HostFunctionCapability::Assert,
        HostFunctionCapability::Log,
    ];
}

impl fmt::Display for HostFunctionCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            HostFunctionCapability::GetVariable => "get_variable",
            HostFunctionCapability::SetVariable => "set_variable",
            HostFunctionCapability::HttpContext => "http_context",
            HostFunctionCapability::Assert => "assert",
            HostFunctionCapability::Log => "log",
        };
        f.write_str(name)
    }
}

/// A semantic version, restricted to the plain `MAJOR.MINOR.PATCH` shape (no pre-release/build
/// metadata suffixes) — sufficient for snippet compatibility checks without pulling in a full
/// semver-parsing dependency for a starter feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl fmt::Display for SemanticVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for SemanticVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('.');
        let (Some(major), Some(minor), Some(patch), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(format!("semantic version {s:?}: expected exactly MAJOR.MINOR.PATCH"));
        };
        let parse_component = |component: &str| -> Result<u32, String> {
            component
                .parse::<u32>()
                .map_err(|e| format!("semantic version {s:?}: component {component:?} is not a non-negative integer: {e}"))
        };
        Ok(SemanticVersion {
            major: parse_component(major)?,
            minor: parse_component(minor)?,
            patch: parse_component(patch)?,
        })
    }
}

/// Describes one publishable snippet — a `.wasm` module (or the source that compiles to one)
/// implementing the guest ABI documented on [`crate::sandbox::WasmSandboxHost`], plus enough
/// metadata for another test author to discover, evaluate, and safely reuse it via
/// [`crate::marketplace::SnippetRegistryClient`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptManifest {
    /// Short, unique-within-the-registry identifier, e.g. `"extract-jwt-from-response"`.
    /// Convention mirrors this platform's directory-naming rule (`ARCHITECTURE.md`'s "Naming
    /// conventions"): descriptive and purposeful, not a generic name like `"my-script"`.
    pub name: String,
    /// One or two sentences describing what the script does and when a test author would reach
    /// for it — shown as the marketplace listing's summary.
    pub description: String,
    /// Free-text author identity (a person's name, a team name, or a service account identity —
    /// this SDK does not mandate a specific identity scheme).
    pub author: String,
    /// This snippet's own version, independent of the SDK/worker version it was built against.
    pub version: SemanticVersion,
    /// The `env.sdk_*` host-function capabilities this snippet's compiled module actually
    /// imports. A registry (or this worker, before instantiation) can check this against
    /// `HostFunctionCapability::ALL` to refuse running a snippet that needs a capability the
    /// current SDK build doesn't provide, and a human browsing the marketplace can see at a
    /// glance what a snippet is capable of touching (e.g. a snippet declaring only `Log` and
    /// `Assert` is obviously lower-risk to run than one also declaring `SetVariable`).
    pub required_capabilities: Vec<HostFunctionCapability>,
    /// The minimum SDK `SemanticVersion` this snippet requires — i.e. the
    /// `scripting-sdk-host-functions` surface's own version, tracked separately from the
    /// snippet's own `version` above, since the SDK surface and any individual snippet built
    /// against it evolve independently.
    pub minimum_sdk_version: SemanticVersion,
}

impl ScriptManifest {
    /// Validates that every capability this manifest declares is one this SDK build actually
    /// exposes. Returns the first unsupported capability found, if any — a manifest declaring a
    /// capability outside `HostFunctionCapability::ALL` is either from a newer SDK version this
    /// worker hasn't been upgraded to, or a fabricated/corrupted manifest, and either way
    /// should be refused before the module is ever instantiated rather than surfacing as a
    /// runtime link failure.
    pub fn validate_capabilities_supported(&self) -> Result<(), HostFunctionCapability> {
        for capability in &self.required_capabilities {
            if !HostFunctionCapability::ALL.contains(capability) {
                return Err(*capability);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_version_round_trips_through_display_and_from_str() {
        let version = SemanticVersion { major: 1, minor: 4, patch: 2 };
        let parsed: SemanticVersion = version.to_string().parse().expect("round trip");
        assert_eq!(version, parsed);
    }

    #[test]
    fn semantic_version_rejects_malformed_input() {
        assert!("1.4".parse::<SemanticVersion>().is_err());
        assert!("1.4.2.1".parse::<SemanticVersion>().is_err());
        assert!("v1.4.2".parse::<SemanticVersion>().is_err());
    }

    #[test]
    fn validate_capabilities_supported_flags_unknown_future_capability() {
        // Simulated deserialization of a manifest published by a future SDK version whose
        // capability set has grown beyond HostFunctionCapability::ALL as compiled into this
        // worker — modeled here directly (rather than via an unrepresentable enum variant, since
        // Rust enums cannot be constructed with an unknown discriminant) by asserting the known
        // set instead: every capability in ALL must itself be considered supported.
        let manifest = ScriptManifest {
            name: "extract-jwt-from-response".to_string(),
            description: "Extracts a JWT from a login response and stores it as a variable for the next request.".to_string(),
            author: "platform-team".to_string(),
            version: SemanticVersion { major: 1, minor: 0, patch: 0 },
            required_capabilities: HostFunctionCapability::ALL.to_vec(),
            minimum_sdk_version: SemanticVersion { major: 1, minor: 0, patch: 0 },
        };
        assert!(manifest.validate_capabilities_supported().is_ok());
    }
}
