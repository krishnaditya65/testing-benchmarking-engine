//! [`SnippetRegistryClient`]: the port through which this worker publishes and fetches shared
//! script snippets ([`crate::sdk::ScriptManifest`] plus compiled `.wasm` bytes) from a central
//! registry, so a script one test author writes against the scripting SDK (see [`crate::sdk`])
//! can be reused by another team's pipeline instead of copy-pasted between DAG node configs.
//!
//! NOTE: the registry service itself — where published snippets are actually stored, how
//! access control/review works before a snippet is trusted, how it's discovered from
//! `workflow-canvas-frontend`'s DAG node config UI — is out of scope for this pass. This module
//! is the client *contract* only: the trait plus a stub HTTP-backed adapter implementing it
//! against an assumed REST shape, matching every other "seam documented, real backend deferred"
//! boundary in this repository's execution-fleet services (e.g. `chaos-ebpf-sidecar`'s
//! `KernelNetworkFaultApplier`/`PodKillTarget` seams).

use serde::{Deserialize, Serialize};

use crate::sdk::ScriptManifest;

/// A published snippet as returned by the registry: the manifest plus the compiled `.wasm`
/// module bytes implementing it. Kept as one type (rather than a manifest-only listing type and
/// a separate bytes-fetch call) because [`SnippetRegistryClient::fetch`] is intentionally the
/// single round trip a caller needs to go from "I know this snippet's name and version" to "I
/// have a module ready to hand to [`crate::sandbox::WasmSandboxHost::execute`]".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedSnippet {
    pub manifest: ScriptManifest,
    /// The compiled `.wasm` module bytes implementing `manifest`'s script, expected to satisfy
    /// the guest ABI documented on `WasmSandboxHost` (exports `memory`, `sandbox_alloc`, and
    /// whichever entry point the snippet's own documentation names).
    pub module_bytes: Vec<u8>,
}

/// Identifies one snippet listing in the registry: a name plus an exact version, since a
/// registry is expected to hold multiple published versions of the same named snippet
/// concurrently (semver-style — see [`crate::sdk::SemanticVersion`]) rather than only the
/// latest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnippetReference {
    pub name: String,
    pub version: crate::sdk::SemanticVersion,
}

/// The port between this worker and a central snippet registry. Kept intentionally small — this
/// worker only ever needs to publish what it (or a test author, via some other tool) produced,
/// and fetch what it's been told to run; listing/search/review workflows belong to whatever
/// registry-facing tool (a CLI, a `workflow-canvas-frontend` panel) a future pass builds against
/// this same trait.
pub trait SnippetRegistryClient {
    /// Publishes snippet to the registry. Implementations should treat re-publishing an
    /// identical `(name, version)` pair as an error — snippet versions are expected to be
    /// immutable once published, the same way a container image tag or a package-registry
    /// release is, so a script that pinned a specific version can trust it never changes under
    /// them.
    fn publish(&self, snippet: &PublishedSnippet) -> Result<(), SnippetRegistryError>;

    /// Fetches one exact, previously-published snippet.
    fn fetch(&self, reference: &SnippetReference) -> Result<PublishedSnippet, SnippetRegistryError>;
}

/// Every way a `SnippetRegistryClient` call can fail. Kept distinct from a generic error type so
/// callers can tell "this specific version was never published" (a caller bug — pin the right
/// version) apart from "the registry itself is unreachable" (retry-worthy) apart from
/// "this snippet requests a capability this worker's SDK build doesn't have" (a genuine
/// compatibility problem, not a transient failure).
#[derive(Debug, thiserror::Error)]
pub enum SnippetRegistryError {
    #[error("snippet {name}@{version} was not found in the registry", name = .reference.name, version = .reference.version)]
    NotFound { reference: SnippetReference },

    #[error("snippet {name}@{version} is already published; snippet versions are immutable", name = .reference.name, version = .reference.version)]
    AlreadyPublished { reference: SnippetReference },

    #[error("registry request failed: {0}")]
    TransportFailure(String),

    #[error("registry returned a malformed response: {0}")]
    MalformedResponse(String),
}

/// Stub HTTP-backed [`SnippetRegistryClient`] adapter. The registry service this talks to is out
/// of scope for this pass (see module doc comment); the request/response shapes below are this
/// adapter's assumed REST contract (`PUT /snippets/:name/:version`, `GET /snippets/:name/:version`),
/// documented here as the wiring point a real registry service should either match or this
/// adapter should be updated to match, whichever is built first.
pub struct HttpSnippetRegistryClient {
    /// The registry's base HTTP address, e.g. `"http://snippet-registry:8080"`.
    pub base_url: String,
    pub http_client: reqwest::blocking::Client,
}

impl HttpSnippetRegistryClient {
    /// Constructs the adapter with a default-configured blocking HTTP client. A blocking client
    /// (rather than async) matches this crate's synchronous `WasmSandboxHost::execute` call
    /// shape — publishing/fetching a snippet is not expected to be on any load-generation hot
    /// path, so there is no throughput reason to require an async runtime here.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http_client: reqwest::blocking::Client::new(),
        }
    }
}

impl SnippetRegistryClient for HttpSnippetRegistryClient {
    fn publish(&self, snippet: &PublishedSnippet) -> Result<(), SnippetRegistryError> {
        let url = format!(
            "{}/snippets/{}/{}",
            self.base_url, snippet.manifest.name, snippet.manifest.version
        );

        let response = self
            .http_client
            .put(&url)
            .json(snippet)
            .send()
            .map_err(|e| SnippetRegistryError::TransportFailure(e.to_string()))?;

        match response.status().as_u16() {
            200..=299 => Ok(()),
            409 => Err(SnippetRegistryError::AlreadyPublished {
                reference: SnippetReference {
                    name: snippet.manifest.name.clone(),
                    version: snippet.manifest.version,
                },
            }),
            status => Err(SnippetRegistryError::TransportFailure(format!(
                "registry returned unexpected status {status} publishing {}@{}",
                snippet.manifest.name, snippet.manifest.version
            ))),
        }
    }

    fn fetch(&self, reference: &SnippetReference) -> Result<PublishedSnippet, SnippetRegistryError> {
        let url = format!("{}/snippets/{}/{}", self.base_url, reference.name, reference.version);

        let response = self
            .http_client
            .get(&url)
            .send()
            .map_err(|e| SnippetRegistryError::TransportFailure(e.to_string()))?;

        match response.status().as_u16() {
            200 => response
                .json::<PublishedSnippet>()
                .map_err(|e| SnippetRegistryError::MalformedResponse(e.to_string())),
            404 => Err(SnippetRegistryError::NotFound { reference: reference.clone() }),
            status => Err(SnippetRegistryError::TransportFailure(format!(
                "registry returned unexpected status {status} fetching {}@{}",
                reference.name, reference.version
            ))),
        }
    }
}
