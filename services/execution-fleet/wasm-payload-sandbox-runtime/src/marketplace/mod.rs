//! The snippet marketplace client: publishing/fetching shared, versioned scripting-SDK snippets
//! (see [`crate::sdk`]) to/from a central registry. See [`snippet_registry_client`]'s module doc
//! comment for what is, and is not, in scope for this pass.

mod snippet_registry_client;

pub use snippet_registry_client::{
    HttpSnippetRegistryClient, PublishedSnippet, SnippetRegistryClient, SnippetRegistryError,
    SnippetReference,
};
