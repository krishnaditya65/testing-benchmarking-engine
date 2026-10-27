//! The scripting SDK: the typed `env.sdk_*` host-function surface user pre/post-request scripts
//! run against inside [`crate::sandbox::WasmSandboxHost`], plus [`ScriptManifest`], the
//! metadata shape a snippet needs to be publishable via [`crate::marketplace`].

mod script_manifest;
mod scripting_sdk_host_functions;

pub use script_manifest::{HostFunctionCapability, ScriptManifest, SemanticVersion};
pub use scripting_sdk_host_functions::{
    register_into_linker, HttpContext, ScriptExecutionContext, SdkAssertionOutcome, SdkLogLevel,
    SdkLogLine, SDK_VARIABLE_NOT_FOUND,
};
