pub mod hello;
pub mod icicle;

use anyhow::Result;
use pap_api::{PipelineStatus, StepStatus};
use std::{collections::HashMap, sync::RwLock};
use tokio::runtime::Handle;

/// Context provided to a step during execution
pub struct StepContext<'a> {
    /// Step configuration and status
    pub status: &'a StepStatus,
    /// Overall pipeline configuration
    pub pipeline_status: &'a PipelineStatus,
    /// Runtime handle for async operations
    rt_handle: Handle,
    /// Log buffer
    log_buffer: RwLock<Vec<u8>>,
    /// Pipeline context
    context: &'a pap_api::Context,
}

impl<'a> StepContext<'a> {
    pub fn new(step: &'a StepStatus, pipeline_status: &'a PipelineStatus, context: &'a pap_api::Context) -> Self {
        Self {
            status: step,
            pipeline_status,
            rt_handle: Handle::current(),
            log_buffer: RwLock::new(Vec::new()),
            context,
        }
    }

    pub fn write_object(&self, namespace: &str, key: &[u8], data: &[u8]) -> Result<()> {
        self.rt_handle
            .block_on(async { crate::queries::put_object(namespace, key, data).await })
            .map_err(Into::into)
    }

    pub fn read_object(&self, namespace: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.rt_handle
            .block_on(async { crate::queries::get_object(namespace, key).await })
            .map_err(Into::into)
    }

    pub fn log(&self, message: &str) {
        self.log_buffer.write().expect("log lock poisoned").extend_from_slice(message.as_bytes());
        self.log_buffer.write().expect("log lock poisoned").push(b'\n');
    }

    pub(crate) fn get_log(&self) -> Vec<u8> {
        self.log_buffer.read().expect("log lock poisoned").clone()
    }

    // Convenience getters
    pub fn is_cancelled(&self) -> bool {
        self.rt_handle
            .block_on(async { crate::queries::is_step_cancelled(self.status.id).await })
            .unwrap_or(false)
    }

    pub fn has_arg(&self, name: &str) -> bool {
        self.status.config.args.contains_key(name)
    }

    pub fn get_arg(&self, name: &str) -> Option<&str> {
        self.status.config.args.get(name).map(|s| s.as_str())
    }

    pub fn has_io(&self, name: &str) -> bool {
        self.status.config.io.contains_key(name)
    }

    pub fn get_io(&self, name: &str) -> Option<&str> {
        self.status.config.io.get(name).map(|s| s.as_str())
    }

    /// Get a file from the context by name
    pub fn get_file(&self, name: &str) -> Option<&[u8]> {
        self.context.files().get(name).map(|v| v.as_slice())
    }
}

/// Trait that must be implemented by step executors
pub trait StepExecutor: Send + Sync {
    fn name(&self) -> String;
    fn execute(&self, ctx: &mut StepContext) -> Result<()>;
}

// This function is used to ensure that the StepExecutor trait is object safe
fn _assert_object_safe(_: &dyn StepExecutor) {}

/// Registry of available step executors
#[derive(Default)]
pub struct StepExecutorRegistry {
    executors: HashMap<String, Box<dyn StepExecutor>>,
}

impl StepExecutorRegistry {
    pub fn register<E: StepExecutor + 'static>(&mut self, executor: E) {
        self.executors
            .insert(executor.name().to_string(), Box::new(executor));
    }

    pub fn get(&self, name: &str) -> Option<&dyn StepExecutor> {
        self.executors.get(name).map(|e| e.as_ref())
    }
}

pub fn builtin_executors() -> StepExecutorRegistry {
    let mut registry = StepExecutorRegistry::default();

    registry.register(hello::HelloStepExecutor);
    registry.register(icicle::IcicleFuzzerExecutor);

    registry
}
