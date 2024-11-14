pub mod hello;

use crate::storage::SqlStorage;
use anyhow::Result;
use std::collections::HashMap;

/// Context provided to a step during execution
pub struct StepContext<'a> {
    /// Arguments from the config
    pub args: &'a HashMap<String, String>,
    /// IO storage interface
    pub storage: &'a SqlStorage,
    /// Log buffer
    log_buffer: Vec<u8>,
}

impl<'a> StepContext<'a> {
    pub fn new(args: &'a HashMap<String, String>, storage: &'a SqlStorage) -> Self {
        Self {
            args,
            storage,
            log_buffer: Vec::new(),
        }
    }

    pub fn log(&mut self, message: &str) {
        self.log_buffer.extend_from_slice(message.as_bytes());
        self.log_buffer.push(b'\n');
    }

    pub(crate) fn take_log(self) -> Vec<u8> {
        self.log_buffer
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

    registry
}
