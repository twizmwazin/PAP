mod default;
#[cfg(test)]
mod test;

use std::{collections::HashMap, io::Read};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};


/// A Config defines how to preform some analysis. The config has two sections:
/// projects and jobs.
///
/// Projects define the programs that will be used for analysis, including how
/// to load them into memory, and how to manage their environment. Currently
/// that is just MMIO, but in the future it could include operating system
/// configuration.
///
/// Jobs define the steps to take to analyze the projects. Currently, these
/// steps have to be built in to the executor. In the future, they could be
/// dynamically loaded, scripted, as a "module", similar to github actions,
/// "actions", or written directly in the config for short routines.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    /// This defines the projects that will be used by jobs.
    pub projects: Vec<Project>,
    /// This defines the jobs that will be run.
    pub jobs: Vec<Job>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Project {
    /// The name of the project. This is used to reference the project in jobs.
    pub name: String,
    /// The path to the binary to load, relative to the config file.
    pub binary: String,
    // TODO: there is a crate for these, use it.
    /// The architecture of the binary, as an llvm target triple.
    pub arch: String,
    /// The loader configuration for the project.
    pub loader: Option<LoaderConfig>,
    /// The MMIO configuration for the project.
    pub mmio: Vec<MMIOEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct LoaderConfig {
    pub base_address: u64,
    pub stack_address: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MMIOEntry {
    pub address: u64,
    #[serde(default = "default::one")]
    pub size: u64,
    pub handler: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Job {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum ArgType {
    Bool(bool),
    Int(i64),
    String(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Step {
    pub name: String,
    pub call: String,
    pub args: HashMap<String, ArgType>,
}

pub fn load_config(reader: impl Read) -> Result<Config, serde_yaml::Error> {
    serde_yaml::from_reader(reader)
}
