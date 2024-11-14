use std::{collections::HashMap, path::PathBuf};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::Config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Context {
    pub config: Config,
    pub files: HashMap<String, Vec<u8>>,
}

impl Context {
    pub fn build_with_config(config: Config, path: PathBuf) -> Result<Self> {
        let files = find_files_in_config(&config, path)?;
        Ok(Self { config, files })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn files(&self) -> &HashMap<String, Vec<u8>> {
        &self.files
    }
}

fn find_files_in_config(config: &Config, base_path: PathBuf) -> Result<HashMap<String, Vec<u8>>> {
    let mut files = HashMap::new();

    for project in &config.projects {
        let full_path = base_path.join(&project.binary);
        let data = std::fs::read(&full_path)
            .map_err(|e| anyhow!("Failed to open {}: {}", full_path.to_string_lossy(), e))?;
        files.insert(project.binary.clone(), data);
    }

    Ok(files)
}
