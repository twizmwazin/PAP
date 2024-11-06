use std::fs::File;

use anyhow::Result;
use pap_config::{load_config, Config};
use pap_executor::{executor::{Executor, SerialExecutor}, job::Job, project::Project, Pipeline};

fn main() -> Result<()> {
    // This is a simple example of how to use the pap-config crate to load a
    // project configuration file and convert it to a Project struct.
    let config_file = File::open("sample.yaml").expect("Could not open file");
    let config: Config = load_config(config_file).expect("Failed to parse config");

    let projects = config
        .projects
        .iter()
        .map(Project::from)
        .collect::<Vec<_>>();

    // Now we have to parse the jobs and steps
    let jobs = config.jobs.iter().map(Job::from).collect::<Vec<_>>();

    let pipeline = Pipeline { projects, jobs };

    let executor = SerialExecutor::default();

    // Check if the executor can run the pipeline
    executor.can_run_pipeline(&pipeline)?;

    executor.run_pipeline(&pipeline);

    Ok(())
}
