use std::collections::HashMap;

use pap_config::ArgType;

use crate::{error::ExecutorError, Pipeline};

trait ExecutorCommand {
    fn name(&self) -> &str;
    fn execute(&self, executor: &dyn Executor, job: &str, args: &HashMap<String, ArgType>);
}

pub trait Executor {
    fn run_job(&self, pipeline: &Pipeline, job: &str);
    fn has_command(&self, name: &str) -> bool;

    fn can_run_pipeline(&self, pipeline: &Pipeline) -> Result<(), ExecutorError> {
        let commands = pipeline
            .jobs
            .iter()
            .flat_map(|job| job.steps.iter().map(|step| step.call.as_str()))
            .collect::<Vec<_>>();
        for command in commands {
            if !self.has_command(command) {
                return Err(ExecutorError::CommandNotFound(command.to_string()));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SerialExecutor {
    commands: HashMap<String, Box<dyn ExecutorCommand>>,
}

impl SerialExecutor {
    pub fn run_pipeline(&self, pipeline: &Pipeline) {
        for job in pipeline.jobs.iter() {
            self.run_job(pipeline, &job.name);
        }
    }
}

impl Executor for SerialExecutor {
    fn run_job(&self, pipeline: &Pipeline, job: &str) {
        for step in pipeline
            .jobs
            .iter()
            .find(|j| j.name == job)
            .unwrap()
            .steps
            .iter()
        {
            let command = self.commands.get(step.call.as_str()).unwrap();
            command.execute(self, job, &step.args);
        }
    }

    fn has_command(&self, name: &str) -> bool {
        self.commands.contains_key(name)
    }
}
