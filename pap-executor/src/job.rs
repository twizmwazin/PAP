use std::collections::HashMap;

use pap_config::ArgType;

pub struct Step {
    pub name: String,
    pub call: String,
    pub args: HashMap<String, ArgType>,
}

impl From<&pap_config::Step> for Step {
    fn from(step: &pap_config::Step) -> Self {
        Step {
            name: step.name.clone(),
            call: step.call.clone(),
            args: step.args.clone(),
        }
    }
}


pub struct Job {
    pub name: String,
    pub steps: Vec<Step>,
}

impl From<&pap_config::Job> for Job {
    fn from(job: &pap_config::Job) -> Self {
        Job {
            name: job.name.clone(),
            steps: job.steps.iter().map(Step::from).collect(),
        }
    }
}
