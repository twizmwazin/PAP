pub mod error;
pub mod executor;
pub mod job;
pub mod project;

pub struct Pipeline {
    pub projects: Vec<project::Project>,
    pub jobs: Vec<job::Job>,
}
