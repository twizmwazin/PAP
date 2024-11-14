use std::fs::File;

use serde_yaml::from_reader;

use crate::*;

#[test]
fn test_load_sample_config() {
    let reader = File::open("../sample.yaml").expect("Could not open file");
    let config: Config = from_reader(reader).expect("Failed to parse config");

    assert_eq!(config.projects.len(), 1);
    assert_eq!(config.projects[0].name, "testbin");
    assert_eq!(config.projects[0].binary, "test.bin");
    assert_eq!(config.jobs.len(), 1);
    assert_eq!(config.jobs[0].steps[0].args["function"], "0x8074e50");
}
