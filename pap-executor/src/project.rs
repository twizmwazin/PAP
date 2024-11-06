use pap_config::LoaderConfig;


pub struct Project {
    name: String,
    binary: String,
    arch: String,
    loader: Option<LoaderConfig>,
    // TODO: use rhai to parse this
    // mmio: Vec<MMIOEntry>,
}

impl From<&pap_config::Project> for Project {
    fn from(project: &pap_config::Project) -> Self {
        Project {
            name: project.name.clone(),
            binary: project.binary.clone(),
            arch: project.arch.clone(),
            loader: project.loader.clone(),
        }
    }
}
