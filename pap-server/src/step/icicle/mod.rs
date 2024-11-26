mod executor;
mod fuzzer;
mod sqlcorpus;

use super::{StepContext, StepExecutor};
use anyhow::{anyhow, bail};
use fuzzer::fuzz;

pub struct IcicleFuzzerExecutor;

impl StepExecutor for IcicleFuzzerExecutor {
    fn name(&self) -> String {
        "icicle-fuzzer".to_string()
    }

    fn execute(&self, ctx: &mut StepContext) -> anyhow::Result<()> {
        // Validate required arguments
        let project_name = ctx
            .get_arg("project")
            .ok_or(anyhow::anyhow!("missing `project` argument"))?;

        // Find and validate the target project
        let project = ctx.pipeline_status.config.projects
            .iter()
            .find(|p| p.name == project_name)
            .ok_or_else(|| anyhow!("project not found: {}", project_name))?;

        // Validate project configuration
        if project.binary.is_empty() {
            bail!("project {} has no binary specified", project_name);
        }

        // Validate architecture (must be ARM/Thumb based)
        if !project.arch.starts_with("thumb") && !project.arch.starts_with("arm") {
            bail!("project {} has unsupported architecture: {}", project_name, project.arch);
        }

        // Validate loader configuration
        let loader = project.loader
            .as_ref()
            .ok_or_else(|| anyhow!("project {} has no loader configuration", project_name))?;

        if loader.base_address == 0 {
            bail!("project {} has invalid base address: 0", project_name);
        }

        if loader.stack_address == 0 {
            bail!("project {} has invalid stack address: 0", project_name);
        }

        // Continue with existing validations
        let function = ctx
            .get_arg("function")
            .ok_or(anyhow::anyhow!("missing `function` argument"))?;

        let _function_addr = u64::from_str_radix(function.trim_start_matches("0x"), 16)
            .map_err(|_| anyhow::anyhow!("invalid function address: {}", function))?;

        ctx
            .get_arg("harness")
            .ok_or(anyhow::anyhow!("missing `harness` argument"))?;

        // Validate required IO configuration
        let required_io = ["input", "output", "solutions"];
        for io_field in required_io {
            if !ctx.has_io(io_field) {
                bail!("missing required IO field: {}", io_field);
            }
        }

        fuzz(ctx)?;

        Ok(())
    }
}
