use super::{StepContext, StepExecutor};

pub struct HelloStepExecutor;

impl StepExecutor for HelloStepExecutor {
    fn name(&self) -> String {
        "hello".to_string()
    }

    fn execute(&self, ctx: &mut StepContext) -> anyhow::Result<()> {
        let name = ctx
            .get_arg("name")
            .ok_or(anyhow::anyhow!("missing `name` argument"))?;
        let message = format!("Hello, {}!", name);
        ctx.log(&message);
        Ok(())
    }
}
