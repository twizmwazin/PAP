use anyhow::{anyhow, Result};
use sqlx::SqlitePool;
use std::sync::RwLock;

static DB_POOL: RwLock<Option<SqlitePool>> = RwLock::new(None);

pub fn init_pool(pool: SqlitePool) -> Result<()> {
    DB_POOL.write().map_err(|e| anyhow!("{}", e))?.replace(pool);
    Ok(())
}

pub fn with_pool() -> Result<SqlitePool> {
    Ok(DB_POOL
        .read()
        .map_err(|e| anyhow!("{}", e))?
        .as_ref()
        .ok_or(anyhow!("Database pool not initialized"))?
        .clone()
    )
}
