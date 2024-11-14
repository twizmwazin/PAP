use crate::queries;
use anyhow::Result;
use sqlx::SqlitePool;

pub struct SqlStorage {
    pool: SqlitePool,
}

impl SqlStorage {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn read(&self, namespace: &str, key: &str) -> Result<Vec<u8>> {
        queries::get_object(&self.pool, namespace, key.as_bytes())
            .await
            .map_err(Into::into)
    }

    pub async fn write(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        queries::put_object(&self.pool, namespace, key.as_bytes(), data)
            .await
            .map_err(Into::into)
    }
}
