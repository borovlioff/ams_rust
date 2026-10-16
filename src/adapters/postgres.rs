use async_trait::async_trait;
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use sqlx::types::Json;
use crate::domain::{DomainError, Entity, EntityId, SchemaVersion};
use crate::port::Storage;

pub struct PostgresStorage {
    pool: PgPool,
    table: String,
}

impl PostgresStorage {
    pub async fn new(dsn: &str, table: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().max_connections(10).connect(dsn).await?;
        Ok(Self { pool, table: table.to_string() })
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn connect(&self) -> anyhow::Result<()> {
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, data JSONB NOT NULL, schema_version INT NOT NULL)",
            self.table
        );
        sqlx::query(&q).execute(&self.pool).await?;
        Ok(())
    }
    
    async fn disconnect(&self) -> anyhow::Result<()> {
        self.pool.close().await;
        Ok(())
    }
    
    async fn get(&self, id: &EntityId) -> Result<Option<(Entity, SchemaVersion)>, DomainError> {
        let q = format!("SELECT id, data, schema_version FROM {} WHERE id = $1", self.table);
        let row = sqlx::query(&q).bind(id).fetch_optional(&self.pool).await?;
        match row {
            Some(r) => {
                let id: String = r.try_get("id")?;
                let Json(data) = r.try_get::<Json<_>, _>("data")?;
                let schema_version: i32 = r.try_get("schema_version")?;
                Ok(Some((
                    Entity { id, data },
                    schema_version as u32,
                )))
            }
            None => Ok(None),
        }
    }
    
    async fn get_all(&self, o: usize, l: usize) -> Result<Vec<(Entity, SchemaVersion)>, DomainError> {
        let q = format!("SELECT id, data, schema_version FROM {} LIMIT {} OFFSET {}", self.table, l, o);
        let rows = sqlx::query(&q).fetch_all(&self.pool).await?;
        let mut result = Vec::new();
        for r in rows {
            let id: String = r.try_get("id")?;
            let Json(data) = r.try_get::<Json<_>, _>("data")?;
            let v: i32 = r.try_get("schema_version")?;
            result.push((Entity { id, data }, v as u32));
        }
        Ok(result)
    }
    
    async fn insert(&self, e: &Entity, v: SchemaVersion) -> Result<(), DomainError> {
        let q = format!("INSERT INTO {} (id, data, schema_version) VALUES ($1, $2, $3)", self.table);
        let data_json = serde_json::to_value(&e.data)?;
        sqlx::query(&q)
            .bind(&e.id)
            .bind(Json(data_json))
            .bind(v as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    
    async fn update(&self, e: &Entity, v: SchemaVersion) -> Result<bool, DomainError> {
        let q = format!("UPDATE {} SET data = $2, schema_version = $3 WHERE id = $1", self.table);
        let data_json = serde_json::to_value(&e.data)?;
        let r = sqlx::query(&q)
            .bind(&e.id)
            .bind(Json(data_json))
            .bind(v as i32)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }
    
    async fn delete(&self, id: &EntityId) -> Result<bool, DomainError> {
        let q = format!("DELETE FROM {} WHERE id = $1", self.table);
        let r = sqlx::query(&q).bind(id).execute(&self.pool).await?;
        Ok(r.rows_affected() > 0)
    }
}