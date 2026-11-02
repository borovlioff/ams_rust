use async_trait::async_trait;
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use sqlx::types::Json;
use crate::domain::{DomainError, Entity, EntityId, SchemaVersion, Cursor};
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
            "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, data JSONB NOT NULL, schema_version INT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())",
            self.table
        );
        sqlx::query(&q).execute(&self.pool).await?;
        
        // Add created_at column if it doesn't exist (migration)
        let alter_q = format!(
            "DO $$ BEGIN ALTER TABLE {} ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW(); EXCEPTION WHEN duplicate_column THEN END $$",
            self.table
        );
        let _ = sqlx::query(&alter_q).execute(&self.pool).await;
        
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
    
    async fn get_all_cursor(&self, cursor: Option<String>, limit: usize) -> Result<(Vec<(Entity, SchemaVersion, chrono::DateTime<chrono::Utc>)>, bool), DomainError> {
        let (rows, has_more) = if let Some(cursor_str) = cursor {
            let cursor = Cursor::decode(&cursor_str)
                .ok_or_else(|| DomainError::ValidationError("Invalid cursor".to_string()))?;
            
            let q = format!(
                "SELECT id, data, schema_version, created_at FROM {} WHERE (created_at, id) < ($1, $2) ORDER BY created_at DESC, id DESC LIMIT $3",
                self.table,
            );
            let rows = sqlx::query(&q)
                .bind(cursor.created_at)
                .bind(&cursor.id)
                .bind((limit + 1) as i64)
                .fetch_all(&self.pool)
                .await?;
            
            let has_more = rows.len() > limit;
            let rows: Vec<_> = rows.into_iter().take(limit).collect();
            (rows, has_more)
        } else {
            let q = format!(
                "SELECT id, data, schema_version, created_at FROM {} ORDER BY created_at DESC, id DESC LIMIT {}",
                self.table,
                limit + 1
            );
            let rows = sqlx::query(&q).fetch_all(&self.pool).await?;
            
            let has_more = rows.len() > limit;
            let rows: Vec<_> = rows.into_iter().take(limit).collect();
            (rows, has_more)
        };
        
        let mut result = Vec::new();
        for r in rows {
            let id: String = r.try_get("id")?;
            let Json(data) = r.try_get::<Json<_>, _>("data")?;
            let v: i32 = r.try_get("schema_version")?;
            let created_at: chrono::DateTime<chrono::Utc> = r.try_get("created_at")?;
            result.push((Entity { id, data }, v as u32, created_at));
        }
        
        Ok((result, has_more))
    }
    
    async fn insert(&self, e: &Entity, v: SchemaVersion) -> Result<(), DomainError> {
        let q = format!("INSERT INTO {} (id, data, schema_version, created_at) VALUES ($1, $2, $3, NOW())", self.table);
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

    async fn count(&self) -> Result<usize, DomainError> {
        let q = format!("SELECT COUNT(*) as count FROM {}", self.table);
        let row = sqlx::query(&q).fetch_one(&self.pool).await?;
        let count: i64 = row.try_get("count")?;
        Ok(count as usize)
    }
}
