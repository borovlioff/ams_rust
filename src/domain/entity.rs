use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type EntityId = String;
pub type NewEntity = HashMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    #[serde(flatten)]
    pub data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("Validation failed: {0}")]
    ValidationError(String),
    #[error("Entity not found")]
    NotFound,
    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}