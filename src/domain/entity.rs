use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use base64::engine::{Engine, general_purpose::URL_SAFE_NO_PAD};

pub type EntityId = String;
pub type NewEntity = HashMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Cursor {
    pub fn encode(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        URL_SAFE_NO_PAD.encode(json)
    }
    
    pub fn decode(s: &str) -> Option<Self> {
        let json = URL_SAFE_NO_PAD.decode(s).ok()?;
        serde_json::from_slice(&json).ok()
    }
}

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
