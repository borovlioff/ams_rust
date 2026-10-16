use async_trait::async_trait;
use serde::Serialize;
use std::sync::Arc;

pub type EventHandler = Arc<dyn Fn(String, String) + Send + Sync + 'static>;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, topic: &str, key: &str, payload: &str) -> anyhow::Result<()>;
    async fn subscribe(&self, topic: &str, handler: EventHandler) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize)]
pub struct DomainEvent {
    pub event_type: String,
    pub entity_id: String,
    pub schema_version: u32,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}