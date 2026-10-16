use async_trait::async_trait;
use crate::domain::{DomainError, Entity, EntityId, SchemaVersion};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Storage: Send + Sync {
    async fn connect(&self) -> anyhow::Result<()>;
    async fn disconnect(&self) -> anyhow::Result<()>;
    async fn get(&self, id: &EntityId) -> Result<Option<(Entity, SchemaVersion)>, DomainError>;
    async fn get_all(&self, offset: usize, limit: usize) -> Result<Vec<(Entity, SchemaVersion)>, DomainError>;
    async fn insert(&self, entity: &Entity, version: SchemaVersion) -> Result<(), DomainError>;
    async fn update(&self, entity: &Entity, version: SchemaVersion) -> Result<bool, DomainError>;
    async fn delete(&self, id: &EntityId) -> Result<bool, DomainError>;
}