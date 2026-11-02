use std::sync::Arc;
use crate::domain::*;
use crate::port::*;

pub struct EntityService {
    storage: Arc<dyn Storage>,
    cache: Option<Arc<dyn Cache>>,
    bus: Option<Arc<dyn EventBus>>,
    migrator: Arc<dyn SchemaMigrator>,
    schema_name: String,
}

impl EntityService {
    pub fn new(
        storage: Arc<dyn Storage>,
        cache: Option<Arc<dyn Cache>>,
        bus: Option<Arc<dyn EventBus>>,
        migrator: Arc<dyn SchemaMigrator>,
        schema_name: String,
    ) -> Self {
        Self { storage, cache, bus, migrator, schema_name }
    }

    pub async fn get(&self, id: &EntityId) -> Result<Entity, DomainError> {
        if let Some(cache) = &self.cache {
            if let Ok(Some(cached)) = cache.get(&format!("{}:{}", self.schema_name, id)).await {
                if let Ok(e) = serde_json::from_str::<Entity>(&cached) {
                    return Ok(e);
                }
            }
        }
        let (mut entity, stored_version) = self.storage.get(id).await?.ok_or(DomainError::NotFound)?;
        if stored_version < self.migrator.current_version() {
            entity.data = self.migrator
                .migrate(
                    stored_version,
                    self.migrator.current_version(),
                    serde_json::to_value(&entity.data).unwrap_or_default(),
                )
                .map(|v| serde_json::from_value(v).unwrap_or_default())
                .map_err(|e| DomainError::ValidationError(e.to_string()))?;
        }
        if let Some(cache) = &self.cache {
            if let Ok(json) = serde_json::to_string(&entity) {
                let _ = cache.set(&format!("{}:{}", self.schema_name, id), &json, None).await;
            }
        }
        Ok(entity)
    }

    pub async fn get_all(&self, offset: usize, limit: usize) -> Result<Vec<Entity>, DomainError> {
        let rows = self.storage.get_all(offset, limit).await?;
        Ok(rows.into_iter().map(|(e, _)| e).collect())
    }
    
    pub async fn get_all_paginated(&self, offset: usize, limit: usize) -> Result<(Vec<Entity>, usize), DomainError> {
        let total = self.storage.count().await?;
        let rows = self.storage.get_all(offset, limit).await?;
        let entities = rows.into_iter().map(|(e, _)| e).collect();
        Ok((entities, total))
    }
    
    pub async fn get_all_cursor(&self, cursor: Option<String>, limit: usize) -> Result<(Vec<Entity>, Option<String>, bool), DomainError> {
        let (rows, has_more) = self.storage.get_all_cursor(cursor, limit).await?;
        let entities: Vec<Entity> = rows.iter().map(|(e, _, _)| e.clone()).collect();
        
        let next_cursor = if let Some((_, _, created_at)) = rows.last() {
            if has_more {
                if let Some(last_entity) = entities.last() {
                    Some(Cursor {
                        id: last_entity.id.clone(),
                        created_at: *created_at,
                    }.encode())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        
        Ok((entities, next_cursor, has_more))
    }

    pub async fn add(&self, data: NewEntity) -> Result<Entity, DomainError> {
        let entity = Entity { id: uuid::Uuid::new_v4().to_string(), data };
        let v = self.migrator.current_version();
        self.storage.insert(&entity, v).await?;
        if let Some(bus) = &self.bus {
            let event = serde_json::to_string(&DomainEvent {
                event_type: "EntityCreated".into(),
                entity_id: entity.id.clone(),
                schema_version: v,
                payload: serde_json::to_value(&entity.data).unwrap_or_default(),
                timestamp: chrono::Utc::now(),
            }).unwrap_or_default();
            let _ = bus.publish(&format!("{}.events", self.schema_name), &entity.id, &event).await;
        }
        Ok(entity)
    }

    pub async fn update(&self, id: EntityId, data: NewEntity) -> Result<Entity, DomainError> {
        let entity = Entity { id, data };
        let v = self.migrator.current_version();
        self.storage.update(&entity, v).await?.then_some(()).ok_or(DomainError::NotFound)?;
        if let Some(cache) = &self.cache {
            let _ = cache.del(&format!("{}:{}", self.schema_name, entity.id)).await;
        }
        Ok(entity)
    }

    pub async fn delete(&self, id: EntityId) -> Result<bool, DomainError> {
        let ok = self.storage.delete(&id).await?;
        if ok {
            if let Some(cache) = &self.cache {
                let _ = cache.del(&format!("{}:{}", self.schema_name, id)).await;
            }
        }
        Ok(ok)
    }
}
