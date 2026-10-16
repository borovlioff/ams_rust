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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Entity, SchemaConfig, SchemaVersionDef};
    use crate::port::{MockCache, MockEventBus, MockStorage};
    use serde_json::json;

    fn create_test_schema() -> SchemaConfig {
        SchemaConfig {
            name: "user".to_string(),
            current_version: 1,
            versions: vec![SchemaVersionDef {
                version: 1,
                fields: std::collections::HashMap::new(),
                migration_from: None,
            }],
        }
    }

    #[tokio::test]
    async fn test_service_get_with_cache_hit() {
        let mut mock_storage = MockStorage::new();
        let mut mock_cache = MockCache::new();
        let mock_bus = MockEventBus::new();

        let cached_entity = Entity {
            id: "cached-id".to_string(),
            data: [("name".to_string(), json!("Cached User"))].into_iter().collect(),
        };

        mock_cache
            .expect_get()
            .withf(|key: &str| key == "user:cached-id")
            .returning({
                let entity = cached_entity.clone();
                move |_| Ok(Some(serde_json::to_string(&entity).unwrap()))
            });

        mock_storage.expect_get().never();

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let result = service.get(&"cached-id".to_string()).await.unwrap();
        assert_eq!(result.id, "cached-id");
        assert_eq!(result.data["name"], json!("Cached User"));
    }

    #[tokio::test]
    async fn test_service_get_with_cache_miss() {
        let mut mock_storage = MockStorage::new();
        let mut mock_cache = MockCache::new();
        let mock_bus = MockEventBus::new();

        let db_entity = Entity {
            id: "db-id".to_string(),
            data: [("name".to_string(), json!("DB User"))].into_iter().collect(),
        };

        mock_cache
            .expect_get()
            .withf(|key: &str| key == "user:db-id")
            .returning(|_| Ok(None));

        mock_storage
            .expect_get()
            .withf(|id: &String| id == "db-id")
            .returning({
                let entity = db_entity.clone();
                move |_| Ok(Some((entity.clone(), 1)))
            });

        mock_cache
            .expect_set()
            .withf(|key: &str, _, _| key == "user:db-id")
            .returning(|_, _, _| Ok(()));

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let result = service.get(&"db-id".to_string()).await.unwrap();
        assert_eq!(result.id, "db-id");
    }

    #[tokio::test]
    async fn test_service_add_publishes_event() {
        let mut mock_storage = MockStorage::new();
        let mock_cache = MockCache::new();
        let mut mock_bus = MockEventBus::new();

        mock_storage
            .expect_insert()
            .returning(|_, _| Ok(()));

        mock_bus
            .expect_publish()
            .withf(|topic: &str, _, _| topic == "user.events")
            .returning(|_, _, _| Ok(()));

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let data = [("name".to_string(), json!("New User"))].into_iter().collect();
        let result = service.add(data).await.unwrap();
        assert!(!result.id.is_empty());
    }

    #[tokio::test]
    async fn test_service_update_invalidates_cache() {
        let mut mock_storage = MockStorage::new();
        let mut mock_cache = MockCache::new();
        let mock_bus = MockEventBus::new();

        mock_storage
            .expect_update()
            .withf(|entity: &Entity, _| entity.id == "update-id")
            .returning(|_, _| Ok(true));

        mock_cache
            .expect_del()
            .withf(|key: &str| key == "user:update-id")
            .returning(|_| Ok(()));

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let data = [("name".to_string(), json!("Updated User"))].into_iter().collect();
        let result = service.update("update-id".to_string(), data).await.unwrap();
        assert_eq!(result.id, "update-id");
    }

    #[tokio::test]
    async fn test_service_delete_invalidates_cache() {
        let mut mock_storage = MockStorage::new();
        let mut mock_cache = MockCache::new();
        let mock_bus = MockEventBus::new();

        mock_storage
            .expect_delete()
            .withf(|id: &String| id == "delete-id")
            .returning(|_| Ok(true));

        mock_cache
            .expect_del()
            .withf(|key: &str| key == "user:delete-id")
            .returning(|_| Ok(()));

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let result = service.delete("delete-id".to_string()).await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_service_get_all() {
        let mut mock_storage = MockStorage::new();
        let mock_cache = MockCache::new();
        let mock_bus = MockEventBus::new();

        let entities = vec![
            (
                Entity {
                    id: "id1".to_string(),
                    data: [("name".to_string(), json!("User 1"))].into_iter().collect(),
                },
                1u32,
            ),
            (
                Entity {
                    id: "id2".to_string(),
                    data: [("name".to_string(), json!("User 2"))].into_iter().collect(),
                },
                1u32,
            ),
        ];

        mock_storage
            .expect_get_all()
            .withf(|offset: &usize, limit: &usize| *offset == 0 && *limit == 10)
            .returning({
                let entities = entities.clone();
                move |_, _| Ok(entities.clone())
            });

        let schema = create_test_schema();
        let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
        let service = EntityService::new(
            Arc::new(mock_storage),
            Some(Arc::new(mock_cache)),
            Some(Arc::new(mock_bus)),
            migrator,
            "user".to_string(),
        );

        let result = service.get_all(0, 10).await.unwrap();
        assert_eq!(result.len(), 2);
    }
}