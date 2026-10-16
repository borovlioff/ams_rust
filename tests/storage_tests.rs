use adaptive_microservice::{
    adapters::{PostgresStorage, RedisAdapter, RedisMode},
    domain::Entity,
    port::{Cache, Storage},
};
use serde_json::json;
use testcontainers_modules::{postgres::Postgres, redis::Redis, testcontainers::runners::AsyncRunner};

#[tokio::test]
async fn test_postgres_storage_lifecycle() {
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();
    let pg_dsn = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", pg_port);

    let storage = PostgresStorage::new(&pg_dsn, "test_entities").await.unwrap();
    
    storage.connect().await.unwrap();

    let entity = Entity {
        id: "test-id-1".to_string(),
        data: [("name".to_string(), json!("Alice"))].into_iter().collect(),
    };
    storage.insert(&entity, 1).await.unwrap();

    let result = storage.get(&"test-id-1".to_string()).await.unwrap();
    assert!(result.is_some());
    let (fetched_entity, version) = result.unwrap();
    assert_eq!(fetched_entity.id, "test-id-1");
    assert_eq!(version, 1);

    let all = storage.get_all(0, 10).await.unwrap();
    assert_eq!(all.len(), 1);

    let updated_entity = Entity {
        id: "test-id-1".to_string(),
        data: [("name".to_string(), json!("Bob"))].into_iter().collect(),
    };
    let updated = storage.update(&updated_entity, 2).await.unwrap();
    assert!(updated);

    let result = storage.get(&"test-id-1".to_string()).await.unwrap();
    let (fetched_entity, version) = result.unwrap();
    assert_eq!(fetched_entity.data["name"], json!("Bob"));
    assert_eq!(version, 2);

    let deleted = storage.delete(&"test-id-1".to_string()).await.unwrap();
    assert!(deleted);

    let result = storage.get(&"test-id-1".to_string()).await.unwrap();
    assert!(result.is_none());

    storage.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_redis_cache_mode() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{}", redis_port);

    let cache = RedisAdapter::new(&redis_url, RedisMode::Cache { ttl_secs: 60 }).unwrap();

    cache.ping().await.unwrap();

    cache.set("key1", "value1", Some(60)).await.unwrap();

    let result = cache.get("key1").await.unwrap();
    assert_eq!(result, Some("value1".to_string()));

    cache.del("key1").await.unwrap();
    let result = cache.get("key1").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_redis_storage_mode() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{}", redis_port);

    let cache = RedisAdapter::new(&redis_url, RedisMode::Storage).unwrap();

    cache.set("key2", "value2", None).await.unwrap();

    let result = cache.get("key2").await.unwrap();
    assert_eq!(result, Some("value2".to_string()));
}