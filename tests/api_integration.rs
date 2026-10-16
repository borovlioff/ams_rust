use std::sync::Arc;
use adaptive_microservice::{
    adapters::{PostgresStorage, RedisAdapter, RedisMode},
    application::EntityService,
    config::AppConfig,
    create_app_router,
    domain::{ConfigSchemaMigrator, SchemaConfig, SchemaVersionDef},
    port::{Cache, Storage}, 
    AppState,
};
use reqwest::StatusCode;
use serde_json::json;
use testcontainers_modules::{postgres::Postgres, redis::Redis, testcontainers::runners::AsyncRunner};



async fn setup_test_app() -> (
    reqwest::Client,
    String,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    // 1. Запускаем тестовые контейнеры (AsyncRunner)
    let pg_container = Postgres::default().start().await.unwrap();
    let redis_container = Redis::default().start().await.unwrap();

    // 2. Формируем connection strings (в testcontainers 0.23+ хост всегда 127.0.0.1)
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let pg_dsn = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", pg_port);
    let redis_url = format!("redis://127.0.0.1:{}", redis_port);

    // 3. Инициализируем адаптеры
    let storage = Arc::new(PostgresStorage::new(&pg_dsn, "test_entities").await.unwrap());
    storage.connect().await.unwrap();

    let cache = Arc::new(RedisAdapter::new(&redis_url, RedisMode::Cache { ttl_secs: 60 }).unwrap()) as Arc<dyn Cache>;

    // 4. Создаем сервис и AppState
    let cfg = AppConfig {
        api: adaptive_microservice::config::ApiConfig { host: "0.0.0.0".into(), port: 0 },
        postgres: adaptive_microservice::config::PgConfig { dsn: pg_dsn, table: "test_entities".into() },
        redis: Some(adaptive_microservice::config::RedisConfig { url: redis_url, mode: "cache".into(), ttl: 60 }),
        kafka: None, // Kafka не нужна для базовых CRUD тестов
        schemas: vec![SchemaConfig {
            name: "user".into(),
            current_version: 1,
            versions: vec![SchemaVersionDef {
                version: 1,
                fields: std::collections::HashMap::new(),
                migration_from: None,
            }],
        }],
    };

    let schema = cfg.schemas.first().cloned().unwrap();
    let migrator = Arc::new(ConfigSchemaMigrator::new(schema));
    
    let service = Arc::new(EntityService::new(storage, Some(cache), None, migrator, "user".into()));
    let state = AppState { service };

    // 5. Запускаем сервер на случайном порту
    let app = create_app_router(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (reqwest::Client::new(), base_url, pg_container, redis_container)
}

#[tokio::test]
async fn test_entity_lifecycle() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;

    let payload = json!({
        "name": "Integration Test User",
        "email": "test@example.com"
    });

    // 1. CREATE
    let resp = client.post(format!("{}/api/v1/entities", base_url))
        .json(&payload)
        .send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["name"], "Integration Test User");

    // 2. READ by ID
    let resp = client.get(format!("{}/api/v1/entities/{}", base_url, id)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["id"], id);

    // 3. LIST ALL
    let resp = client.get(format!("{}/api/v1/entities", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(list.len(), 1);

    // 4. UPDATE
    let update_payload = json!({"name": "Updated Name", "email": "updated@example.com"});
    let resp = client.put(format!("{}/api/v1/entities/{}", base_url, id))
        .json(&update_payload).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. DELETE
    let resp = client.delete(format!("{}/api/v1/entities/{}", base_url, id)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // 6. VERIFY DELETION
    let resp = client.get(format!("{}/api/v1/entities/{}", base_url, id)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_nonexistent_entity() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;
    let fake_id = "00000000-0000-0000-0000-000000000000";

    let resp = client.get(format!("{}/api/v1/entities/{}", base_url, fake_id)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_update_nonexistent_entity() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;
    let fake_id = "00000000-0000-0000-0000-000000000000";

    let update_payload = json!({"name": "Updated"});
    let resp = client
        .put(format!("{}/api/v1/entities/{}", base_url, fake_id))
        .json(&update_payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_delete_nonexistent_entity() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;
    let fake_id = "00000000-0000-0000-0000-000000000000";

    let resp = client
        .delete(format!("{}/api/v1/entities/{}", base_url, fake_id))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_pagination() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;

    // Create 5 entities
    for i in 0..5 {
        let payload = json!({"name": format!("User {}", i)});
        client
            .post(format!("{}/api/v1/entities", base_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
    }

    // Test limit
    let resp = client
        .get(format!("{}/api/v1/entities?limit=2", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(list.len(), 2);

    // Test offset
    let resp = client
        .get(format!("{}/api/v1/entities?offset=3&limit=10", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(list.len(), 2); // 5 - 3 = 2
}

#[tokio::test]
async fn test_api_health_check() {
    let (client, base_url, _pg, _redis) = setup_test_app().await;

    let resp = client
        .get(format!("{}/health", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "OK");
}