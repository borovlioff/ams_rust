use std::sync::Arc;
use adaptive_microservice::{
    adapters::{KafkaBus, PostgresStorage, RedisAdapter, RedisMode},
    application::EntityService,
    config::AppConfig,
    create_app_router,
    domain::ConfigSchemaMigrator,
    port::{Cache, EventBus, Storage},
    AppState,
};
use tokio::net::TcpListener;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Инициализация JSON-логирования (СТРОГО ОДИН РАЗ)
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    
    fmt()
        .json()
        .with_target(false)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(filter)
        .init();

    // 2. Чтение конфигурации
    let cfg_raw = std::fs::read_to_string("config/config.yaml")
        .or_else(|_| std::fs::read_to_string("config.yaml"))?;
    let cfg: AppConfig = serde_yaml::from_str(&cfg_raw)?;

    // 3. Инициализация адаптеров
    let storage = Arc::new(PostgresStorage::new(&cfg.postgres.dsn, &cfg.postgres.table).await?);
    storage.connect().await?;

    let cache: Option<Arc<dyn Cache>> = cfg.redis.as_ref().map(|r| {
        let mode = if r.mode == "storage" {
            RedisMode::Storage
        } else {
            RedisMode::Cache { ttl_secs: r.ttl }
        };
        Arc::new(RedisAdapter::new(&r.url, mode).unwrap()) as Arc<dyn Cache>
    });

    let bus: Option<Arc<dyn EventBus>> = cfg.kafka.as_ref().map(|k| {
        Arc::new(KafkaBus::new(&k.brokers, &k.group).unwrap()) as Arc<dyn EventBus>
    });

    let user_schema = cfg
        .schemas
        .iter()
        .find(|s| s.name == "user")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("user schema not found"))?;

    let migrator = Arc::new(ConfigSchemaMigrator::new(user_schema));
    let service = Arc::new(EntityService::new(storage, cache, bus, migrator, "user".to_string()));

    // 4. Запуск сервера
    let state = AppState { service };
    let app = create_app_router(state);

    let addr = format!("{}:{}", cfg.api.host, cfg.api.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("API listening on {}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}