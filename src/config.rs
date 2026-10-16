use serde::Deserialize;
use crate::domain::SchemaConfig;

#[derive(Deserialize)]
pub struct AppConfig {
    pub api: ApiConfig,
    pub postgres: PgConfig,
    pub redis: Option<RedisConfig>,
    pub kafka: Option<KafkaConfig>,
    pub schemas: Vec<SchemaConfig>,
}

#[derive(Deserialize)]
pub struct ApiConfig { pub host: String, pub port: u16 }
#[derive(Deserialize)]
pub struct PgConfig { pub dsn: String, pub table: String }
#[derive(Deserialize)]
pub struct RedisConfig { pub url: String, pub mode: String, pub ttl: u64 }
#[derive(Deserialize)]
pub struct KafkaConfig { pub brokers: String, pub group: String }