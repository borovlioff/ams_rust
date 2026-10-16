use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type SchemaVersion = u32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub name: String,
    pub current_version: SchemaVersion,
    pub versions: Vec<SchemaVersionDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionDef {
    pub version: SchemaVersion,
    pub fields: HashMap<String, FieldDefinition>,
    pub migration_from: Option<SchemaVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub required: bool,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub pattern: Option<String>,
}

pub trait SchemaMigrator: Send + Sync {
    fn migrate(&self, from: SchemaVersion, to: SchemaVersion, data: serde_json::Value) -> anyhow::Result<serde_json::Value>;
    fn current_version(&self) -> SchemaVersion;
}

pub struct ConfigSchemaMigrator {
    config: SchemaConfig,
}

impl ConfigSchemaMigrator {
    pub fn new(config: SchemaConfig) -> Self {
        Self { config }
    }
}

impl SchemaMigrator for ConfigSchemaMigrator {
    fn migrate(&self, _from: SchemaVersion, _to: SchemaVersion, data: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        // Заглушка: реальная миграция через цепочку версий
        Ok(data)
    }
    fn current_version(&self) -> SchemaVersion {
        self.config.current_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_migrator_current_version() {
        let config = SchemaConfig {
            name: "test".into(),
            current_version: 3,
            versions: vec![],
        };
        let migrator = ConfigSchemaMigrator::new(config);
        assert_eq!(migrator.current_version(), 3);
    }

    #[test]
    fn test_schema_migrator_passthrough() {
        let config = SchemaConfig {
            name: "test".into(),
            current_version: 1,
            versions: vec![],
        };
        let migrator = ConfigSchemaMigrator::new(config);
        let input_data = json!({"name": "Alice", "age": 30});
        
        let result = migrator.migrate(1, 1, input_data.clone()).unwrap();
        assert_eq!(result, input_data);
    }
}