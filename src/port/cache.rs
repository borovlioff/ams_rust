use async_trait::async_trait;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>>;
    async fn set(&self, key: &str, value: &str, ttl_secs: Option<u64>) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;
    async fn ping(&self) -> anyhow::Result<()>;
}