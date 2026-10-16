use async_trait::async_trait;
use redis::AsyncCommands;
use crate::port::Cache;

#[derive(Clone, Copy)]
pub enum RedisMode {
    Cache { ttl_secs: u64 },
    Storage,
}

pub struct RedisAdapter {
    client: redis::Client,
    mode: RedisMode,
}

impl RedisAdapter {
    pub fn new(url: &str, mode: RedisMode) -> anyhow::Result<Self> {
        Ok(Self { client: redis::Client::open(url)?, mode })
    }
    
    async fn conn(&self) -> anyhow::Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }
}

#[async_trait]
impl Cache for RedisAdapter {
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut c = self.conn().await?;
        let result: Option<String> = c.get(key).await?;
        Ok(result)
    }
    
    async fn set(&self, key: &str, v: &str, ttl: Option<u64>) -> anyhow::Result<()> {
        let mut c = self.conn().await?;
        match (self.mode, ttl) {
            (RedisMode::Cache { ttl_secs }, _) | (_, Some(ttl_secs)) => {
                let _: () = c.set_ex(key, v, ttl_secs).await?;
            }
            (RedisMode::Storage, None) => {
                let _: () = c.set(key, v).await?;
            }
        }
        Ok(())
    }
    
    async fn del(&self, key: &str) -> anyhow::Result<()> {
        let mut c = self.conn().await?;
        let _: () = c.del(key).await?;
        Ok(())
    }
    
    async fn ping(&self) -> anyhow::Result<()> {
        let mut c = self.conn().await?;
        let _: String = redis::cmd("PING").query_async(&mut c).await?;
        Ok(())
    }
}