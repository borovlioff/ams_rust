use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    consumer::{StreamConsumer, Consumer},
    ClientConfig, Message,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::port::{EventBus, EventHandler};
use futures::StreamExt;

#[allow(dead_code)]
pub struct KafkaBus {
    producer: FutureProducer,
    consumer: Arc<Mutex<Option<StreamConsumer>>>,
}

impl KafkaBus {
    pub fn new(brokers: &str, group_id: &str) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.auto.commit", "false")
            .create()?;
        Ok(Self { producer, consumer: Arc::new(Mutex::new(Some(consumer))) })
    }
}

#[async_trait]
impl EventBus for KafkaBus {
    async fn publish(&self, topic: &str, key: &str, payload: &str) -> anyhow::Result<()> {
        let record = FutureRecord::to(topic).key(key).payload(payload);
        self.producer
            .send(record, tokio::time::Duration::from_secs(5))
            .await
            .map_err(|e| anyhow::anyhow!(e.0))?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str, handler: EventHandler) -> anyhow::Result<()> {
        let consumer = self
            .consumer
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("Already subscribed"))?;
        consumer.subscribe(&[topic])?;
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                if let Ok(m) = msg {
                    let key = m
                        .key()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .unwrap_or_default();
                    let payload = m
                        .payload()
                        .map(|p| String::from_utf8_lossy(p).to_string())
                        .unwrap_or_default();
                    handler(key, payload);
                }
            }
        });
        Ok(())
    }
}