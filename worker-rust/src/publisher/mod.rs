use anyhow::Result;
use async_trait::async_trait;

use crate::kernel::types::WorkerStateEvent;

#[async_trait]
pub trait StatePublisher: Send {
    async fn publish(&mut self, event: &WorkerStateEvent) -> Result<()>;
}

#[derive(Default)]
pub struct StdoutStatePublisher;

#[async_trait]
impl StatePublisher for StdoutStatePublisher {
    async fn publish(&mut self, event: &WorkerStateEvent) -> Result<()> {
        let line = serde_json::to_string(event)?;
        println!("{line}");
        Ok(())
    }
}
