mod kernel;
mod providers;
mod publisher;
mod runtime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    runtime::run().await
}
