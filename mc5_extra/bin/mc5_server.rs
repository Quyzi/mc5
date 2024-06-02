use std::path::PathBuf;
use anyhow::Result;
use clap::Parser;
use mc5_core::{config::MangoChainsawConfig, mango::MangoChainsaw};

use mc5_extra::server::MangoChainsawServer;
use tracing::instrument;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = None)]
struct Flags {
    #[arg(short, long)]
    pub config: PathBuf,

    #[arg(short, long)]
    pub profile: String,
}

#[tokio::main]
#[instrument]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_ansi(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let flags = Flags::parse();
    let config = MangoChainsawConfig::load(flags.config, &flags.profile)?;
    let backend = MangoChainsaw::new(config)?;

    MangoChainsawServer::run(backend).await?;

    Ok(())
}