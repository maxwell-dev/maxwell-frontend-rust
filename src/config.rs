use anyhow::{Context, Result};
use once_cell::sync::Lazy;

#[derive(Debug, Deserialize)]
pub struct Config {
  pub http_port: u32,
  pub https_port: u32,
  pub master: MasterConfig,
}

#[derive(Debug, Deserialize)]
pub struct MasterConfig {
  pub private_ctrl_endpoints: Vec<String>,
  pub public_ctrl_endpoints: Vec<String>,
}

impl Config {
  pub(crate) fn new(path: &str) -> Result<Self> {
    Ok(
      config::Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .with_context(|| format!("Failed to read config from: {:?}", path))?
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from: {:?}", path))?,
    )
  }
}

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new("config/config.toml").unwrap());
