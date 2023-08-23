use std::time::Duration;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::de::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct Config {
  pub server: ServerConfig,
  pub master_client: MasterClientConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
  pub http_port: u32,
  pub https_port: u32,
  pub backlog: u32,
  #[serde(deserialize_with = "deserialize_keep_alive", default)]
  pub keep_alive: Option<Duration>,
  pub max_connection_rate: usize,
  pub max_connections: usize,
  pub workers: usize,
  pub max_frame_size: usize,
}

fn deserialize_keep_alive<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where D: Deserializer<'de> {
  let keep_alive: u64 = Deserialize::deserialize(deserializer)?;
  if keep_alive == 0 {
    Ok(None)
  } else {
    Ok(Some(Duration::from_secs(keep_alive)))
  }
}

#[derive(Debug, Deserialize)]
pub struct MasterClientConfig {
  pub endpoints: Vec<String>,
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
