use std::{env::current_dir, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::de::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct Config {
  pub server: ServerConfig,
  pub master_client: MasterClientConfig,
  pub route_syncer: RouteSyncerConfig,
  pub topic_localizer: TopicLocalizerConfig,
  pub handler: HandlerConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
  pub id: String,
  pub http_port: u32,
  pub https_port: u32,
  #[serde(deserialize_with = "deserialize_path")]
  pub cert_file: String,
  #[serde(deserialize_with = "deserialize_path")]
  pub key_file: String,
  pub backlog: u32,
  #[serde(deserialize_with = "deserialize_keep_alive", default)]
  pub keep_alive: Option<Duration>,
  pub max_connection_rate: usize,
  pub max_connections: usize,
  pub workers: usize,
  pub max_frame_size: usize,
}

fn deserialize_path<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
  let path: String = Deserialize::deserialize(deserializer)?;
  let path = PathBuf::from(path);
  if path.is_absolute() {
    Ok(path.display().to_string())
  } else {
    current_dir()
      .with_context(|| format!("Failed to get current dir"))
      .map_err(serde::de::Error::custom)?
      .join(path)
      .display()
      .to_string()
      .parse()
      .map_err(serde::de::Error::custom)
  }
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

#[derive(Debug, Deserialize)]
pub struct RouteSyncerConfig {
  pub sync_interval: u64,
}

#[derive(Debug, Deserialize)]
pub struct TopicLocalizerConfig {
  pub cache_size: u32,
}

#[derive(Debug, Deserialize)]
pub struct HandlerConfig {
  pub id_recip_map_size: u32,
  pub connection_cache_size: u32,
  pub connection_pool_slot_size: u8,
  pub max_continuous_disconnected_times: u32,
  pub pull_timeout: u64,
  pub request_timeout: u64,
  pub client_check_interval: u32,
  pub client_idle_timeout: u32,
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
