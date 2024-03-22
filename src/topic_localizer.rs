use std::sync::Arc;

use anyhow::{anyhow, Result};
use maxwell_protocol::{self, *};
use once_cell::sync::Lazy;
use quick_cache::{sync::Cache, Weighter};

use crate::{config::CONFIG, master_client::MASTER_CLIENT};

#[derive(Clone)]
pub struct EndpointWeighter;

impl Weighter<String, Arc<String>> for EndpointWeighter {
  fn weight(&self, _key: &String, val: &Arc<String>) -> u32 {
    val.len() as u32
  }
}

pub struct TopicLocalizer {
  cache: Cache<String, Arc<String>, EndpointWeighter>,
}

pub static TOPIC_LOCALIZER: Lazy<TopicLocalizer> = Lazy::new(|| TopicLocalizer::new());

impl TopicLocalizer {
  #[inline]
  pub fn new() -> Self {
    Self {
      cache: Cache::with_weighter(
        CONFIG.topic_localizer.cache_size as usize,
        CONFIG.topic_localizer.cache_size as u64 * 30,
        EndpointWeighter,
      ),
    }
  }

  pub async fn locate(&self, topic: &String) -> Result<Arc<String>> {
    self
      .cache
      .get_or_insert_async(topic, async {
        match MASTER_CLIENT
          .send(LocateTopicReq { topic: topic.clone(), r#ref: 0 }.into_enum())
          .await
        {
          Ok(rep) => match rep {
            ProtocolMsg::LocateTopicRep(rep) => Ok(Arc::new(rep.endpoint)),
            err => {
              Err(anyhow!(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
            }
          },
          Err(err) => {
            Err(anyhow!(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
          }
        }
      })
      .await
  }

  #[inline]
  pub fn clear(&self) {
    self.cache.clear();
  }
}
