use std::sync::Arc;

use anyhow::{Error, Result};
use maxwell_protocol::{self, *};
use once_cell::sync::OnceCell;
use quick_cache::{sync::Cache, Weighter};

use crate::master_client::MASTER_CLIENT;

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

static TOPIC_LOCALIZER: OnceCell<TopicLocalizer> = OnceCell::new();

impl TopicLocalizer {
  #[inline]
  pub fn new() -> Self {
    Self { cache: Cache::with_weighter(100000, 100000 * 30, EndpointWeighter) }
  }

  #[inline]
  pub fn singleton() -> &'static Self {
    TOPIC_LOCALIZER.get_or_init(Self::new)
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
              Err(Error::msg(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
            }
          },
          Err(err) => {
            Err(Error::msg(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
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
