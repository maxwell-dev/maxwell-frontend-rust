use std::{num::NonZeroU32, sync::Arc};

use anyhow::{Error, Result};
use maxwell_protocol::{self, *};
use once_cell::sync::OnceCell;
use quick_cache::{sync::Cache, Weighter};

use crate::master_client::MASTER_CLIENT;

#[derive(Clone)]
struct NormalTableWeighter;

impl Weighter<String, (), Arc<String>> for NormalTableWeighter {
  fn weight(&self, _key: &String, _qey: &(), val: &Arc<String>) -> NonZeroU32 {
    NonZeroU32::new(val.len() as u32).unwrap()
  }
}

pub struct TopicLocalizer {
  cache: Cache<String, Arc<String>, NormalTableWeighter>,
}

static TOPIC_LOCALIZER: OnceCell<TopicLocalizer> = OnceCell::new();

impl TopicLocalizer {
  pub fn new() -> Self {
    Self { cache: Cache::with_weighter(100000, 3200000, NormalTableWeighter) }
  }

  pub fn singleton() -> &'static Self {
    TOPIC_LOCALIZER.get_or_init(Self::new)
  }

  pub async fn locate(&self, topic: &String) -> Result<Arc<String>> {
    self
      .cache
      .get_or_insert_async(topic, async {
        match MASTER_CLIENT
          .send(maxwell_protocol::LocateTopicReq { topic: topic.clone(), r#ref: 0 }.into_enum())
          .await
        {
          Ok(rep) => match rep {
            maxwell_protocol::ProtocolMsg::LocateTopicRep(rep) => Ok(Arc::new(rep.endpoint)),
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
}
