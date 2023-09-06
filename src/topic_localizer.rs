use std::{
  rc::Rc,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use actix::prelude::*;
use ahash::RandomState as AHasher;
use anyhow::{Error, Result};
use dashmap::DashMap;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::ObservableEvent;
use once_cell::sync::OnceCell;

use crate::master_client::MASTER_CLIENT;

pub struct TopicLocalizer {
  cache: DashMap<String, Arc<String>, AHasher>,
}

static TOPIC_LOCALIZER: OnceCell<TopicLocalizer> = OnceCell::new();

impl TopicLocalizer {
  pub fn new() -> Self {
    Self { cache: DashMap::with_capacity_and_hasher(10000, AHasher::default()) }
  }

  pub fn singleton() -> &'static Self {
    TOPIC_LOCALIZER.get_or_init(Self::new)
  }

  pub async fn locate(&self, topic: &String) -> Result<Arc<String>> {
    match self.cache.entry(topic.clone()) {
      dashmap::mapref::entry::Entry::Occupied(entry) => {
        let endpoint = Arc::clone(entry.get());
        return Ok(endpoint);
      }
      dashmap::mapref::entry::Entry::Vacant(entry) => {
        match MASTER_CLIENT
          .send(LocateTopicReq { topic: topic.clone(), r#ref: 0 }.into_enum())
          .await
        {
          Ok(rep) => match rep {
            ProtocolMsg::LocateTopicRep(rep) => {
              let endpoint = Arc::new(rep.endpoint);
              entry.insert(Arc::clone(&endpoint));
              Ok(endpoint)
            }
            err => {
              Err(Error::msg(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
            }
          },
          Err(err) => {
            Err(Error::msg(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
          }
        }
      }
    }
  }

  pub fn clear(&self) {
    self.cache.clear();
  }
}

struct TopicLocalizerHelperInner {
  checksum: AtomicU32,
}

impl TopicLocalizerHelperInner {
  pub async fn check(self: Rc<Self>) {
    let req = GetTopicDistChecksumReq { r#ref: 0 }.into_enum();
    log::info!("Getting TopicDistChecksum: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => match rep {
        ProtocolMsg::GetTopicDistChecksumRep(rep) => {
          log::info!("Successfully to get TopicDistChecksum: rep: {:?}", rep);
          let local_checksum = self.checksum.load(Ordering::SeqCst);
          if rep.checksum != local_checksum {
            log::info!(
              "TopicDistChecksum has changed: local: {:?}, remote: {:?}",
              local_checksum,
              rep.checksum,
            );
            self.checksum.store(rep.checksum, Ordering::SeqCst);
            TopicLocalizer::singleton().clear();
          }
        }
        _ => {
          log::warn!("Received unknown msg: {:?}", rep);
        }
      },
      Err(err) => {
        log::warn!("Failed to get TopicDistChecksum: {:?}", err);
      }
    }
  }
}

pub struct TopicLocalizerHelper {
  inner: Rc<TopicLocalizerHelperInner>,
}

impl Actor for TopicLocalizerHelper {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("TopicLocalizerHelper actor started.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.observe_connection_event(r);
  }

  fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
    log::info!("TopicLocalizerHelper actor stopping.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.unobserve_connection_event(r);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::info!("TopicLocalizerHelper actor stopped.");
  }
}

impl Handler<ObservableEvent> for TopicLocalizerHelper {
  type Result = ResponseFuture<()>;

  fn handle(&mut self, msg: ObservableEvent, _ctx: &mut Context<Self>) -> Self::Result {
    log::debug!("Received a ObservableEvent: {:?}", msg);
    let inner = self.inner.clone();
    Box::pin(async move {
      match msg {
        ObservableEvent::Connected(_) => {
          inner.check().await;
        }
        _ => (),
      }
    })
  }
}

impl TopicLocalizerHelper {
  pub fn new() -> Self {
    Self { inner: Rc::new(TopicLocalizerHelperInner { checksum: AtomicU32::new(0) }) }
  }
}
