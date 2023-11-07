use std::{
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::prelude::*;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::ObservableEvent;

use crate::master_client::MASTER_CLIENT;
use crate::topic_localizer::TOPIC_LOCALIZER;

struct TopicCleanerInner {
  checksum: AtomicU32,
}

impl TopicCleanerInner {
  pub async fn check(self: Rc<Self>) {
    let req = GetTopicDistChecksumReq { r#ref: 0 }.into_enum();
    log::debug!("Getting TopicDistChecksum: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => match rep {
        ProtocolMsg::GetTopicDistChecksumRep(rep) => {
          log::debug!("Successfully to get TopicDistChecksum: rep: {:?}", rep);
          let local_checksum = self.checksum.load(Ordering::SeqCst);
          if rep.checksum != local_checksum {
            log::info!(
              "TopicDistChecksum has changed: local: {:?}, remote: {:?}, clear cache.",
              local_checksum,
              rep.checksum,
            );
            self.checksum.store(rep.checksum, Ordering::SeqCst);
            TOPIC_LOCALIZER.clear();
          } else {
            log::debug!(
              "TopicDistChecksum stays the same: local: {:?}, remote: {:?}, do nothing.",
              local_checksum,
              rep.checksum,
            );
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

pub struct TopicCleaner {
  inner: Rc<TopicCleanerInner>,
}

impl Actor for TopicCleaner {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("TopicCleaner actor started.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.observe_connection_event(r);
  }

  fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
    log::info!("TopicCleaner actor stopping.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.unobserve_connection_event(r);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::info!("TopicCleaner actor stopped.");
  }
}

impl Handler<ObservableEvent> for TopicCleaner {
  type Result = ResponseFuture<()>;

  fn handle(&mut self, msg: ObservableEvent, _ctx: &mut Context<Self>) -> Self::Result {
    log::debug!("Received an ObservableEvent: {:?}", msg);
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

impl TopicCleaner {
  pub fn new() -> Self {
    Self { inner: Rc::new(TopicCleanerInner { checksum: AtomicU32::new(0) }) }
  }
}
