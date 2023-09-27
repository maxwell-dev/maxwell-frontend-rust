use std::{
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::prelude::*;
use ahash::AHashSet;
use futures_intrusive::sync::LocalManualResetEvent;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::*;
use tokio::time::{sleep, Duration};

use crate::route_table::ROUTE_TABLE;
use crate::{config::CONFIG, master_client::MASTER_CLIENT};

struct RouteSyncerInner {
  connected_event: LocalManualResetEvent,
  checksum: AtomicU32,
}

impl RouteSyncerInner {
  pub fn new() -> Self {
    RouteSyncerInner {
      connected_event: LocalManualResetEvent::new(false),
      checksum: AtomicU32::new(0),
    }
  }

  pub async fn get_repeatedly(self: Rc<Self>) {
    loop {
      self.connected_event.wait().await;

      if self.check().await {
        loop {
          if self.get().await {
            break;
          } else {
            sleep(Duration::from_secs(1)).await;
            continue;
          }
        }
      }
      sleep(Duration::from_secs(CONFIG.route_syncer.sync_interval)).await;
    }
  }

  async fn check(&self) -> bool {
    let req = GetRouteDistChecksumReq { r#ref: 0 }.into_enum();
    log::info!("Getting RouteDistChecksum: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => match rep {
        ProtocolMsg::GetRouteDistChecksumRep(rep) => {
          log::info!("Successfully to get RouteDistChecksum: rep: {:?}", rep);
          let local_checksum = self.checksum.load(Ordering::SeqCst);
          if rep.checksum != local_checksum {
            log::info!(
              "RouteDistChecksum has changed: local: {:?}, remote: {:?}",
              local_checksum,
              rep.checksum,
            );
            self.checksum.store(rep.checksum, Ordering::SeqCst);
            return true;
          } else {
            return false;
          }
        }
        _ => {
          log::warn!("Received unknown msg: {:?}", rep);
          false
        }
      },
      Err(err) => {
        log::warn!("Failed to get RouteDistChecksum: {:?}", err);
        false
      }
    }
  }

  async fn get(&self) -> bool {
    let req = GetRoutesReq { r#ref: 0 }.into_enum();
    log::info!("Getting routes: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => match rep {
        ProtocolMsg::GetRoutesRep(rep) => {
          log::info!("Successfully to get routes: rep: {:?}", rep);
          let mut paths = AHashSet::with_capacity(rep.route_groups.len());
          for route in rep.route_groups.iter() {
            ROUTE_TABLE.set_route_group(
              route.path.clone(),
              route.healthy_endpoints.clone(),
              route.unhealthy_endpoints.clone(),
            );
            paths.insert(route.path.clone());
          }
          ROUTE_TABLE.remove_if_not_in(paths);
          true
        }
        _ => {
          log::warn!("Received unknown msg: {:?}", rep);
          false
        }
      },
      Err(err) => {
        log::warn!("Failed to get routes: {:?}", err);
        false
      }
    }
  }
}

pub struct RouteSyncer {
  inner: Rc<RouteSyncerInner>,
}

impl RouteSyncer {
  pub fn new() -> Self {
    RouteSyncer { inner: Rc::new(RouteSyncerInner::new()) }
  }
}

impl Actor for RouteSyncer {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("RouteSyncer actor started.");
    Rc::clone(&self.inner).get_repeatedly().into_actor(self).spawn(ctx);
    let r = ctx.address().recipient();
    MASTER_CLIENT.observe_connection_event(r);
  }

  fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
    log::info!("RouteSyncer actor stopping.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.unobserve_connection_event(r);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::info!("RouteSyncer actor stopped.");
  }
}

impl Handler<ObservableEvent> for RouteSyncer {
  type Result = ();

  fn handle(&mut self, msg: ObservableEvent, _ctx: &mut Self::Context) -> Self::Result {
    log::debug!("Received a ObservableEvent: {:?}", msg);
    match msg {
      ObservableEvent::Connected(_) => self.inner.connected_event.set(),
      ObservableEvent::Disconnected(_) => self.inner.connected_event.reset(),
      _ => {}
    }
  }
}
