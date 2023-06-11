use std::rc::Rc;

use actix::prelude::*;
use futures_intrusive::sync::LocalManualResetEvent;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::*;
use tokio::time::{sleep, Duration};

use crate::master_client::MASTER_CLIENT;
use crate::route_table::ROUTE_TABLE;

struct RouteSyncerInner {
  connected_event: LocalManualResetEvent,
}

impl RouteSyncerInner {
  pub fn new() -> Self {
    RouteSyncerInner { connected_event: LocalManualResetEvent::new(false) }
  }

  pub async fn fetch_repeatedly(self: Rc<Self>) {
    loop {
      self.connected_event.wait().await;

      if self.fetch().await {
        sleep(Duration::from_secs(60)).await;
      } else {
        sleep(Duration::from_millis(1000)).await;
      }
    }
  }

  async fn fetch(&self) -> bool {
    let req = GetRoutesReq { r#ref: 0 }.into_enum();
    log::info!("Fetching: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => match rep {
        ProtocolMsg::GetRoutesRep(rep) => {
          for route in rep.route_groups.iter() {
            ROUTE_TABLE.add_route_group(
              route.path.clone(),
              route.healthy_endpoints.clone(),
              route.unhealthy_endpoints.clone(),
            );
          }
          log::info!("Fetched successfully: rep: {:?}", rep);
          true
        }
        _ => false,
      },
      Err(err) => {
        log::warn!("Failed to fetch: {:?}", err);
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
    Rc::clone(&self.inner).fetch_repeatedly().into_actor(self).spawn(ctx);
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
