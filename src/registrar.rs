use std::{cell::RefCell, rc::Rc};

use actix::prelude::*;
use futures_intrusive::sync::LocalManualResetEvent;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::*;
use tokio::time::{sleep, Duration};

use crate::config::CONFIG;
use crate::ip_resolver::IP_RESOLVER;
use crate::master_client::MASTER_CLIENT;

struct RegistrarInner {
  connected_event: LocalManualResetEvent,
  resolved_ip: RefCell<Option<String>>,
}

impl RegistrarInner {
  pub fn new() -> Self {
    RegistrarInner {
      resolved_ip: RefCell::new(None),
      connected_event: LocalManualResetEvent::new(false),
    }
  }

  pub async fn register_repeatedly(self: Rc<Self>) {
    loop {
      self.connected_event.wait().await;

      let ip = self.resolve_ip().await;
      if ip.is_none() {
        sleep(Duration::from_millis(1000)).await;
        continue;
      }

      if self.register(ip.unwrap()).await {
        self.connected_event.reset();
      } else {
        sleep(Duration::from_millis(1000)).await;
      }
    }
  }

  async fn resolve_ip(&self) -> Option<String> {
    {
      if let Some(ip) = &*self.resolved_ip.borrow() {
        return Some(ip.clone());
      }
    }

    match IP_RESOLVER.resolve_ip().await {
      Ok(ip) => {
        *self.resolved_ip.borrow_mut() = Some(ip.clone());
        Some(ip)
      }
      Err(err) => {
        log::error!("Failed to resolve ip: {:?}", err);
        None
      }
    }
  }

  async fn register(&self, ip: String) -> bool {
    let req = RegisterFrontendReq {
      public_ip: ip,
      http_port: CONFIG.http_port,
      https_port: CONFIG.https_port,
      r#ref: 0,
    }
    .into_enum();
    log::info!("Registering: req: {:?}", req);
    match MASTER_CLIENT.send(req).await {
      Ok(rep) => {
        log::info!("Registered successfully: rep: {:?}", rep);
        true
      }
      Err(err) => {
        log::warn!("Failed to register: {:?}", err);
        false
      }
    }
  }
}

pub struct Registrar {
  inner: Rc<RegistrarInner>,
}

impl Registrar {
  pub fn new() -> Self {
    Registrar { inner: Rc::new(RegistrarInner::new()) }
  }
}

impl Actor for Registrar {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Registrar actor started.");
    Rc::clone(&self.inner).register_repeatedly().into_actor(self).spawn(ctx);
    let r = ctx.address().recipient();
    MASTER_CLIENT.observe_connection_event(r);
  }

  fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
    log::info!("Registrar actor stopping.");
    let r = ctx.address().recipient();
    MASTER_CLIENT.unobserve_connection_event(r);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::info!("Registrar actor stopped.");
  }
}

impl Handler<ObservableEvent> for Registrar {
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
