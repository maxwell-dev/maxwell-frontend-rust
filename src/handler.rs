use std::{
  cell::RefCell,
  rc::Rc,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use actix::{prelude::*, Actor};
use actix_web_actors::ws;
use ahash::{AHashMap as HashMap, RandomState as AHasher};
use dashmap::DashMap;
use maxwell_protocol::{self, SendError, *};
use maxwell_utils::prelude::{EventHandler as MaxwellEventHandler, *};
use once_cell::sync::Lazy;

use crate::route_table::ROUTE_TABLE;
use crate::topic_localizer::TopicLocalizer;

static ID_SEED: AtomicU32 = AtomicU32::new(1);

fn next_id() -> u32 {
  ID_SEED.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug)]
struct IdAddressMap(DashMap<u32, Recipient<ProtocolMsg>, AHasher>);

impl IdAddressMap {
  pub fn new() -> Self {
    IdAddressMap(DashMap::with_capacity_and_hasher(1024, AHasher::default()))
  }

  pub fn add(&self, id: u32, address: Recipient<ProtocolMsg>) {
    self.0.insert(id, address);
  }

  pub fn remove(&self, id: u32) {
    self.0.remove(&id);
  }

  pub fn get(&self, id: u32) -> Option<Recipient<ProtocolMsg>> {
    if let Some(address) = self.0.get(&id) {
      Some(address.clone())
    } else {
      None
    }
  }
}

static ID_ADDRESS_MAP: Lazy<IdAddressMap> = Lazy::new(|| IdAddressMap::new());

struct EventHandler;

impl MaxwellEventHandler for EventHandler {
  #[inline]
  fn on_msg(&self, msg: ProtocolMsg) {
    log::info!("@@@@@@{:?}", msg);
    match msg {
      ProtocolMsg::ReqRep(rep) => {
        if let Some(address) = ID_ADDRESS_MAP.get(rep.conn0_ref) {
          let _ = address.do_send(rep.into_enum());
        }
      }
      ProtocolMsg::PullRep(rep) => {
        if let Some(address) = ID_ADDRESS_MAP.get(rep.conn0_ref) {
          let _ = address.do_send(rep.into_enum());
        }
      }
      other => {
        log::warn!("unexpected msg: {:?}", other);
      }
    }
  }
}

static CONNECTION_POOL: Lazy<ConnectionPool<ConnectionLite<EventHandler>>> =
  Lazy::new(|| ConnectionPool::new(PoolOptions { initial_slot_size: 1 }));

struct HandlerInner {
  id: u32,
  recipient: RefCell<Option<Recipient<ProtocolMsg>>>,
  endpoints: RefCell<HashMap<String, String>>,
}

impl HandlerInner {
  fn new() -> Self {
    HandlerInner {
      id: next_id(),
      recipient: RefCell::new(None),
      endpoints: RefCell::new(HashMap::default()),
    }
  }

  fn assign_address(&self, address: Recipient<ProtocolMsg>) {
    *self.recipient.borrow_mut() = Some(address.clone());
  }

  async fn handle_external_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::info!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum(),
      ProtocolMsg::ReqReq(mut req) => {
        let r#ref = req.r#ref;
        if let Some(endpoint) = self.get_endpoint(&req.path) {
          req.conn0_ref = self.id;
          log::info!("!!!!!!!!!!!!!{:?}", req);
          let rep = self.get_connection(&endpoint).try_send(req.into_enum());
          match rep {
            Ok(_) => ProtocolMsg::None,
            Err(err) => {
              log::error!("send error: {:?}", err);
              maxwell_protocol::ErrorRep { code: 1, desc: format!("Send error: {:?}", err), r#ref }
                .into_enum()
            }
          }
        } else {
          maxwell_protocol::ErrorRep {
            code: 1,
            desc: format!("Failed to find endpoint for path: {:?}", req.path),
            r#ref,
          }
          .into_enum()
        }
      }
      ProtocolMsg::PullReq(mut req) => {
        let r#ref = req.r#ref;
        req.conn0_ref = self.id;
        match TopicLocalizer::singleton().locate(&req.topic).await {
          Ok(endpoint) => {
            req.conn0_ref = self.id;
            let rep = self.get_connection(&*endpoint).try_send(req.into_enum());
            match rep {
              Ok(_) => ProtocolMsg::None,
              Err(err) => {
                log::error!("send error: {:?}", err);
                maxwell_protocol::ErrorRep {
                  code: 1,
                  desc: format!("Send error: {:?}", err),
                  r#ref,
                }
                .into_enum()
              }
            }
          }
          Err(err) => {
            log::error!("localize error: {:?}", err);
            maxwell_protocol::ErrorRep {
              code: 1,
              desc: format!("Localize error: {:?}", err),
              r#ref,
            }
            .into_enum()
          }
        }
      }
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
  }

  async fn handle_internal_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::info!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      ProtocolMsg::ReqRep(_) => protocol_msg,
      ProtocolMsg::PullRep(_) => protocol_msg,
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
  }

  fn get_endpoint(&self, path: &String) -> Option<String> {
    let endpoint = { self.endpoints.borrow().get(path).cloned() };
    if let Some(endpoint) = endpoint {
      Some(endpoint)
    } else {
      if let Some(endpoint) = ROUTE_TABLE.next_endpoint(&path) {
        self.endpoints.borrow_mut().insert(path.clone(), endpoint.clone());
        Some(endpoint)
      } else {
        None
      }
    }
  }

  fn get_connection(&self, endpoint: &String) -> Arc<Addr<ConnectionLite<EventHandler>>> {
    CONNECTION_POOL.fetch_with(&endpoint, &|endpoint| {
      ConnectionLite::start3(
        endpoint.clone(),
        ConnectionOptions { reconnect_delay: 1000, ping_interval: None },
        EventHandler,
      )
    })
  }
}

pub struct Handler {
  inner: Rc<HandlerInner>,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Handler actor started: id: {:?}", self.inner.id);
    let address = ctx.address().recipient();
    ID_ADDRESS_MAP.add(self.inner.id, address.clone());
    self.inner.assign_address(address);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Handler actor stopping: id: {:?}", self.inner.id);
    ID_ADDRESS_MAP.remove(self.inner.id);
    Running::Stop
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    log::info!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Handler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        ctx.pong(&ws_msg);
      }
      Ok(ws::Message::Pong(_)) => (),
      Ok(ws::Message::Text(_)) => (),
      Ok(ws::Message::Binary(bin)) => {
        let inner = self.inner.clone();
        async move {
          let res = maxwell_protocol::decode(&bin.into());
          match res {
            Ok(req) => Ok(inner.handle_external_msg(req).await),
            Err(err) => Err(err),
          }
        }
        .into_actor(self)
        .map(move |res, _act, ctx| match res {
          Ok(msg) => {
            if msg.is_some() {
              ctx.binary(maxwell_protocol::encode(&msg));
            }
          }
          Err(err) => log::error!("Failed to decode msg: {:?}", err),
        })
        .spawn(ctx);
      }
      Ok(ws::Message::Close(_)) => ctx.stop(),
      _ => log::error!("Received unknown msg: {:?}", ws_msg),
    }
  }
}

impl actix::Handler<ProtocolMsg> for Handler {
  type Result = Result<ProtocolMsg, SendError>;

  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    let inner = self.inner.clone();
    ctx.spawn(async move { inner.handle_internal_msg(protocol_msg).await }.into_actor(self).map(
      move |res, _act, ctx| {
        if res.is_some() {
          ctx.binary(maxwell_protocol::encode(&res));
        }
      },
    ));
    Ok(ProtocolMsg::None)
  }
}

impl Handler {
  pub fn new() -> Self {
    Self { inner: Rc::new(HandlerInner::new()) }
  }
}
