use std::{
  cell::RefCell,
  rc::Rc,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use actix::{prelude::*, Actor, Addr};
use actix_web_actors::ws;
use ahash::RandomState as AHasher;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use maxwell_client::prelude::ConnectionMgr;
use maxwell_protocol::{self, SendError, *};
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;

use crate::route_table::ROUTE_TABLE;

static ID_SEED: AtomicU32 = AtomicU32::new(1);

fn next_id() -> u32 {
  ID_SEED.fetch_add(1, Ordering::Relaxed)
}

static ID_ADDRESS_MAP: OnceCell<IdAddressMap> = OnceCell::new();

#[derive(Debug)]
struct IdAddressMap(DashMap<u32, Recipient<ProtocolMsg>, AHasher>);

impl IdAddressMap {
  pub fn singleton() -> &'static Self {
    ID_ADDRESS_MAP
      .get_or_init(|| IdAddressMap(DashMap::with_capacity_and_hasher(1024, AHasher::default())))
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

pub static CONNECTION_MGR: Lazy<ConnectionMgr> = Lazy::new(|| ConnectionMgr::default());

#[derive(Debug, Clone)]
struct HandlerInner {
  id: u32,
  recipient: RefCell<Option<Recipient<ProtocolMsg>>>,
  id_address_map: &'static IdAddressMap,
}

impl HandlerInner {
  fn new() -> Self {
    HandlerInner {
      id: next_id(),
      recipient: RefCell::new(None),
      id_address_map: IdAddressMap::singleton(),
    }
  }

  fn assign_address(&self, address: Recipient<ProtocolMsg>) {
    *self.recipient.borrow_mut() = Some(address.clone());
    self.id_address_map.add(self.id, address);
  }

  async fn handle_external_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::info!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum(),
      ProtocolMsg::ReqReq(req) => {
        let r#ref = req.r#ref;
        if let Some(endpoint) = ROUTE_TABLE.next_endpoint(&req.path) {
          log::info!("endpoint: {:?}", endpoint);
          let connection = CONNECTION_MGR.fetch_connection(&endpoint);
          let rep = connection.send(req.into_enum()).await;
          log::info!("received rep: {:?}", rep);
          match rep {
            Ok(rep) => match rep {
              Ok(ProtocolMsg::ReqRep(mut rep)) => {
                rep.r#ref = r#ref;
                rep.into_enum()
              }
              Err(err) => maxwell_protocol::ErrorRep {
                code: 1,
                desc: format!("Failed to send msg: {:?}", err),
                r#ref: r#ref,
              }
              .into_enum(),
              _ => maxwell_protocol::ErrorRep {
                code: 1,
                desc: format!("Received unexpected msg: {:?}", rep),
                r#ref: r#ref,
              }
              .into_enum(),
            },
            Err(err) => maxwell_protocol::ErrorRep {
              code: 1,
              desc: format!("Failed to send msg: {:?}", err),
              r#ref: r#ref,
            }
            .into_enum(),
          }
        } else {
          maxwell_protocol::ErrorRep {
            code: 1,
            desc: format!("Failed to find endpoint for path: {:?}", req.path),
            r#ref: r#ref,
          }
          .into_enum()
        }
      }
      // ProtocolMsg::DoRep(rep) => {
      //   if let Some(handler) = self.id_address_map.get(rep.traces[0].handler_id) {
      //     handler.do_send(rep.into_enum());
      //   }
      //   ProtocolMsg::None
      // }
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
  }

  async fn handle_internal_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::info!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      // ProtocolMsg::DoReq(_) => protocol_msg,
      // ProtocolMsg::DoRep(_) => protocol_msg,
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
  }
}

#[derive(Debug)]
pub struct Handler {
  inner: Rc<HandlerInner>,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Handler actor started: id: {:?}", self.inner.id);
    self.inner.assign_address(ctx.address().recipient());
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Handler actor stopping: id: {:?}", self.inner.id);
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
