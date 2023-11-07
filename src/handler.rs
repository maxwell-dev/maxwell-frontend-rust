use std::{
  cell::RefCell,
  future::Future,
  num::NonZeroUsize,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use actix::{prelude::*, Actor};
use actix_http::{header, ws::Item};
use actix_web::HttpRequest;
use actix_web_actors::ws;
use ahash::RandomState as AHasher;
use anyhow::{Error, Result};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use lru::LruCache;
use maxwell_protocol::{self, HandleError, *};
use maxwell_utils::prelude::{Arc, EventHandler as MaxwellEventHandler, *};
use once_cell::sync::Lazy;

use crate::topic_localizer::TOPIC_LOCALIZER;
use crate::{config::CONFIG, route_table::ROUTE_TABLE};

static ID_SEED: AtomicU32 = AtomicU32::new(1);

#[inline]
fn next_id() -> u32 {
  ID_SEED.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug)]
struct IdRecipMap(DashMap<u32, Arc<Recipient<ProtocolMsg>>, AHasher>);

impl IdRecipMap {
  #[inline]
  pub fn new() -> Self {
    IdRecipMap(DashMap::with_capacity_and_hasher(
      CONFIG.handler.id_recip_map_size as usize,
      AHasher::default(),
    ))
  }

  #[inline]
  pub fn add(&self, id: u32, recip: Arc<Recipient<ProtocolMsg>>) {
    self.0.insert(id, recip);
  }

  #[inline]
  pub fn remove(&self, id: u32) {
    self.0.remove(&id);
  }

  #[inline]
  pub fn get(&self, id: u32) -> Option<Arc<Recipient<ProtocolMsg>>> {
    if let Some(recip) = self.0.get(&id) {
      Some(recip.clone())
    } else {
      None
    }
  }
}

static ID_RECIP_MAP: Lazy<IdRecipMap> = Lazy::new(|| IdRecipMap::new());

pub struct StickyConnectionMgr<C: Connection> {
  connections: RefCell<LruCache<String, Arc<Addr<C>>, AHasher>>,
}

impl<C: Connection> StickyConnectionMgr<C> {
  #[inline]
  pub fn new() -> Self {
    StickyConnectionMgr {
      connections: RefCell::new(LruCache::with_hasher(
        NonZeroUsize::new(CONFIG.handler.connection_cache_size as usize).unwrap(),
        AHasher::new(),
      )),
    }
  }

  #[inline]
  pub fn get_or_init(
    &self, key: &String, init_connection: impl FnOnce() -> Result<Arc<Addr<C>>>,
  ) -> Result<Arc<Addr<C>>> {
    match self.connections.try_borrow_mut() {
      Ok(mut connections) => {
        if let Some(connection) = connections.get(key) {
          if connection.connected() {
            return Ok(connection.clone());
          }
        }
        return init_connection().and_then(|connection| {
          connections.put(key.to_owned(), Arc::clone(&connection));
          Ok(connection)
        });
      }
      Err(err) => {
        log::warn!("Failed to get connection: err: {:?}", err);
        init_connection()
      }
    }
  }

  #[inline]
  pub async fn get_or_init_async(
    &self, key: &String, init_connection: impl Future<Output = Result<Arc<Addr<C>>>>,
  ) -> Result<Arc<Addr<C>>> {
    match self.connections.try_borrow_mut() {
      Ok(mut connections) => {
        if let Some(connection) = connections.get(key) {
          if connection.connected() {
            return Ok(connection.clone());
          }
        }
        return init_connection.await.and_then(|connection| {
          connections.put(key.to_owned(), Arc::clone(&connection));
          Ok(connection)
        });
      }
      Err(err) => {
        log::warn!("Failed to get connection: err: {:?}", err);
        init_connection.await
      }
    }
  }

  #[inline]
  pub fn remove(&self, key: &String) {
    match self.connections.try_borrow_mut() {
      Ok(mut connections) => {
        connections.pop(key);
      }
      Err(err) => {
        log::warn!("Failed to remove connection: err: {:?}", err);
      }
    }
  }
}

struct EventHandler {
  endpoint: String,
  continuous_disconnected_times: AtomicU32,
}

impl MaxwellEventHandler for EventHandler {
  #[inline]
  fn on_msg(&self, msg: ProtocolMsg) {
    match msg {
      ProtocolMsg::ReqRep(rep) => {
        if let Some(recip) = ID_RECIP_MAP.get(rep.conn0_ref) {
          let _ = recip.do_send(rep.into_enum());
        }
      }
      ProtocolMsg::PullRep(rep) => {
        if let Some(recip) = ID_RECIP_MAP.get(rep.conn0_ref) {
          let _ = recip.do_send(rep.into_enum());
        }
      }
      ProtocolMsg::Error2Rep(rep) => {
        log::warn!("Received error msg: {:?}", rep);
        if let Some(recip) = ID_RECIP_MAP.get(rep.conn0_ref) {
          let _ = recip.do_send(rep.into_enum());
        }
      }
      other => {
        log::warn!("Received unknown msg: {:?}", other);
      }
    }
  }

  #[inline]
  fn on_connected(&self, _addr: Addr<CallbackStyleConnection<Self>>) {
    log::debug!("Connected: endpoint: {:?}", self.endpoint);
    self.continuous_disconnected_times.store(0, Ordering::Relaxed);
  }

  #[inline]
  fn on_disconnected(&self, addr: Addr<CallbackStyleConnection<Self>>) {
    log::debug!("Disconnected: endpoint: {:?}", self.endpoint);
    let times = self.continuous_disconnected_times.fetch_add(1, Ordering::Relaxed) + 1;
    if times >= CONFIG.handler.max_continuous_disconnected_times {
      log::warn!(
        "Disconnected too many times: {:?}, stop and remove this connection: endpoint: {:?}",
        times,
        self.endpoint
      );
      addr.do_send(StopMsg);
      CONNECTION_POOL.remove_by_endpoint(&self.endpoint);
    }
  }
}

impl EventHandler {
  #[inline]
  pub fn new(endpoint: String) -> Self {
    Self { endpoint, continuous_disconnected_times: AtomicU32::new(0) }
  }
}

static CONNECTION_POOL: Lazy<Arc<ConnectionPool<CallbackStyleConnection<EventHandler>>>> =
  Lazy::new(|| {
    Arc::new(ConnectionPool::new(ConnectionPoolOptions {
      slot_size: CONFIG.handler.connection_pool_slot_size,
    }))
  });

struct HandlerInner {
  id: u32,
  user_agent: String,
  peer_addr: String,
  sticky_connection_mgr: StickyConnectionMgr<CallbackStyleConnection<EventHandler>>,
  sticky_connection_mgr2: StickyConnectionMgr<CallbackStyleConnection<EventHandler>>,
}

impl HandlerInner {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    HandlerInner {
      id: next_id(),
      user_agent: req.headers().get(header::USER_AGENT).map_or_else(
        || "unknown".to_owned(),
        |value| value.to_str().unwrap_or("unknown").to_owned(),
      ),
      peer_addr: req.peer_addr().map_or_else(|| "0.0.0.0".to_owned(), |value| value.to_string()),
      sticky_connection_mgr: StickyConnectionMgr::new(),
      sticky_connection_mgr2: StickyConnectionMgr::new(),
    }
  }

  pub async fn handle_external_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum(),
      ProtocolMsg::ReqReq(mut req) => {
        let r#ref = req.r#ref;

        match self.get_connection_by_path(&req.path) {
          Ok(connection) => {
            req.conn0_ref = self.id;
            if let Some(header) = &mut req.header {
              header.agent = self.user_agent.clone();
              header.endpoint = self.peer_addr.clone();
            }

            let rep = connection
              .send(req.into_enum())
              .timeout_ext(Duration::from_secs(CONFIG.handler.request_timeout))
              .await;
            match rep {
              Ok(_) => ProtocolMsg::None,
              Err(err) => {
                log::error!("Failed to send: error: {:?}", err);
                let rep = maxwell_protocol::ErrorRep {
                  code: ErrorCode::FailedToRequestService as i32,
                  desc: format!("Failed to send: error: {:?}", err),
                  r#ref,
                }
                .into_enum();
                if let HandleError::Any { msg, .. } = err {
                  let req = &ReqReq::from(msg);
                  log::warn!("Removing sticky connection: path: {:?}", req.path);
                  self.sticky_connection_mgr.remove(&req.path)
                }
                rep
              }
            }
          }
          Err(err) => {
            log::error!("Failed to get connetion: err: {:?}", err);
            maxwell_protocol::ErrorRep {
              code: ErrorCode::FrontendError as i32,
              desc: format!("Failed to get connetion: err: {:?}", err),
              r#ref,
            }
            .into_enum()
          }
        }
      }
      ProtocolMsg::PullReq(mut req) => {
        let r#ref = req.r#ref;

        match self.get_connection_by_topic(&req.topic).await {
          Ok(connection) => {
            req.conn0_ref = self.id;

            let rep = connection
              .send(req.into_enum())
              .timeout_ext(Duration::from_secs(CONFIG.handler.pull_timeout))
              .await;
            match rep {
              Ok(_) => ProtocolMsg::None,
              Err(err) => {
                log::error!("Failed to send: error: {:?}", err);
                let rep = maxwell_protocol::ErrorRep {
                  code: ErrorCode::FailedToRequestBackend as i32,
                  desc: format!("Failed to send: error: {:?}", err),
                  r#ref,
                }
                .into_enum();
                if let HandleError::Any { msg, .. } = err {
                  let req = PullReq::from(msg);
                  log::warn!("Removing sticky connection: topic: {:?}", req.topic);
                  self.sticky_connection_mgr2.remove(&req.topic)
                }
                rep
              }
            }
          }
          Err(err) => {
            log::error!("Failed to localize: err: {:?}", err);
            maxwell_protocol::ErrorRep {
              code: ErrorCode::FrontendError as i32,
              desc: format!("Failed to localize: err: {:?}", err),
              r#ref,
            }
            .into_enum()
          }
        }
      }
      _ => {
        log::error!("Received unknown msg: {:?}", protocol_msg);
        maxwell_protocol::ErrorRep {
          code: ErrorCode::UnknownMsg as i32,
          desc: format!("Received unknown msg: {:?}", protocol_msg),
          r#ref: 0,
        }
        .into_enum()
      }
    }
  }

  pub async fn handle_internal_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      ProtocolMsg::ReqRep(_) => protocol_msg,
      ProtocolMsg::PullRep(_) => protocol_msg,
      ProtocolMsg::Error2Rep(err) => {
        if err.code == ErrorCode::UnknownPath as i32 {
          if let Some((_, path)) = err.desc.split_once(':') {
            let path = path.trim().to_owned();
            log::warn!("Removing sticky connection: path: {:?}", path);
            self.sticky_connection_mgr.remove(&path);
          }
        } else if err.code == ErrorCode::UnknownTopic as i32 {
          if let Some((_, topic)) = err.desc.split_once(':') {
            let topic = topic.trim().to_owned();
            log::warn!("Removing sticky connection: topic: {:?}", topic);
            self.sticky_connection_mgr2.remove(&topic);
          }
        }
        protocol_msg
      }
      _ => {
        log::error!("Received unknown msg: {:?}", protocol_msg);
        maxwell_protocol::ErrorRep {
          code: ErrorCode::UnknownMsg as i32,
          desc: format!("Received unknown msg: {:?}", protocol_msg),
          r#ref: get_ref(&protocol_msg),
        }
        .into_enum()
      }
    }
  }

  #[inline]
  fn get_connection_by_path(
    &self, path: &String,
  ) -> Result<Arc<Addr<CallbackStyleConnection<EventHandler>>>> {
    self.sticky_connection_mgr.get_or_init(path, || {
      if let Some(endpoint) = ROUTE_TABLE.next_endpoint(path) {
        Ok(CONNECTION_POOL.get_or_init(endpoint.as_str(), &|endpoint| {
          CallbackStyleConnection::start3(
            endpoint.to_owned(),
            ConnectionOptions::default(),
            EventHandler::new(endpoint.clone()),
          )
        }))
      } else {
        Err(Error::msg(format!("Failed to find endpoint: path: {:?}", path)))
      }
    })
  }

  #[inline]
  async fn get_connection_by_topic(
    &self, topic: &String,
  ) -> Result<Arc<Addr<CallbackStyleConnection<EventHandler>>>> {
    self
      .sticky_connection_mgr2
      .get_or_init_async(topic, async {
        match TOPIC_LOCALIZER.locate(topic).await {
          Ok(endpoint) => Ok(CONNECTION_POOL.get_or_init_with_index_seed(
            endpoint.as_str(),
            self.id,
            &|endpoint| {
              CallbackStyleConnection::start3(
                endpoint.to_owned(),
                ConnectionOptions::default(),
                EventHandler::new(endpoint.clone()),
              )
            },
          )),
          Err(err) => {
            Err(Error::msg(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
          }
        }
      })
      .await
  }
}

pub struct Handler {
  inner: Rc<HandlerInner>,
  buf: BytesMut,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  #[inline]
  fn started(&mut self, ctx: &mut Self::Context) {
    log::debug!("Handler actor started: id: {:?}", self.inner.id);
    ID_RECIP_MAP.add(self.inner.id, Arc::new(ctx.address().recipient()));
  }

  #[inline]
  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::debug!("Handler actor stopping: id: {:?}", self.inner.id);
    ID_RECIP_MAP.remove(self.inner.id);
    Running::Stop
  }

  #[inline]
  fn stopped(&mut self, _: &mut Self::Context) {
    log::debug!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Handler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        ctx.pong(&ws_msg);
      }
      Ok(ws::Message::Pong(_)) => (),
      Ok(ws::Message::Binary(bin)) => self.spawn_to_handle_external_msg(bin, ctx),
      Ok(ws::Message::Continuation(cont)) => match cont {
        Item::FirstText(data) => {
          log::error!("Received invalid continuation item: {:?}", data)
        }
        Item::FirstBinary(data) => {
          self.buf.extend_from_slice(&data);
        }
        Item::Continue(data) => {
          self.buf.extend_from_slice(&data);
        }
        Item::Last(data) => {
          self.buf.extend_from_slice(&data);
          let buf = std::mem::replace(&mut self.buf, BytesMut::new());
          self.spawn_to_handle_external_msg(buf.freeze(), ctx);
        }
      },
      Ok(ws::Message::Close(_)) => ctx.stop(),
      _ => {
        log::error!("Received unknown msg: {:?}", ws_msg);
        ctx.stop();
      }
    }
  }
}

impl actix::Handler<ProtocolMsg> for Handler {
  type Result = Result<ProtocolMsg, HandleError<ProtocolMsg>>;

  #[inline]
  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    self.spawn_to_handle_internal_msg(protocol_msg, ctx);
    Ok(ProtocolMsg::None)
  }
}

impl Handler {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    Self { inner: Rc::new(HandlerInner::new(req)), buf: BytesMut::new() }
  }

  #[inline(always)]
  fn spawn_to_handle_internal_msg(
    &mut self, protocol_msg: ProtocolMsg, ctx: &mut <Self as actix::Actor>::Context,
  ) {
    let inner = self.inner.clone();
    async move { inner.handle_internal_msg(protocol_msg).await }
      .into_actor(self)
      .map(move |res, _act, ctx| {
        if res.is_some() {
          ctx.binary(maxwell_protocol::encode(&res));
        }
      })
      .spawn(ctx);
  }

  #[inline(always)]
  fn spawn_to_handle_external_msg(&self, bin: Bytes, ctx: &mut ws::WebsocketContext<Self>) {
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
}
