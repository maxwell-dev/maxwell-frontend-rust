use std::{
  future::Future,
  rc::Rc,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
  time::Duration,
};

use actix::{prelude::*, Actor};
use actix_http::{header, ws::Item};
use actix_web::HttpRequest;
use actix_web_actors::ws;
use ahash::RandomState as AHasher;
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use maxwell_protocol::{self, HandleError, *};
use maxwell_utils::prelude::{EventHandler as MaxwellEventHandler, *};
use moka::future::Cache as AsyncCache;
use moka::sync::Cache;
use once_cell::sync::Lazy;
use quanta::Instant;

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
  connections: Cache<String, Arc<Addr<C>>, AHasher>,
}

impl<C: Connection> StickyConnectionMgr<C> {
  #[inline]
  pub fn new() -> Self {
    StickyConnectionMgr {
      connections: Cache::builder()
        .initial_capacity(8)
        .max_capacity(CONFIG.handler.connection_cache_size as u64)
        .build_with_hasher(AHasher::new()),
    }
  }

  #[inline]
  pub fn get_or_init(
    &self, key: &String, init_connection: impl FnOnce() -> Result<Arc<Addr<C>>>,
  ) -> Result<Arc<Addr<C>>> {
    self
      .connections
      .try_get_with_by_ref(key, init_connection)
      .or_else(|err| Err(anyhow!(format!("{}", err))))
  }

  #[inline]
  pub fn remove(&self, key: &String) {
    self.connections.invalidate(key);
  }
}

pub struct AsyncStickyConnectionMgr<C: Connection> {
  connections: AsyncCache<String, Arc<Addr<C>>, AHasher>,
}

impl<C: Connection> AsyncStickyConnectionMgr<C> {
  #[inline]
  pub fn new() -> Self {
    AsyncStickyConnectionMgr {
      connections: AsyncCache::builder()
        .initial_capacity(8)
        .max_capacity(CONFIG.handler.connection_cache_size as u64)
        .build_with_hasher(AHasher::new()),
    }
  }

  #[inline]
  pub async fn get_or_init(
    &self, key: &String, init_connection: impl Future<Output = Result<Arc<Addr<C>>>>,
  ) -> Result<Arc<Addr<C>>> {
    self
      .connections
      .try_get_with_by_ref(key, async { init_connection.await })
      .await
      .or_else(|err| Err(anyhow!(format!("{}", err))))
  }

  #[inline]
  pub async fn remove(&self, key: &String) {
    self.connections.invalidate(key).await;
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

struct WsHandlerInner {
  id: u32,
  user_agent: String,
  peer_addr: String,
  service_connection_mgr: StickyConnectionMgr<CallbackStyleConnection<EventHandler>>,
  backend_connection_mgr: AsyncStickyConnectionMgr<CallbackStyleConnection<EventHandler>>,
}

impl WsHandlerInner {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    WsHandlerInner {
      id: next_id(),
      user_agent: req.headers().get(header::USER_AGENT).map_or_else(
        || "unknown".to_owned(),
        |value| value.to_str().unwrap_or("unknown").to_owned(),
      ),
      peer_addr: req.peer_addr().map_or_else(|| "0.0.0.0".to_owned(), |value| value.to_string()),
      service_connection_mgr: StickyConnectionMgr::new(),
      backend_connection_mgr: AsyncStickyConnectionMgr::new(),
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
                  self.service_connection_mgr.remove(&req.path)
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
                  self.backend_connection_mgr.remove(&req.topic).await
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
            self.service_connection_mgr.remove(&path);
          }
        } else if err.code == ErrorCode::UnknownTopic as i32 {
          if let Some((_, topic)) = err.desc.split_once(':') {
            let topic = topic.trim().to_owned();
            log::warn!("Removing sticky connection: topic: {:?}", topic);
            self.backend_connection_mgr.remove(&topic).await;
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
    loop {
      let result = self.service_connection_mgr.get_or_init(path, || {
        if let Some(endpoint) = ROUTE_TABLE.next_ws_endpoint(path) {
          Ok(CONNECTION_POOL.get_or_init(endpoint.as_str(), &|endpoint| {
            CallbackStyleConnection::start3(
              endpoint.to_owned(),
              ConnectionOptions::default(),
              EventHandler::new(endpoint.clone()),
            )
          }))
        } else {
          Err(anyhow!(format!("Failed to find endpoint: path: {:?}", path)))
        }
      });
      match result {
        Ok(connection) => {
          if connection.connected() {
            return Ok(connection);
          } else {
            log::warn!("The returned connection is broken, retry again: path: {:?}", path);
            self.service_connection_mgr.remove(path);
          }
        }
        Err(err) => return Err(err),
      }
    }
  }

  #[inline]
  async fn get_connection_by_topic(
    &self, topic: &String,
  ) -> Result<Arc<Addr<CallbackStyleConnection<EventHandler>>>> {
    loop {
      let result = self
        .backend_connection_mgr
        .get_or_init(topic, async {
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
              Err(anyhow!(format!("Failed to locate topic: topic: {:?}, err: {:?}", topic, err)))
            }
          }
        })
        .await;
      match result {
        Ok(connection) => {
          if connection.connected() {
            return Ok(connection);
          } else {
            log::warn!("The returned connection is broken, retry again: topic: {:?}", topic);
            self.backend_connection_mgr.remove(topic).await;
          }
        }
        Err(err) => return Err(err),
      }
    }
  }
}

pub struct WsHandler {
  inner: Rc<WsHandlerInner>,
  receive_buf: BytesMut,
  received_last: bool,
  check_timer: SpawnHandle,
  active_at: Instant,
}

impl Actor for WsHandler {
  type Context = ws::WebsocketContext<Self>;

  #[inline]
  fn started(&mut self, ctx: &mut Self::Context) {
    log::debug!("Handler actor started: id: {:?}", self.inner.id);
    ID_RECIP_MAP.add(self.inner.id, Arc::new(ctx.address().recipient()));
    ctx.run_interval(
      Duration::from_secs(CONFIG.handler.client_check_interval as u64),
      |act, ctx| {
        log::debug!("Checking alive for handler actor: id: {:?}", act.inner.id);
        if act.active_at.elapsed() > Duration::from_secs(CONFIG.handler.client_idle_timeout as u64)
        {
          log::info!("Handler actor met idle timeout: id: {:?}", act.inner.id);
          ctx.stop();
        }
      },
    );
  }

  #[inline]
  fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
    log::debug!("Handler actor stopping: id: {:?}", self.inner.id);
    ID_RECIP_MAP.remove(self.inner.id);
    ctx.cancel_future(self.check_timer);
    self.receive_buf = BytesMut::new();
    Running::Stop
  }

  #[inline]
  fn stopped(&mut self, _: &mut Self::Context) {
    log::debug!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsHandler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    self.active_at = Instant::recent();
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        ctx.pong(&ws_msg);
      }
      Ok(ws::Message::Pong(_)) => (),
      Ok(ws::Message::Binary(bin)) => self.spawn_to_handle_external_msg(bin, ctx),
      Ok(ws::Message::Continuation(cont)) => match cont {
        Item::FirstText(data) => {
          log::error!("Received invalid continuation item: {:?}", data);
          ctx.stop();
        }
        Item::FirstBinary(data) => {
          ctx.cancel_future(self.check_timer);
          self.received_last = false;
          self.receive_buf = BytesMut::with_capacity(data.len() * 3);
          self.receive_buf.extend_from_slice(&data);
          self.check_timer = ctx.run_later(Duration::from_secs(5), |act, _ctx| {
            if !act.received_last {
              log::warn!("Timeout to receive the last item, reset the buffer.");
              act.receive_buf = BytesMut::new();
            }
          });
        }
        Item::Continue(data) => {
          let wanted_len = self.receive_buf.len() + data.len();
          if wanted_len > CONFIG.server.max_frame_size {
            log::error!("Received too large msg: len: {}", wanted_len);
            ctx.stop();
          } else {
            self.receive_buf.extend_from_slice(&data);
          }
        }
        Item::Last(data) => {
          ctx.cancel_future(self.check_timer);
          self.received_last = true;
          let wanted_len = self.receive_buf.len() + data.len();
          if wanted_len > CONFIG.server.max_frame_size {
            log::error!("Received too large msg: len: {}", wanted_len);
            ctx.stop();
          } else {
            self.receive_buf.extend_from_slice(&data);
          }
          let receive_buf = std::mem::replace(&mut self.receive_buf, BytesMut::new());
          self.spawn_to_handle_external_msg(receive_buf.freeze(), ctx);
        }
      },
      Ok(ws::Message::Close(reason)) => {
        log::info!("Received close msg: reason: {:?}", reason);
        ctx.stop();
      }
      _ => {
        log::error!("Received unknown msg: {:?}", ws_msg);
        ctx.stop();
      }
    }
  }
}

impl actix::Handler<ProtocolMsg> for WsHandler {
  type Result = Result<ProtocolMsg, HandleError<ProtocolMsg>>;

  #[inline]
  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    self.spawn_to_handle_internal_msg(protocol_msg, ctx);
    Ok(ProtocolMsg::None)
  }
}

impl WsHandler {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    Self {
      inner: Rc::new(WsHandlerInner::new(req)),
      receive_buf: BytesMut::new(),
      received_last: false,
      check_timer: SpawnHandle::default(),
      active_at: Instant::now(),
    }
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
