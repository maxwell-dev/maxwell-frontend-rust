#[macro_use]
extern crate serde_derive;

mod config;
mod handler;
mod master_client;
mod registrar;
mod route_syncer;
mod route_table;
mod topic_cleaner;
mod topic_localizer;

use std::{fs::File, io::BufReader};

use actix::prelude::*;
use actix_cors::Cors;
use actix_web::{
  error::ErrorInternalServerError, middleware, web, App, Error, HttpRequest, HttpResponse,
  HttpServer,
};
use actix_web_actors::ws;
use anyhow::{anyhow, Result};
use futures::future;
use route_syncer::RouteSyncer;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use topic_cleaner::TopicCleaner;

use crate::config::CONFIG;
use crate::handler::{HttpHandler, WsHandler};
use crate::registrar::Registrar;

async fn ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let resp = ws::WsResponseBuilder::new(WsHandler::new(&req), &req, stream)
    .frame_size(CONFIG.server.max_frame_size)
    .start();
  log::info!("ws req: {:?}, resp: {:?}", req, resp);
  resp
}

async fn get(req: HttpRequest) -> Result<HttpResponse, Error> {
  let resp = HttpHandler::handle_get(&req).await.or_else(|err| Err(ErrorInternalServerError(err)));
  log::info!("http req: {:?}, resp: {:?}", req, resp);
  resp
}

async fn other(req: HttpRequest, body: web::Payload) -> Result<HttpResponse, Error> {
  let resp =
    HttpHandler::handle_request(&req, body).await.or_else(|err| Err(ErrorInternalServerError(err)));
  log::info!("http req: {:?}, resp: {:?}", req, resp);
  resp
}

#[actix_web::main]
async fn main() -> Result<()> {
  // console_subscriber::init();
  log4rs::init_file("config/log4rs.yaml", Default::default())?;

  Registrar::new().start();
  RouteSyncer::new().start();
  TopicCleaner::new().start();

  future::try_join(create_http_server(false), create_http_server(true)).await?;
  Ok(())
}

async fn create_http_server(is_https: bool) -> Result<()> {
  let http_server = HttpServer::new(move || {
    App::new()
      .wrap(middleware::Logger::default())
      .wrap(middleware::Compress::default())
      .wrap(
        Cors::default()
          .allow_any_header()
          .allow_any_origin()
          .send_wildcard()
          .block_on_origin_mismatch(false)
          .expose_any_header()
          .max_age(None),
      )
      .route("/$ws", web::get().to(ws))
      .route("/{pnq:.*}", web::get().to(get))
      .route("/{pnq:.*}", web::route().to(other))
  })
  .backlog(CONFIG.server.backlog)
  .keep_alive(CONFIG.server.keep_alive)
  .max_connection_rate(CONFIG.server.max_connection_rate)
  .max_connections(CONFIG.server.max_connections)
  .workers(CONFIG.server.workers);

  if is_https {
    http_server.bind_rustls_021(
      format!("{}:{}", "0.0.0.0", CONFIG.server.https_port),
      create_tls_config()?,
    )?
  } else {
    http_server.bind(format!("{}:{}", "0.0.0.0", CONFIG.server.http_port))?
  }
  .run()
  .await
  .map_err(|err| anyhow!("Failed to run the server: err: {:?}", err))
}

fn create_tls_config() -> Result<ServerConfig> {
  let cert_file = File::open(CONFIG.server.cert_file.clone())?;
  let key_file = File::open(CONFIG.server.key_file.clone())?;

  let cert_buf = &mut BufReader::new(cert_file);
  let key_buf = &mut BufReader::new(key_file);

  let cert_chain = certs(cert_buf)?.into_iter().map(Certificate).collect();
  let mut keys = pkcs8_private_keys(key_buf)?;

  Ok(
    ServerConfig::builder()
      .with_safe_defaults()
      .with_no_client_auth()
      .with_single_cert(cert_chain, PrivateKey(keys.remove(0)))?,
  )
}
