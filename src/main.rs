#[macro_use]
extern crate serde_derive;

mod config;
mod handler;
mod master_client;
mod registrar;
mod route_syncer;
mod route_table;
mod topic_localizer;

use actix::prelude::*;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use route_syncer::RouteSyncer;

use crate::config::CONFIG;
use crate::handler::Handler;
use crate::registrar::Registrar;

async fn ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let resp = ws::WsResponseBuilder::new(Handler::new(&req), &req, stream)
    .frame_size(CONFIG.server.max_frame_size)
    .start();
  log::info!("http req: {:?}, resp: {:?}", req, resp);
  resp
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  Registrar::new().start();
  RouteSyncer::new().start();

  HttpServer::new(move || App::new().route("/$ws", web::get().to(ws)))
    .backlog(CONFIG.server.backlog)
    .keep_alive(CONFIG.server.keep_alive)
    .max_connection_rate(CONFIG.server.max_connection_rate)
    .max_connections(CONFIG.server.max_connections)
    .workers(CONFIG.server.workers)
    .bind(format!("{}:{}", "0.0.0.0", CONFIG.server.http_port))
    .unwrap()
    .run()
    .await
    .unwrap();
}
