#[macro_use]
extern crate serde_derive;

mod config;
mod handler;
mod master_client;
mod registrar;
mod route_syncer;
mod route_table;

use actix::prelude::*;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use route_syncer::RouteSyncer;

use crate::config::CONFIG;
use crate::handler::Handler;
use crate::registrar::Registrar;

async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let resp = ws::start(Handler::new(), &req, stream);
  resp
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  Registrar::new().start();
  RouteSyncer::new().start();

  HttpServer::new(move || {
    App::new().wrap(middleware::Logger::default()).route("/", web::get().to(index))
  })
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}
