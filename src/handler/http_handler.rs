use std::time::Duration;

use actix_http::{uri::PathAndQuery, Uri};
use actix_web::{http::Method, web, HttpRequest, HttpResponse, HttpResponseBuilder};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{channel::mpsc::unbounded, SinkExt, StreamExt};
use once_cell::sync::Lazy;
use reqwest::{
  Body as ReqwestBody, Client as ReqwestClient, ClientBuilder as ReqwestClientBuilder,
  RequestBuilder as ReqwestRequestBuilder, Response as ReqwestResponse,
};
use tokio::task::JoinHandle;

use crate::route_table::ROUTE_TABLE;

static EMPTY_PNQ: Lazy<PathAndQuery> = Lazy::new(|| PathAndQuery::from_static(""));

static REQWEST: Lazy<ReqwestClient> = Lazy::new(|| {
  ReqwestClientBuilder::new()
    .no_proxy()
    .connect_timeout(Duration::from_secs(3))
    .timeout(Duration::from_secs(30))
    .pool_idle_timeout(Duration::from_secs(90))
    .pool_max_idle_per_host(128)
    .build()
    .unwrap()
});

pub struct HttpHandler {}

impl HttpHandler {
  #[inline]
  pub async fn handle_get(req: &HttpRequest) -> Result<HttpResponse, Error> {
    let reqwest_req_builder = Self::build_reqwest_req_builder(req)?;
    let reqwest_resp = reqwest_req_builder.send().await?;
    let mut resp_builder = Self::build_resp_builder(&reqwest_resp);
    Ok(resp_builder.streaming(reqwest_resp.bytes_stream()))
  }

  #[inline]
  pub async fn handle_request(
    req: &HttpRequest, mut body: web::Payload,
  ) -> Result<HttpResponse, Error> {
    let reqwest_req_builder = Self::build_reqwest_req_builder(req)?;
    let (mut tx, rx) = unbounded::<Result<Bytes, Error>>();
    let join_handle: JoinHandle<Result<ReqwestResponse, Error>> = tokio::spawn(async move {
      let reqwest_resp = reqwest_req_builder.body(ReqwestBody::wrap_stream(rx)).send().await?;
      Ok(reqwest_resp)
    });
    while let Some(chunk) = body.next().await {
      tx.send(Ok(chunk?)).await?
    }
    let reqwest_resp = join_handle.await??;
    let mut resp_builder = Self::build_resp_builder(&reqwest_resp);
    Ok(resp_builder.streaming(reqwest_resp.bytes_stream()))
  }

  #[inline(always)]
  fn build_reqwest_req_builder(req: &HttpRequest) -> Result<ReqwestRequestBuilder> {
    if let Some(endpoint) = Self::next_endpoint(req.method(), req.path()) {
      let mut reqwest_req_builder =
        REQWEST.request(req.method().clone(), Self::build_url(&endpoint, req.uri()));
      for header in req.headers() {
        reqwest_req_builder = reqwest_req_builder.header(header.0, header.1);
      }
      if let Some(peer_addr) = req.peer_addr() {
        reqwest_req_builder =
          reqwest_req_builder.header("X-Forwarded-For", format!("{}", peer_addr));
      }
      Ok(reqwest_req_builder)
    } else {
      Err(anyhow!(format!("Failed to find endpoint: path: {:?}", req.path())))
    }
  }

  #[inline(always)]
  fn build_resp_builder(reqwest_resp: &ReqwestResponse) -> HttpResponseBuilder {
    let mut resp_builder = HttpResponse::build(reqwest_resp.status());
    for header in reqwest_resp.headers() {
      resp_builder.insert_header(header.clone());
    }
    resp_builder
  }

  #[inline(always)]
  fn next_endpoint<S: AsRef<str>>(method: &Method, path: S) -> Option<String> {
    match *method {
      Method::GET => ROUTE_TABLE.next_get_endpoint(path),
      Method::POST => ROUTE_TABLE.next_post_endpoint(path),
      Method::PUT => ROUTE_TABLE.next_put_endpoint(path),
      Method::PATCH => ROUTE_TABLE.next_patch_endpoint(path),
      Method::DELETE => ROUTE_TABLE.next_delete_endpoint(path),
      Method::HEAD => ROUTE_TABLE.next_head_endpoint(path),
      Method::OPTIONS => ROUTE_TABLE.next_options_endpoint(path),
      Method::TRACE => ROUTE_TABLE.next_trace_endpoint(path),
      _ => None,
    }
  }

  #[inline(always)]
  fn build_url(endpoint: &String, uri: &Uri) -> String {
    format!("http://{}{}", endpoint, uri.path_and_query().unwrap_or_else(|| { &EMPTY_PNQ }))
  }
}
