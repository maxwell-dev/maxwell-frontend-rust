use std::time::Duration;

use anyhow::{Error, Result};
use maxwell_protocol::*;
use maxwell_utils::prelude::{ConnectionOptions, FutureStyleConnection, TimeoutExt};
use once_cell::sync::Lazy;

use crate::config::CONFIG;

pub struct IpResolver;

pub static IP_RESOLVER: Lazy<IpResolver> = Lazy::new(|| IpResolver::new());

impl IpResolver {
  pub fn new() -> Self {
    IpResolver
  }

  pub async fn resolve_ip(&self) -> Result<String> {
    let conn = FutureStyleConnection::start_with_alt_endpoints2(
      CONFIG.master.public_ctrl_endpoints.clone(),
      ConnectionOptions::default(),
    );

    let req = ResolveIpReq { r#ref: 0 };
    log::info!("Resolving ip: req: {:?}", req);
    match conn.send(req.into_enum()).timeout_ext(Duration::from_secs(5)).await {
      Ok(rep) => match rep {
        ProtocolMsg::ResolveIpRep(rep) => {
          log::info!("Resolved ip: rep: {:?}", rep);
          Ok(rep.ip)
        }
        err => Err(Error::msg(format!("Failed to resolve ip: err: {:?}", err))),
      },
      Err(err) => Err(Error::msg(format!("Failed to resolve ip: err: {:?}", err))),
    }
  }
}
