use std::time::Duration;

use actix::prelude::{Addr, Recipient};
use maxwell_protocol::{HandleError, ProtocolMsg};
use maxwell_utils::prelude::{
  ConnectionOptions, FutureStyleConnection, ObservableEvent, ObserveObservableEventWithActorMsg,
  TimeoutExt, UnobserveObservableEventWithActorMsg,
};
use once_cell::sync::Lazy;

use crate::config::CONFIG;

pub struct MasterClient {
  connection: Addr<FutureStyleConnection>,
}

impl MasterClient {
  pub fn new(endpoints: &Vec<String>) -> Self {
    let connection = FutureStyleConnection::start_with_alt_endpoints2(
      endpoints.clone(),
      ConnectionOptions::default(),
    );
    MasterClient { connection }
  }

  pub fn observe_connection_event(&self, recip: Recipient<ObservableEvent>) {
    self.connection.do_send(ObserveObservableEventWithActorMsg { recip })
  }

  pub fn unobserve_connection_event(&self, recip: Recipient<ObservableEvent>) {
    self.connection.do_send(UnobserveObservableEventWithActorMsg { recip })
  }

  pub async fn send(&self, msg: ProtocolMsg) -> Result<ProtocolMsg, HandleError<ProtocolMsg>> {
    self.connection.send(msg).timeout_ext(Duration::from_secs(5)).await
  }
}

pub static MASTER_CLIENT: Lazy<MasterClient> =
  Lazy::new(|| MasterClient::new(&CONFIG.master.private_ctrl_endpoints));

#[cfg(test)]
mod tests {
  #[actix::test]
  async fn test_send() {}
}
