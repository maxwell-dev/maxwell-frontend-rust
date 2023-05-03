use std::time::Duration;

use actix::prelude::{Addr, Recipient};
use maxwell_client::prelude::*;
use maxwell_protocol::{ProtocolMsg, SendError};
use once_cell::sync::Lazy;

use crate::config::CONFIG;

pub struct MasterClient {
  _endpoints: Vec<String>,
  connection: Addr<Connection>,
}

impl MasterClient {
  pub fn new(endpoints: &Vec<String>) -> Self {
    let connection = Connection::start2(endpoints[0].clone());
    MasterClient { _endpoints: endpoints.clone(), connection }
  }

  pub async fn send(&self, msg: ProtocolMsg) -> Result<ProtocolMsg, SendError> {
    self.connection.send(msg).timeout_ext(Duration::from_secs(5)).await
  }

  pub fn subscribe_connection_status_changed(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.connection.do_send(SubscribeConnectionStatusMsg(r))
  }

  pub fn unsubscribe_connection_status_changed(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.connection.do_send(UnsubscribeConnectionStatusMsg(r))
  }
}

pub static MASTER_CLIENT: Lazy<MasterClient> =
  Lazy::new(|| MasterClient::new(&CONFIG.master_endpoints));

#[cfg(test)]
mod tests {
  #[actix::test]
  async fn test_send() {}
}
