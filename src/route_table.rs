use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};

use ahash::{AHashMap, RandomState as AHasher};
use anyhow::{anyhow, Error, Result};
use arc_swap::ArcSwap;
use indexmap::IndexSet;
use matchit::Router;
use maxwell_protocol::RouteGroup;
use once_cell::sync::Lazy;

type AIndexSet<T> = IndexSet<T, AHasher>;

#[derive(Debug)]
struct EndpointSet {
  healthy_endpoints: AIndexSet<String>,
  unhealthy_endpoints: AIndexSet<String>,
  healthy_index_seed: AtomicUsize,
  unhealthy_index_seed: AtomicUsize,
}

impl EndpointSet {
  #[inline]
  pub fn new(healthy_endpoints: AIndexSet<String>, unhealthy_endpoints: AIndexSet<String>) -> Self {
    EndpointSet {
      healthy_endpoints,
      unhealthy_endpoints,
      healthy_index_seed: AtomicUsize::new(usize::MAX),
      unhealthy_index_seed: AtomicUsize::new(usize::MAX),
    }
  }

  #[inline]
  pub fn next_endpoint(&self) -> Option<String> {
    if let Some(endpoint) = self.next_healthy_endpoint() {
      return Some(endpoint);
    }
    if let Some(endpoint) = self.next_unhealthy_endpoint() {
      return Some(endpoint);
    }
    return None;
  }

  #[inline]
  pub fn get_endpoint(&self, index_seed: u32) -> Option<String> {
    if let Some(endpoint) = self.get_healthy_endpoint(index_seed) {
      return Some(endpoint);
    }
    if let Some(endpoint) = self.get_unhealthy_endpoint(index_seed) {
      return Some(endpoint);
    }
    return None;
  }

  #[inline]
  fn next_healthy_endpoint(&self) -> Option<String> {
    let len = self.healthy_endpoints.len();
    if len <= 0 {
      return None;
    }

    let next_index = self.healthy_index_seed.fetch_add(1, Ordering::Relaxed) % len;

    if let Some(endpoint) = self.healthy_endpoints.get_index(next_index) {
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }

  #[inline]
  fn next_unhealthy_endpoint(&self) -> Option<String> {
    let len = self.unhealthy_endpoints.len();
    if len <= 0 {
      return None;
    }

    let next_index = self.unhealthy_index_seed.fetch_add(1, Ordering::Relaxed) % len;

    if let Some(endpoint) = self.unhealthy_endpoints.get_index(next_index) {
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }

  #[inline]
  fn get_healthy_endpoint(&self, index_seed: u32) -> Option<String> {
    let len = self.healthy_endpoints.len();
    if len <= 0 {
      return None;
    }
    let index = (index_seed % len as u32) as usize;
    if let Some(endpoint) = self.healthy_endpoints.get_index(index) {
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }

  #[inline]
  fn get_unhealthy_endpoint(&self, index_seed: u32) -> Option<String> {
    let len = self.unhealthy_endpoints.len();
    if len <= 0 {
      return None;
    }
    let index = (index_seed % len as u32) as usize;
    if let Some(endpoint) = self.unhealthy_endpoints.get_index(index) {
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }
}

pub struct RouteTable {
  ws_route_groups: ArcSwap<AHashMap<String, EndpointSet>>,
  get_route_groups: ArcSwap<Router<EndpointSet>>,
  post_route_groups: ArcSwap<Router<EndpointSet>>,
  put_route_groups: ArcSwap<Router<EndpointSet>>,
  patch_route_groups: ArcSwap<Router<EndpointSet>>,
  delete_route_groups: ArcSwap<Router<EndpointSet>>,
  head_route_groups: ArcSwap<Router<EndpointSet>>,
  options_route_groups: ArcSwap<Router<EndpointSet>>,
  trace_route_groups: ArcSwap<Router<EndpointSet>>,
}

impl RouteTable {
  #[inline]
  fn new() -> Self {
    RouteTable {
      ws_route_groups: ArcSwap::from_pointee(AHashMap::new()),
      get_route_groups: ArcSwap::from_pointee(Router::new()),
      post_route_groups: ArcSwap::from_pointee(Router::new()),
      put_route_groups: ArcSwap::from_pointee(Router::new()),
      patch_route_groups: ArcSwap::from_pointee(Router::new()),
      delete_route_groups: ArcSwap::from_pointee(Router::new()),
      head_route_groups: ArcSwap::from_pointee(Router::new()),
      options_route_groups: ArcSwap::from_pointee(Router::new()),
      trace_route_groups: ArcSwap::from_pointee(Router::new()),
    }
  }

  #[inline]
  pub fn set_ws_route_groups(&self, ws_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    let mut new_ws_route_groups = AHashMap::with_capacity(ws_route_groups.len());
    for ws_route in ws_route_groups.iter() {
      let path = ws_route.path.clone();
      let endpoint_set = EndpointSet::new(
        ws_route.healthy_endpoints.clone().into_iter().collect(),
        ws_route.unhealthy_endpoints.clone().into_iter().collect(),
      );
      new_ws_route_groups.insert(path, endpoint_set);
    }
    self.ws_route_groups.store(Arc::new(new_ws_route_groups));
    Ok(())
  }

  #[inline]
  pub fn next_ws_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    self
      .ws_route_groups
      .load()
      .get(path.as_ref())
      .and_then(|ws_route_group| ws_route_group.next_endpoint())
  }

  #[allow(dead_code)]
  #[inline]
  pub fn get_ws_endpoint<S: AsRef<str>>(&self, path: S, index_seed: u32) -> Option<String> {
    self
      .ws_route_groups
      .load()
      .get(path.as_ref())
      .and_then(|ws_route_group| ws_route_group.get_endpoint(index_seed))
  }

  #[inline]
  pub fn set_get_route_groups(&self, get_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.get_route_groups, get_route_groups)
  }

  #[inline]
  pub fn next_get_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.get_route_groups, path)
  }

  #[inline]
  pub fn set_post_route_groups(&self, post_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.post_route_groups, post_route_groups)
  }

  #[inline]
  pub fn next_post_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.post_route_groups, path)
  }

  #[inline]
  pub fn set_put_route_groups(&self, put_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.put_route_groups, put_route_groups)
  }

  #[inline]
  pub fn next_put_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.put_route_groups, path)
  }

  #[inline]
  pub fn set_patch_route_groups(&self, patch_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.patch_route_groups, patch_route_groups)
  }

  #[inline]
  pub fn next_patch_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.patch_route_groups, path)
  }

  #[inline]
  pub fn set_delete_route_groups(&self, delete_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.delete_route_groups, delete_route_groups)
  }

  #[inline]
  pub fn next_delete_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.delete_route_groups, path)
  }

  #[inline]
  pub fn set_head_route_groups(&self, head_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.head_route_groups, head_route_groups)
  }

  #[inline]
  pub fn next_head_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.head_route_groups, path)
  }

  #[inline]
  pub fn set_options_route_groups(
    &self, options_route_groups: Vec<RouteGroup>,
  ) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.options_route_groups, options_route_groups)
  }

  #[inline]
  pub fn next_options_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.options_route_groups, path)
  }

  #[inline]
  pub fn set_trace_route_groups(&self, trace_route_groups: Vec<RouteGroup>) -> Result<(), Error> {
    Self::set_xxx_route_groups(&self.trace_route_groups, trace_route_groups)
  }

  #[inline]
  pub fn next_trace_endpoint<S: AsRef<str>>(&self, path: S) -> Option<String> {
    Self::next_xxx_endpoint(&self.trace_route_groups, path)
  }

  #[inline]
  fn set_xxx_route_groups(
    dest: &ArcSwap<Router<EndpointSet>>, src: Vec<RouteGroup>,
  ) -> Result<(), Error> {
    let mut router = Router::new();
    for route_group in src.iter() {
      let path = route_group.path.clone();
      let endpoint_set = EndpointSet::new(
        route_group.healthy_endpoints.clone().into_iter().collect(),
        route_group.unhealthy_endpoints.clone().into_iter().collect(),
      );
      router.insert(path, endpoint_set).map_err(|err| anyhow!(format!("{:?}", err)))?;
    }
    dest.store(Arc::new(router));
    Ok(())
  }

  #[inline]
  fn next_xxx_endpoint<S: AsRef<str>>(
    dest: &ArcSwap<Router<EndpointSet>>, path: S,
  ) -> Option<String> {
    match dest.load().at(path.as_ref()) {
      Ok(m) => m.value.next_endpoint(),
      Err(_) => None,
    }
  }
}

pub static ROUTE_TABLE: Lazy<RouteTable> = Lazy::new(|| RouteTable::new());
