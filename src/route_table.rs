use ahash::RandomState as AHasher;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use indexmap::IndexSet;
use once_cell::sync::Lazy;

type AIndexSet<T> = IndexSet<T, AHasher>;

#[derive(Debug)]
struct EndpointSet {
  healthy_endpoints: AIndexSet<String>,
  unhealthy_endpoints: AIndexSet<String>,
  healthy_index: usize,
  unhealthy_index: usize,
}

impl EndpointSet {
  pub fn new() -> Self {
    EndpointSet {
      healthy_endpoints: AIndexSet::with_capacity_and_hasher(64, AHasher::default()),
      unhealthy_endpoints: AIndexSet::with_capacity_and_hasher(64, AHasher::default()),
      healthy_index: 0,
      unhealthy_index: 0,
    }
  }

  pub fn replace_healthy_endpoints(&mut self, endpoints: AIndexSet<String>) {
    self.healthy_endpoints = endpoints;
  }

  pub fn replace_unhealthy_endpoints(&mut self, endpoints: AIndexSet<String>) {
    self.unhealthy_endpoints = endpoints;
  }

  pub fn next_endpoint(&mut self) -> Option<String> {
    if let Some(endpoint) = self.next_healthy_endpoint() {
      return Some(endpoint);
    }
    if let Some(endpoint) = self.next_unhealthy_endpoint() {
      return Some(endpoint);
    }
    return None;
  }

  fn next_healthy_endpoint(&mut self) -> Option<String> {
    let len = self.healthy_endpoints.len();
    if len <= 0 {
      return None;
    }
    let next_index = if self.healthy_index < len - 1 { self.healthy_index + 1 } else { 0 };
    if let Some(endpoint) = self.healthy_endpoints.get_index(next_index) {
      self.healthy_index = next_index;
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }

  fn next_unhealthy_endpoint(&mut self) -> Option<String> {
    let len = self.unhealthy_endpoints.len();
    if len <= 0 {
      return None;
    }
    let next_index = if self.unhealthy_index < len - 1 { self.unhealthy_index + 1 } else { 0 };
    if let Some(endpoint) = self.healthy_endpoints.get_index(next_index) {
      self.unhealthy_index = next_index;
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }
}

#[derive(Debug)]
pub struct RouteTable {
  route_groups: DashMap<String, EndpointSet, AHasher>,
}

impl RouteTable {
  fn new() -> Self {
    RouteTable { route_groups: DashMap::with_capacity_and_hasher(64, AHasher::default()) }
  }

  pub fn add_route_group(
    &self, path: String, healthy_endpoints: Vec<String>, unhealthy_endpoints: Vec<String>,
  ) {
    let mut route_group = self.route_groups.entry(path).or_insert_with(|| {
      let route_group = EndpointSet::new();
      route_group
    });
    route_group.replace_healthy_endpoints(healthy_endpoints.into_iter().collect());
    route_group.replace_unhealthy_endpoints(unhealthy_endpoints.into_iter().collect());
  }

  pub fn next_endpoint(&self, path: &String) -> Option<String> {
    match self.route_groups.entry(path.clone()) {
      DashEntry::Occupied(mut occupied) => {
        let route_group = occupied.get_mut();
        let endpoint = route_group.next_endpoint();
        if endpoint.is_some() {
          endpoint
        } else {
          occupied.remove();
          None
        }
      }
      DashEntry::Vacant(_) => None,
    }
  }
}

pub static ROUTE_TABLE: Lazy<RouteTable> = Lazy::new(|| RouteTable::new());
