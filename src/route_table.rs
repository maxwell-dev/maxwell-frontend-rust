use ahash::{AHashSet, RandomState as AHasher};
use dashmap::DashMap;
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
  #[inline]
  pub fn new() -> Self {
    EndpointSet {
      healthy_endpoints: AIndexSet::with_capacity_and_hasher(64, AHasher::default()),
      unhealthy_endpoints: AIndexSet::with_capacity_and_hasher(64, AHasher::default()),
      healthy_index: 0,
      unhealthy_index: 0,
    }
  }

  #[inline]
  pub fn replace_healthy_endpoints(&mut self, endpoints: AIndexSet<String>) {
    self.healthy_endpoints = endpoints;
  }

  #[inline]
  pub fn replace_unhealthy_endpoints(&mut self, endpoints: AIndexSet<String>) {
    self.unhealthy_endpoints = endpoints;
  }

  #[inline]
  pub fn next_endpoint(&mut self) -> Option<String> {
    if let Some(endpoint) = self.next_healthy_endpoint() {
      return Some(endpoint);
    }
    if let Some(endpoint) = self.next_unhealthy_endpoint() {
      return Some(endpoint);
    }
    return None;
  }

  // #[inline]
  // pub fn get_endpoint(&self, index_seed: u32) -> Option<String> {
  //   if let Some(endpoint) = self.get_healthy_endpoint(index_seed) {
  //     return Some(endpoint);
  //   }
  //   if let Some(endpoint) = self.get_unhealthy_endpoint(index_seed) {
  //     return Some(endpoint);
  //   }
  //   return None;
  // }

  #[inline]
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

  #[inline]
  fn next_unhealthy_endpoint(&mut self) -> Option<String> {
    let len = self.unhealthy_endpoints.len();
    if len <= 0 {
      return None;
    }
    let next_index = if self.unhealthy_index < len - 1 { self.unhealthy_index + 1 } else { 0 };
    if let Some(endpoint) = self.unhealthy_endpoints.get_index(next_index) {
      self.unhealthy_index = next_index;
      return Some(endpoint.clone());
    } else {
      return None;
    }
  }

  // #[inline]
  // fn get_healthy_endpoint(&self, index_seed: u32) -> Option<String> {
  //   let len = self.healthy_endpoints.len();
  //   if len <= 0 {
  //     return None;
  //   }
  //   let index = (index_seed % len as u32) as usize;
  //   if let Some(endpoint) = self.healthy_endpoints.get_index(index) {
  //     return Some(endpoint.clone());
  //   } else {
  //     return None;
  //   }
  // }

  // #[inline]
  // fn get_unhealthy_endpoint(&self, index_seed: u32) -> Option<String> {
  //   let len = self.unhealthy_endpoints.len();
  //   if len <= 0 {
  //     return None;
  //   }
  //   let index = (index_seed % len as u32) as usize;
  //   if let Some(endpoint) = self.unhealthy_endpoints.get_index(index) {
  //     return Some(endpoint.clone());
  //   } else {
  //     return None;
  //   }
  // }
}

#[derive(Debug)]
pub struct RouteTable {
  route_groups: DashMap<String, EndpointSet, AHasher>,
}

impl RouteTable {
  #[inline]
  fn new() -> Self {
    RouteTable { route_groups: DashMap::with_capacity_and_hasher(64, AHasher::default()) }
  }

  #[inline]
  pub fn set_route_group(
    &self, path: String, healthy_endpoints: Vec<String>, unhealthy_endpoints: Vec<String>,
  ) {
    let mut route_group = self.route_groups.entry(path).or_insert_with(|| {
      let route_group = EndpointSet::new();
      route_group
    });
    route_group.replace_healthy_endpoints(healthy_endpoints.into_iter().collect());
    route_group.replace_unhealthy_endpoints(unhealthy_endpoints.into_iter().collect());
  }

  #[inline]
  pub fn remove_if_not_in(&self, paths: AHashSet<String>) {
    let mut stale_paths = Vec::with_capacity(self.route_groups.len());
    for route_group in self.route_groups.iter() {
      if !paths.contains(route_group.key()) {
        stale_paths.push(route_group.key().clone());
      }
    }
    for stale_path in stale_paths {
      self.route_groups.remove(&stale_path);
    }
  }

  #[allow(dead_code)]
  #[inline]
  pub fn next_endpoint(&self, path: &String) -> Option<String> {
    self.route_groups.get_mut(path).and_then(|mut route_group| route_group.next_endpoint())
  }

  // #[inline]
  // pub fn get_endpoint(&self, path: &String, index_seed: u32) -> Option<String> {
  //   self.route_groups.get(path).and_then(|route_group| route_group.get_endpoint(index_seed))
  // }
}

pub static ROUTE_TABLE: Lazy<RouteTable> = Lazy::new(|| RouteTable::new());
