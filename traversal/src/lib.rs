pub mod blockstore;
mod empty_map;
pub mod link_system;
pub mod path_segment;
pub mod selector;
pub mod unixfs;

use libipld::{Cid, Ipld};
use path_segment::PathSegment;
use smallvec::SmallVec;
use std::{collections::HashSet, vec};
use thiserror::Error;
use unixfs::resolve_unixfs;

pub use link_system::{BlockLoader, IpldLoader, LinkSystem, Prefix};
pub use selector::Selector;

#[derive(Error, Debug, PartialEq)]
pub enum IterError {
    #[error("block not found for {0}")]
    NotFound(Cid),
    #[error("skip block for {0}")]
    SkipMe(Cid),
    #[error("failed to decode: {0}")]
    Decode(String),
    #[error("{0}")]
    Other(&'static str),
}

/// Executes an iterative traversal based on the given root CID and selector
/// and returns the blocks resolved along the way.
#[derive(Debug)]
pub struct BlockIterator<L> {
    loader: L,
    selector: Selector,
    start: Option<Cid>,
    stack_list: SmallVec<[vec::IntoIter<(Ipld, Selector)>; 8]>,
    seen: Option<HashSet<Cid>>,
    restart: bool,
}

impl<L> Iterator for BlockIterator<L>
where
    L: BlockLoader,
{
    type Item = Result<(Cid, Vec<u8>), IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cid) = self.start.take() {
            if let Some(block) = self.handle_node(Ipld::Link(cid), self.selector.clone()) {
                return Some(block);
            }
        }
        while !self.stack_list.is_empty() {
            let next = self
                .stack_list
                .last_mut()
                .expect("stack should be non-empty")
                .next();
            match next {
                None => {
                    self.stack_list.pop();
                }
                Some((node, selector)) => {
                    if let Some(block) = self.handle_node(node, selector) {
                        return Some(block);
                    }
                }
            }
        }
        None
    }
}

impl<L> BlockIterator<L>
where
    L: BlockLoader,
{
    pub fn new(loader: L, root: Cid, selector: Selector) -> Self {
        Self {
            loader,
            selector,
            start: Some(root),
            stack_list: SmallVec::new(),
            seen: None,
            restart: false,
        }
    }
    /// if activated, the traversal will not revisit the same block if it is linked
    /// somewhere else.
    pub fn ignore_duplicate_links(mut self) -> Self {
        self.seen = Some(HashSet::default());
        self
    }
    /// if activated, the traversal will always reattempt to traverse from the
    /// last missing link. May cause an infinite loop if the the block is never inserted
    /// into the underlying blockstore. To continue traversal even if a block is missing,
    /// the underlying blockstore should return a SkipMe or custom error.
    pub fn restart_missing_link(mut self) -> Self {
        self.restart = true;
        self
    }

    fn handle_node(
        &mut self,
        mut ipld: Ipld,
        mut selector: Selector,
    ) -> Option<Result<(Cid, Vec<u8>), IterError>> {
        let maybe_block = self.maybe_resolve_link(&mut ipld);

        if let Selector::ExploreInterpretAs { next, reifier } = selector.clone() {
            // only handle unixfs use case until a different one is needed
            if &reifier == "unixfs" {
                if let Some(reified) = resolve_unixfs(&ipld) {
                    ipld = reified;
                    selector = *next;
                }
            }
        }

        match ipld {
            Ipld::Map(_) | Ipld::List(_) => self.push(ipld, selector.clone()),
            _ => {}
        }
        match maybe_block {
            Some(Err(IterError::NotFound(cid))) => {
                // if the block is missing the next iteration will restart from there
                if self.restart {
                    self.start = Some(cid);
                    self.selector = selector;
                }
                Some(Err(IterError::NotFound(cid)))
            }
            mb => mb,
        }
    }

    fn maybe_resolve_link(&mut self, ipld: &mut Ipld) -> Option<Result<(Cid, Vec<u8>), IterError>> {
        let mut result = None;
        while let Ipld::Link(cid) = ipld {
            let c = *cid;
            if let Some(ref mut seen) = self.seen {
                if !seen.insert(c) {
                    break;
                }
            }
            let (node, block) = match self.loader.load_plus_raw(c) {
                Ok(nb_tuple) => nb_tuple,
                Err(_) => return Some(Err(IterError::NotFound(c))),
            };
            *ipld = node;
            result = Some(Ok((c, block)));
        }
        result
    }

    fn push(&mut self, ipld: Ipld, selector: Selector) {
        self.stack_list
            .push(select_next_entries(ipld, selector).into_iter());
    }
}

/// Executes an iterative traversal based on the given root CID and selector
/// and returns the decoded IPLD nodes resolved along the way.
#[derive(Debug)]
pub struct IpldTraversal<L> {
    loader: L,
    selector: Selector,
    start: Option<Cid>,
    stack_list: SmallVec<[vec::IntoIter<(Ipld, Selector)>; 8]>,
    seen: Option<HashSet<Cid>>,
    restart: bool,
}

impl<L> Iterator for IpldTraversal<L>
where
    L: IpldLoader,
{
    type Item = Result<Ipld, IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cid) = self.start.take() {
            if let Some(block) = self.handle_node(Ipld::Link(cid), self.selector.clone()) {
                return Some(block);
            }
        }
        while !self.stack_list.is_empty() {
            let next = self
                .stack_list
                .last_mut()
                .expect("stack should be non-empty")
                .next();
            match next {
                None => {
                    self.stack_list.pop();
                }
                Some((node, selector)) => {
                    if let Some(n) = self.handle_node(node, selector) {
                        return Some(n);
                    }
                }
            }
        }
        None
    }
}

impl<L> IpldTraversal<L>
where
    L: IpldLoader,
{
    pub fn new(loader: L, root: Cid, selector: Selector) -> Self {
        Self {
            loader,
            selector,
            start: Some(root),
            stack_list: SmallVec::new(),
            seen: None,
            restart: false,
        }
    }
    /// if activated, the traversal will always reattempt to traverse from the
    /// last missing link. May cause an infinite loop if the the block is never inserted
    /// into the underlying blockstore. To continue traversal even if a block is missing,
    /// the underlying blockstore should return a SkipMe or custom error.
    pub fn restart_missing_link(mut self, restart: bool) -> Self {
        self.restart = restart;
        self
    }

    fn handle_node(
        &mut self,
        mut ipld: Ipld,
        mut selector: Selector,
    ) -> Option<Result<Ipld, IterError>> {
        let maybe_node = self.maybe_resolve_link(&mut ipld);

        if let Selector::ExploreInterpretAs { next, reifier } = selector.clone() {
            // only handle unixfs use case until a different one is needed
            if &reifier == "unixfs" {
                if let Some(reified) = resolve_unixfs(&ipld) {
                    ipld = reified;
                    selector = *next;
                }
            }
        }

        match ipld {
            Ipld::Map(_) | Ipld::List(_) => self.push(ipld, selector.clone()),
            _ => {}
        }
        match maybe_node {
            Some(Err(IterError::NotFound(cid))) => {
                // if the block is missing the next iteration will restart from there
                if self.restart {
                    self.start = Some(cid);
                    self.selector = selector;
                }
                Some(Err(IterError::NotFound(cid)))
            }
            mn => mn,
        }
    }

    fn maybe_resolve_link(&mut self, ipld: &mut Ipld) -> Option<Result<Ipld, IterError>> {
        let mut result = None;
        while let Ipld::Link(cid) = ipld {
            let c = *cid;
            if let Some(ref mut seen) = self.seen {
                if !seen.insert(c) {
                    break;
                }
            }
            let node = match self.loader.load(c) {
                Ok(node) => node,
                Err(_) => return Some(Err(IterError::NotFound(c))),
            };
            *ipld = node;
            result = Some(Ok(()));
        }
        result.map(|inner| inner.map(|_| ipld.clone()))
    }

    fn push(&mut self, ipld: Ipld, selector: Selector) {
        self.stack_list
            .push(select_next_entries(ipld, selector).into_iter());
    }
}

/// returns a list of ipld values selected by the given selector.
/// TODO: there should be a way to return an iterator so it can be lazily evaluated.
fn select_next_entries(ipld: Ipld, selector: Selector) -> Vec<(Ipld, Selector)> {
    match selector.interests() {
        Some(attn) => attn
            .into_iter()
            .filter_map(|ps| {
                if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                    let v = match ipld.get(ps.ipld_index()) {
                        Ok(node) => node,
                        _ => return None,
                    };
                    return Some((v.clone(), next_sel));
                }
                None
            })
            .collect(),
        None => match &ipld {
            Ipld::Map(m) => m
                .keys()
                .filter_map(|k| {
                    let ps = PathSegment::from(k.as_ref());
                    if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                        let v = match ipld.get(ps.ipld_index()) {
                            Ok(node) => node,
                            _ => return None,
                        };
                        return Some((v.clone(), next_sel));
                    }
                    None
                })
                .collect(),
            Ipld::List(l) => l
                .iter()
                .enumerate()
                .filter_map(|(i, _)| {
                    let ps = PathSegment::from(i);
                    if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                        let v = match ipld.get(ps.ipld_index()) {
                            Ok(node) => node,
                            _ => return None,
                        };
                        return Some((v.clone(), next_sel));
                    }
                    None
                })
                .collect(),
            _ => Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::MemoryBlockstore;
    use crate::link_system::{LinkSystem, Prefix};
    use crate::selector::{RecursionLimit, Selector};
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;

    #[test]
    fn test_walk_next() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        let prefix = Prefix::new(DagCborCodec.into(), Code::Sha2_256.into());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let (leaf1_cid, leaf1_blk) = lsys.store_plus_raw(prefix, &leaf1).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let (leaf2_cid, leaf2_blk) = lsys.store_plus_raw(prefix, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_cid, leaf2_cid],
            "favouriteChild": leaf2_cid,
            "name": "parent",
        });
        let (root, parent_blk) = lsys.store_plus_raw(prefix, &parent).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let mut it = BlockIterator::new(lsys, root, selector);

        let (_, first) = it.next().unwrap().unwrap();
        assert_eq!(first, parent_blk);

        let (_, second) = it.next().unwrap().unwrap();
        assert_eq!(second, leaf1_blk);

        let (_, third) = it.next().unwrap().unwrap();
        assert_eq!(third, leaf2_blk);

        let (_, last) = it.next().unwrap().unwrap();
        assert_eq!(last, leaf2_blk);

        let end = it.next();
        assert_eq!(end, None);
    }

    #[test]
    fn test_walk_next_node() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        let prefix = Prefix::new(DagCborCodec.into(), Code::Sha2_256.into());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_cid = lsys.store(prefix, &leaf1).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_cid = lsys.store(prefix, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_cid, leaf2_cid],
            "favouriteChild": leaf2_cid,
            "name": "parent",
        });
        let root = lsys.store(prefix, &parent).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let mut it = IpldTraversal::new(lsys, root, selector);

        let first = it.next().unwrap().unwrap();
        assert_eq!(first, parent);

        let second = it.next().unwrap().unwrap();
        assert_eq!(second, leaf1);

        let third = it.next().unwrap().unwrap();
        assert_eq!(third, leaf2);

        let last = it.next().unwrap().unwrap();
        assert_eq!(last, leaf2);

        let end = it.next();
        assert_eq!(end, None);
    }
}
