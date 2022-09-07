/*!
This crate provides a library for constructing IPLD selectors and running traversals
for the GraphSync protocol implementation. It tries to mirror go-ipld-prime implementation
although it does not contain all the features.

# Blockstore
The trait copies one used by the Filecoin FVM implementation. It does not worry about
codecs contrary to the Libipld blockstore architecture which can be unfriendly to consumers
because of the generic parameter types requirements.

Instead, a LinkSystem akin to the ipld-prime implementation can wrap the blockstore to provide
codec support and a simplement interface for storing Ipld values directly.

```rust
use ipld_traversal::{LinkSystem, blockstore::MemoryBlockstore, Prefix};
use libipld::ipld;

let store = MemoryBlockstore::new();

let link_system = LinkSystem::new(store);

let sample_data = ipld!({
    "Size": 100,
});

let cid = link_system.store(Prefix::builder().dag_json().sha256().build(), &sample_data).unwrap();
```

# Selectors
Most commonly used selector features including reification.

```rust
use ipld_traversal::selector::{Selector, RecursionLimit};
use indexmap::indexmap;

let selector = Selector::ExploreRecursive {
    limit: RecursionLimit::None,
    sequence: Box::new(Selector::ExploreAll {
        next: Box::new(Selector::ExploreRecursiveEdge),
    }),
    current: None,
};

let reified = Selector::ExploreInterpretAs {
    reifier: "unixfs".to_string(),
    next: Box::new(Selector::ExploreFields {
        fields: indexmap! {
            "path".to_string() => Selector::Matcher,
        },
    }),
};
```

Or not to bother with constructing one from scratch one can simply use:
```rust
use ipld_traversal::unixfs::unixfs_path_selector;

let (cid, selector) = unixfs_path_selector("bafybeihq3wo4u27amukm36i7vbpirym4y2lvy53uappzhl3oehcm4ukphu/dir/file.ext".into()).unwrap();
```

# Traversal
To run an ipld traversal, an iterator interface is available.
Note that the iterator will traverse the exact tree as defined by the selector
but only return the IPLD node the links resolve to. This is different from
say the ipld-prime walkAdv callback which will return every intermediary ipld nodes.

```rust
use ipld_traversal::{Selector, LinkSystem, blockstore::MemoryBlockstore, IpldTraversal, Prefix};
use libipld::ipld;

let store = MemoryBlockstore::new();
let lsys = LinkSystem::new(store);

let prefix = Prefix::builder().dag_cbor().sha256().build();
let leaf = ipld!({ "name": "leaf1", "size": 12 });
let root = lsys.store(prefix, &leaf).unwrap();

let selector = Selector::ExploreAll {
    next: Box::new(Selector::ExploreRecursiveEdge),
};

let mut it = IpldTraversal::new(lsys, root, selector);

let node = it.next().unwrap().unwrap();
assert_eq!(node, leaf);
```

A block traversal walks the IPLD tree as defined by a given selector while
returning all the blocks resolved along the way.

```rust
use ipld_traversal::{Selector, LinkSystem, blockstore::MemoryBlockstore, BlockTraversal, Prefix};
use libipld::ipld;

let store = MemoryBlockstore::new();
let lsys = LinkSystem::new(store);

let prefix = Prefix::builder().dag_cbor().sha256().build();
let leaf = ipld!({ "name": "ipld node", "size": 10 });
let (root, bytes) = lsys.store_plus_raw(prefix, &leaf).unwrap();

let selector = Selector::ExploreAll {
    next: Box::new(Selector::ExploreRecursiveEdge),
};

let mut it = BlockTraversal::new(lsys, root, selector);

let (cid, block) = it.next().unwrap().unwrap();
assert_eq!(block, bytes);
```
*/

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

pub use link_system::{BlockLoader, IpldLoader, LinkSystem, LoaderError, Prefix};
pub use selector::Selector;

#[derive(Error, Debug, PartialEq, Eq)]
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

impl From<LoaderError> for IterError {
    fn from(err: LoaderError) -> Self {
        match err {
            LoaderError::NotFound(cid) => Self::NotFound(cid),
            LoaderError::SkipMe(cid) => Self::SkipMe(cid),
            LoaderError::Decoding(s) => Self::Decode(s),
            LoaderError::Codec(_) => Self::Other("invalid multicodec"),
            LoaderError::Blockstore(_) => Self::Other("blockstore error"),
        }
    }
}

/// Executes an iterative traversal based on the given root CID and selector
/// and returns the blocks resolved along the way.
#[derive(Debug)]
pub struct BlockTraversal<L> {
    loader: L,
    selector: Selector,
    start: Option<Cid>,
    stack_list: SmallVec<[vec::IntoIter<(Ipld, Selector)>; 8]>,
    seen: Option<HashSet<Cid>>,
    restart: bool,
}

impl<L> Iterator for BlockTraversal<L>
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

impl<L> BlockTraversal<L>
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
                Err(e) => {
                    return match e.downcast::<LoaderError>() {
                        Ok(e) => Some(Err(e.into())),
                        Err(_) => Some(Err(IterError::NotFound(c))),
                    };
                }
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
    use crate::blockstore::{Blockstore, MemoryBlockstore};
    use crate::link_system::{LinkSystem, Prefix};
    use crate::selector::{RecursionLimit, Selector};
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use rand::prelude::*;

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
        let mut it = BlockTraversal::new(lsys, root, selector);

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
    fn test_missing_links() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store.clone());

        const CHUNK_SIZE: usize = 250 * 1024;

        let mut bytes = vec![0u8; 3 * CHUNK_SIZE];
        thread_rng().fill(&mut bytes[..]);

        let chunks = bytes.chunks(CHUNK_SIZE);

        let links: Vec<Ipld> = chunks
            .map(|chunk| {
                let leaf = Ipld::Bytes(chunk.to_vec());
                let cid = lsys
                    .store(Prefix::new(0x55, 0x13), &leaf)
                    .expect("link system should store leaf node");
                let link = ipld!({
                    "Hash": cid,
                    "Tsize": CHUNK_SIZE,
                });
                link
            })
            .collect();

        let mut missing = Vec::new();
        // delete the last 2 blocks
        links.iter().skip(1).for_each(|link| {
            let _ = link.get("Hash").map(|l| {
                if let Ipld::Link(cid) = l {
                    store.delete_block(cid).unwrap();
                    missing.push(*cid);
                }
            });
        });

        let root_node = ipld!({
            "Links": links,
        });

        let root = lsys
            .store(Prefix::new(0x71, 0x13), &root_node)
            .expect("link system to store root node");

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let mut it = IpldTraversal::new(lsys, root, selector);

        let first = it.next().unwrap().unwrap();
        assert_eq!(first, root_node);

        // first chunk resolves as expected
        let _ = it.next().unwrap().unwrap();

        let result = it.next().unwrap();
        assert_eq!(result, Err(IterError::NotFound(missing[0])));

        let result = it.next().unwrap();
        assert_eq!(result, Err(IterError::NotFound(missing[1])));

        let done = it.next();
        assert_eq!(done, None);
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
