use crate::selector::{RecursionLimit, Selector};
use indexmap::indexmap;
use libipld::pb::PbNode;
use libipld::{Cid, Ipld};
use std::borrow::Cow;
use std::collections::BTreeMap;
use unixfs_v1::{UnixFs, UnixFsType};

pub fn resolve_unixfs(node: &Ipld) -> Option<Ipld> {
    let pb_node = PbNode::try_from(node).ok()?;
    let unixfs = UnixFs::try_from(Some(&pb_node.data[..])).ok()?;
    match unixfs.Type {
        // we only care about directories for now
        UnixFsType::Directory => {
            let mut map = BTreeMap::new();
            for link in pb_node.links {
                map.insert(link.name.clone(), link.into());
            }
            Some(Ipld::Map(map))
        }
        UnixFsType::File => {
            if pb_node.links.is_empty() {
                let data = match unixfs.Data {
                    Some(Cow::Borrowed(x)) => x,
                    None => &[][..],
                    _ => panic!("should not be Cow::Owned"),
                };
                // need to copy the bytes here unfortunately
                return Some(Ipld::Bytes(data.to_vec()));
            }
            None
        }
        _ => None,
    }
}

pub fn unixfs_path_selector(path: String) -> Option<(Cid, Selector)> {
    let segments: Vec<&str> = path.split('/').collect();
    // defaults to a full traversal
    let mut selector = Selector::ExploreRecursive {
        limit: RecursionLimit::None,
        sequence: Box::new(Selector::ExploreAll {
            next: Box::new(Selector::ExploreRecursiveEdge),
        }),
        current: None,
    };
    let root = Cid::try_from(segments[0]).ok()?;
    if segments.len() == 1 {
        return Some((root, selector));
    }
    // ignore the first one which was the root CID
    for seg in segments[1..].iter().rev() {
        selector = Selector::ExploreInterpretAs {
            reifier: "unixfs".to_string(),
            next: Box::new(Selector::ExploreFields {
                fields: indexmap! {
                    seg.to_string() => selector,
                },
            }),
        };
    }
    Some((root, selector))
}
