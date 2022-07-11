use super::messages::{
    Extensions, GraphSyncLinkData, GraphSyncMessage, GraphSyncResponse, LinkAction, RequestId,
    ResponseStatusCode,
};
use ipld_traversal::{link_system::BlockLoader, selector::Selector, BlockIterator, IterError};
use libipld::Cid;
use std::mem;

// ResponseBuilder is an iterator that yields response messages until the traversal comes
// to an end. It handles response status and block payloads.
pub struct ResponseBuilder<L> {
    pub paused: bool,
    pub cancelled: bool,
    pub rejected: bool,
    pub status: ResponseStatusCode,
    req_id: RequestId,
    it: BlockIterator<L>,
    max_size: usize,
    partial: bool,
    error: Option<IterError>,
    extensions: Extensions,
}

impl<L> ResponseBuilder<L>
where
    L: BlockLoader,
{
    pub fn new(id: RequestId, root: Cid, selector: Selector, loader: L) -> Self {
        Self {
            req_id: id,
            partial: false,
            max_size: 500 * 1024,
            extensions: Default::default(),
            paused: false,
            cancelled: false,
            rejected: false,
            error: None,
            // no response messages have been created yet
            status: ResponseStatusCode::RequestAcknowledged,
            // we tell the block iterator not to follow the same link twice as it was already sent.
            it: BlockIterator::new(loader, root, selector).ignore_duplicate_links(),
        }
    }
    /// set custom extensions to be sent with the first message of the iterator.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions.extend(extensions);
        self
    }
    /// pause the iterator and send the status in the next response.
    pub fn pause(mut self) -> Self {
        self.paused = true;
        self
    }
    /// cancel the iterator and send the status in the last response.
    pub fn cancel(mut self) -> Self {
        self.cancelled = true;
        self
    }
    /// cancel the iterator and send the rejected status in the last response.
    pub fn reject(mut self) -> Self {
        self.rejected = true;
        self
    }
    pub fn is_completed(&self) -> bool {
        match self.status {
            ResponseStatusCode::RequestCompletedFull
            | ResponseStatusCode::RequestCompletedPartial => true,
            _ => false,
        }
    }
}

impl<L> Iterator for ResponseBuilder<L>
where
    L: BlockLoader,
{
    type Item = GraphSyncMessage;

    fn next(&mut self) -> Option<Self::Item> {
        // if the status was set to final this iterator is done and will only return None.
        match self.status {
            ResponseStatusCode::RequestCompletedFull
            | ResponseStatusCode::RequestCompletedPartial
            | ResponseStatusCode::RequestFailedUnknown
            | ResponseStatusCode::RequestPaused
            | ResponseStatusCode::RequestCancelled
            | ResponseStatusCode::RequestFailedContentNotFound
            | ResponseStatusCode::RequestRejected => return None,
            _ => {}
        };

        if self.paused {
            self.status = ResponseStatusCode::RequestPaused;
        }
        if self.cancelled {
            self.status = ResponseStatusCode::RequestCancelled;
        }
        if self.rejected {
            self.status = ResponseStatusCode::RequestRejected;
        }
        if self.paused | self.cancelled | self.rejected {
            return Some(
                GraphSyncResponse {
                    id: self.req_id,
                    status: self.status,
                    metadata: None,
                    extensions: None,
                }
                .into(),
            );
        }

        let mut size = 0;
        let mut metadata = Vec::new();
        let mut blocks = Vec::new();
        // iterate until we've filled the message to capacity
        while size < self.max_size {
            match self.it.next() {
                Some(Ok((cid, data))) => {
                    size += data.len();
                    blocks.push((cid, data));
                    // at least one block was yielded so the response can be partial
                    self.status = ResponseStatusCode::PartialResponse;
                    metadata.push(GraphSyncLinkData {
                        link: cid,
                        action: LinkAction::Present,
                    });
                    println!("attaching block");
                }
                // we found a missing block but more might still be coming
                Some(Err(IterError::NotFound(cid))) => {
                    self.partial = true;
                    metadata.push(GraphSyncLinkData {
                        link: cid,
                        action: LinkAction::Missing,
                    });
                }
                Some(Err(e)) => {
                    self.status = ResponseStatusCode::RequestFailedUnknown;
                    self.error = Some(e);
                }
                None => {
                    self.status = match self.status {
                        // If we end the traversal and no content was ever returned, the request
                        // failed. TODO: we should be able to pass an error message somewhere in
                        // the extensions maybe?
                        ResponseStatusCode::RequestAcknowledged => {
                            if self.partial {
                                ResponseStatusCode::RequestFailedContentNotFound
                            } else {
                                ResponseStatusCode::RequestFailedUnknown
                            }
                        }
                        // in this case we've yielded some blocks
                        ResponseStatusCode::PartialResponse => {
                            if self.partial {
                                ResponseStatusCode::RequestCompletedPartial
                            } else {
                                ResponseStatusCode::RequestCompletedFull
                            }
                        }
                        // no change
                        status => status,
                    };
                    break;
                }
            }
        }

        let extensions = mem::take(&mut self.extensions);
        let mut response = GraphSyncResponse {
            id: self.req_id,
            status: self.status,
            metadata: None,
            extensions: None,
        };
        if !metadata.is_empty() {
            response.metadata = Some(metadata);
        }
        if !extensions.is_empty() {
            response.extensions = Some(extensions);
        }
        Some(GraphSyncMessage::res_with_blk(response, blocks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipld_traversal::{
        blockstore::MemoryBlockstore,
        link_system::{LinkSystem, Prefix},
        selector::RecursionLimit,
    };
    use libipld::{cbor::DagCborCodec, ipld, multihash::Code};
    use uuid::Uuid;

    #[test]
    fn response_builder() {
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

        let mut builder = ResponseBuilder::new(Uuid::new_v4(), root, selector, lsys);

        let GraphSyncMessage::V2 {
            responses, blocks, ..
        } = builder.next().unwrap();
        assert_eq!(responses.as_ref().unwrap().len(), 1);
        assert_eq!(blocks.as_ref().unwrap().len(), 3);
        assert_eq!(
            responses.as_ref().unwrap()[0].status,
            ResponseStatusCode::RequestCompletedFull
        );
        let end = builder.next();
        assert_eq!(end, None);
    }

    #[test]
    fn response_builder_missing_block() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        let prefix = Prefix::new(DagCborCodec.into(), Code::Sha2_256.into());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_cid = lsys.store(prefix, &leaf1).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_cid = lsys.compute_link(prefix, &leaf2).unwrap();

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

        let mut builder = ResponseBuilder::new(Uuid::new_v4(), root, selector, lsys);
        let GraphSyncMessage::V2 {
            responses, blocks, ..
        } = builder.next().unwrap();
        assert_eq!(responses.as_ref().unwrap().len(), 1);
        assert_eq!(blocks.as_ref().unwrap().len(), 2);
        assert_eq!(
            responses.as_ref().unwrap()[0].status,
            ResponseStatusCode::RequestCompletedPartial
        );

        responses.as_ref().unwrap()[0]
            .metadata
            .as_ref()
            .unwrap()
            .iter()
            .find(|item| item.action == LinkAction::Missing)
            .unwrap();
        let end = builder.next();
        assert_eq!(end, None);
    }
}
