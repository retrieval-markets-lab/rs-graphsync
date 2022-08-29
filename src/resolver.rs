use crate::network::{Behaviour, GraphSyncEvent, GraphSyncHandler};
use crate::request::RequestId;
use futures::prelude::*;
use libipld::Ipld;
use libp2p::swarm::SwarmEvent;
use std::io;

pub async fn resolve_raw_bytes(
    req_id: RequestId,
    from: impl Stream<Item = SwarmEvent<GraphSyncEvent, io::Error>>,
) -> Vec<u8> {
    from.take_while(|event| {
        if let SwarmEvent::Behaviour(GraphSyncEvent::Completed { id }) = event {
            if *id == req_id {
                return future::ready(false);
            }
        }
        future::ready(true)
    })
    .filter_map(|event| async {
        if let SwarmEvent::Behaviour(GraphSyncEvent::Block { id, data }) = event {
            if id != req_id {
                return None;
            }
            if let Ipld::Bytes(bytes) = data {
                return Some(bytes);
            }
        }
        None
    })
    .concat()
    .await
}
