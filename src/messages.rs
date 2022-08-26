use crate::request::{Extensions, Request, RequestId, RequestType};
use crate::response::{Block, LinkData, Response, StatusCode};
use deque;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use futures::stream::{self, unfold, Stream, StreamExt};
use ipld_traversal::{link_system::Prefix, IterError, Selector};
use libipld::{Cid, Ipld};
use libp2p::{core::upgrade, Multiaddr};
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_slice, to_vec};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{fmt, io, mem};
use thiserror::Error;

const DEFAULT_MSG_SIZE: usize = 500 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    // V2 is the default, other message formats could be easily added for future upgrades
    #[serde(rename = "gs2")]
    V2 {
        #[serde(rename = "req", skip_serializing_if = "Option::is_none")]
        requests: Option<Vec<Request>>,
        #[serde(rename = "rsp", skip_serializing_if = "Option::is_none")]
        responses: Option<Vec<Response>>,
        #[serde(rename = "blk", skip_serializing_if = "Option::is_none")]
        blocks: Option<Vec<Block>>,
    },
}

// util for deriving a message for a single response without blocks.
impl From<Response> for Message {
    fn from(res: Response) -> Self {
        Self::V2 {
            requests: None,
            responses: Some(vec![res]),
            blocks: None,
        }
    }
}

// util for deriving a message for a single request.
impl From<Request> for Message {
    fn from(req: Request) -> Self {
        Self::V2 {
            requests: Some(vec![req]),
            responses: None,
            blocks: None,
        }
    }
}

// read and write length prefixed messages from a network stream.
impl Message {
    pub async fn from_net<T>(stream: &mut T) -> io::Result<Self>
    where
        T: AsyncRead + Send + Unpin,
    {
        let buf = upgrade::read_length_prefixed(stream, 1024 * 1024).await?;
        if buf.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let msg: Message =
            from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(msg)
    }
    pub async fn to_net<T>(self, stream: &mut T) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let buf = to_vec(&self).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        upgrade::write_length_prefixed(stream, buf).await
    }
}

// build messages with blocks
impl Message {
    pub fn res_with_blk<D, I>(res: Response, blocks: I) -> Self
    where
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        Self::V2 {
            requests: None,
            responses: Some(vec![res]),
            blocks: Some(
                blocks
                    .into_iter()
                    .map(|(k, v)| Block {
                        prefix: Prefix::from(k).to_bytes(),
                        data: v.as_ref().to_vec(),
                    })
                    .collect(),
            ),
        }
    }
}

// getters
impl Message {
    #[inline]
    pub fn builder() -> Builder {
        Builder::new()
    }
    pub fn get_reqs(&self) -> Option<&Vec<Request>> {
        let Self::V2 { requests, .. } = self;
        requests.as_ref()
    }
    pub fn as_reqs(self) -> Option<Vec<Request>> {
        let Self::V2 { requests, .. } = self;
        requests
    }
    pub fn get_res(&self) -> Option<&Vec<Response>> {
        let Self::V2 { responses, .. } = self;
        responses.as_ref()
    }
    pub fn get_blocks(&self) -> Option<&Vec<Block>> {
        let Self::V2 { blocks, .. } = self;
        blocks.as_ref()
    }
    pub fn into_inner(
        self,
    ) -> (
        Option<Vec<Request>>,
        Option<Vec<Response>>,
        Option<Vec<Block>>,
    ) {
        let Self::V2 {
            requests,
            responses,
            blocks,
        } = self;
        (requests, responses, blocks)
    }
}

pub struct ExtensionData {
    name: String,
    data: Ipld,
}

#[derive(Default)]
pub struct Builder {
    blk_size: usize,
    blocks: Vec<Block>,
    completed: HashMap<RequestId, StatusCode>,
    outgoing: HashMap<RequestId, Vec<LinkData>>,
    extensions: HashMap<RequestId, Vec<ExtensionData>>,
    requests: HashMap<RequestId, Request>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder::default()
    }

    pub fn insert_request(&mut self, req: Request) -> &mut Builder {
        self.requests.insert(*req.id(), req);
        self
    }

    pub fn insert_block(&mut self, blk: (Cid, Vec<u8>)) -> &mut Builder {
        let (cid, data) = blk;
        self.blk_size += data.len();
        self.blocks.push(Block {
            prefix: Prefix::from(cid).to_bytes(),
            data,
        });
        self
    }

    pub fn insert_extension(&mut self, req_id: RequestId, ext: ExtensionData) -> &mut Builder {
        self.extensions
            .entry(req_id)
            .or_insert(Vec::new())
            .push(ext);
        self
    }

    pub fn insert_link(&mut self, req_id: RequestId, data: LinkData) -> &mut Builder {
        self.outgoing.entry(req_id).or_insert(Vec::new()).push(data);
        self
    }

    pub fn complete(&mut self, req_id: RequestId, status: StatusCode) -> &mut Builder {
        self.completed.insert(req_id, status);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.outgoing.is_empty() && self.blocks.is_empty()
    }

    pub fn build(&mut self) -> Message {
        let mut requests: Option<Vec<Request>> = None;
        if !self.requests.is_empty() {
            requests = Some(mem::take(&mut self.requests).into_values().collect());
        }
        let mut responses: Option<Vec<Response>> = None;
        if !self.outgoing.is_empty() {
            responses = Some(Vec::new());
            for (id, links) in self.outgoing.drain() {
                let mut resp =
                    Response::builder(id, StatusCode::PartialResponse).set_metadata(links);
                if let Some(status) = self.completed.get(&id) {
                    resp = resp.status(*status);
                }
                if let Some(exts) = self.extensions.get_mut(&id) {
                    while let Some(ext) = exts.pop() {
                        resp = resp.extension(ext.name, ext.data);
                    }
                }
                responses = responses.and_then(|mut resps| {
                    resps.push(resp.build().unwrap());
                    Some(resps)
                });
            }
        }
        let mut blocks: Option<Vec<Block>> = None;
        if !self.blocks.is_empty() {
            blocks = Some(mem::take(&mut self.blocks));
        }
        Message::V2 {
            requests,
            responses,
            blocks,
        }
    }
}

pub struct MsgWriter<W> {
    inner: W,
    builder: Builder,
    capacity: usize,
}

impl<W> fmt::Debug for MsgWriter<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MsgWriter").finish()
    }
}

impl<W> MsgWriter<W>
where
    W: AsyncWrite + Send + Unpin,
{
    pub fn new(inner: W) -> MsgWriter<W> {
        MsgWriter::with_capacity(DEFAULT_MSG_SIZE, inner)
    }

    pub fn with_capacity(capacity: usize, inner: W) -> MsgWriter<W> {
        MsgWriter {
            inner,
            capacity,
            builder: Builder::new(),
        }
    }

    pub async fn write(
        mut self,
        req_id: RequestId,
        mut blocks: impl Iterator<Item = Result<(Cid, Vec<u8>), IterError>>,
    ) -> io::Result<()> {
        let mut incomplete = false;
        loop {
            match blocks.next() {
                Some(Ok((cid, data))) => {
                    if data.len() > self.spare_capacity() && !self.builder.is_empty() {
                        self.send_msg().await?;
                    }
                    self.builder.insert_block((cid, data));
                    self.builder.insert_link(req_id, LinkData::present(cid));
                }
                Some(Err(IterError::NotFound(cid))) => {
                    self.builder.insert_link(req_id, LinkData::missing(cid));
                    incomplete = true;
                }
                Some(Err(e)) => {
                    let status = StatusCode::RequestFailedUnknown;
                    self.builder.complete(req_id, status);
                    self.send_msg().await?;
                    return Err(io::Error::new(io::ErrorKind::Other, e));
                }
                _ => break,
            };
        }
        let status = if incomplete {
            StatusCode::RequestCompletedPartial
        } else {
            StatusCode::RequestCompletedFull
        };
        self.builder.complete(req_id, status);
        self.send_msg().await?;
        self.inner.close().await?;
        Ok(())
    }

    pub async fn send_request(mut self, request: Request) -> io::Result<()> {
        self.builder.insert_request(request);
        self.send_msg().await?;
        self.inner.close().await?;
        Ok(())
    }

    pub async fn close(mut self) -> io::Result<()> {
        self.inner.close().await
    }

    async fn send_msg(&mut self) -> io::Result<()> {
        let msg = self.builder.build();
        msg.to_net(&mut self.inner).await
    }

    #[inline]
    fn spare_capacity(&self) -> usize {
        if self.builder.blk_size > self.capacity {
            0
        } else {
            self.capacity - self.builder.blk_size
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundEvent {
    NewRequest {
        root: Cid,
        selector: Selector,
        extensions: Option<Extensions>,
    },
    Partial(Cid, Vec<u8>),
    Missing(Cid),
    NotFound,
    Rejected,
    Completed,
}

// filter out events to bubble up to the client notifying why a request is finished.
fn final_event(status: StatusCode) -> Option<InboundEvent> {
    match status {
        StatusCode::RequestCompletedFull | StatusCode::RequestCompletedPartial => {
            Some(InboundEvent::Completed)
        }
        StatusCode::RequestFailedContentNotFound => Some(InboundEvent::NotFound),
        StatusCode::RequestRejected => Some(InboundEvent::Rejected),
        _ => None,
    }
}

pub struct MsgReader<R> {
    inner: R,
}

impl<R> MsgReader<R>
where
    R: AsyncRead + Send + Unpin,
{
    pub fn new(inner: R) -> MsgReader<R> {
        MsgReader { inner }
    }

    pub fn events(self) -> impl Stream<Item = (RequestId, InboundEvent)> {
        unfold(self.inner, |mut r| async move {
            if let Ok(msg) = Message::from_net(&mut r).await {
                match msg.into_inner() {
                    // we could receive a response with no blocks in the case where the
                    // provider is missing some.
                    (_, Some(responses), None) => {
                        let mut item = Vec::new();
                        for res in responses {
                            if let Some(meta) = res.metadata {
                                for data in meta {
                                    item.push((res.id, InboundEvent::Missing(data.link)));
                                }
                            }
                            if let Some(event) = final_event(res.status) {
                                item.push((res.id, event));
                            }
                        }
                        return Some((item, r));
                    }
                    (_, Some(responses), Some(blocks)) => {
                        let mut blk_map: HashMap<Cid, Vec<u8>> = HashMap::new();
                        let mut item = Vec::new();
                        for blk in blocks {
                            let prefix = match Prefix::new_from_bytes(&blk.prefix) {
                                Ok(prefix) => prefix,
                                Err(_e) => {
                                    continue;
                                }
                            };
                            let cid = match prefix.to_cid(&blk.data) {
                                Ok(cid) => cid,
                                Err(_e) => {
                                    continue;
                                }
                            };
                            blk_map.insert(cid, blk.data);
                        }
                        for res in responses {
                            if let Some(meta) = res.metadata {
                                for data in meta {
                                    let event = match blk_map.remove(&data.link) {
                                        Some(blk) => InboundEvent::Partial(data.link, blk),
                                        None => InboundEvent::Missing(data.link),
                                    };
                                    item.push((res.id, event));
                                }
                            }
                            if let Some(event) = final_event(res.status) {
                                item.push((res.id, event));
                            }
                        }
                        // if we still have blocks in there, the provider is faulty.
                        debug_assert!(blk_map.is_empty());

                        return Some((item, r));
                    }
                    // For now we only support requests sent in individual messages as
                    // there is not a strong case for sending requests alongside responses/blocks from
                    // other requests with the same peer.
                    (Some(requests), _, _) => {
                        let mut item = Vec::new();
                        for req in requests {
                            if let Some((root, selector)) = req.root().zip(req.selector()) {
                                item.push((
                                    *req.id(),
                                    InboundEvent::NewRequest {
                                        root: *root,
                                        selector: selector.clone(),
                                        extensions: req.extensions().cloned(),
                                    },
                                ));
                            }
                        }
                        return Some((item, r));
                    }
                    _ => (),
                };
            }
            None
        })
        .flat_map(stream::iter)
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum ReaderError {
    #[error("block not found for {0}")]
    BlockNotFound(Cid),
    #[error("content not found")]
    NotFound,
    #[error("request rejected")]
    Rejected,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StatusCode;
    use futures::io::Cursor;
    use uuid::Uuid;

    #[async_std::test]
    async fn request_compat() {
        let msg_data = hex::decode("73a163677332a16372657181a462696450326a3f8c261e47ca8b4ea121ba8085676373656ca16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a064726f6f74d82a582500017012209f35d67c4183fdfa4401b5fdc185cf26a0ec18dcf929e3cdb6cdcea09c853a786474797065616e").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = Message::from_net(&mut buf).await.unwrap();

        let mut buf = Vec::new();
        msg.to_net(&mut buf).await.unwrap();
        assert_eq!(buf, input);
    }

    #[async_std::test]
    async fn response_compat() {
        let msg_data = hex::decode("ff02a163677332a16372737082a3646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e616d82d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a6164647374617418226572657169645023f795f246c445588a2a4cb970a525fda463657874a16f486970706974792b486f70706974795864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e35646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e617382d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a617064737461740e657265716964508a63ea3fd29141f48ce56ef043a17c23").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = Message::from_net(&mut buf).await.unwrap();

        let mut buf = Vec::new();
        msg.to_net(&mut buf).await.unwrap();
        assert_eq!(buf, input);
    }

    #[async_std::test]
    async fn blocks_compat() {
        let msg_data = hex::decode("e301a163677332a163626c6b828244015512205864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3582440155122058644204cb9a1e34c5f08e9b20aa76090e70020bb56c0ca3d3af7296cd1058a5112890fed218488f084d8df9e4835fb54ad045ffd936e3bf7261b0426c51352a097816ed74482bb9084b4a7ed8adc517f3371e0e0434b511625cd1a41792243dccdcfe88094b").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = Message::from_net(&mut buf).await.unwrap();

        let mut buf = Vec::new();
        msg.to_net(&mut buf).await.unwrap();
        assert_eq!(buf, input);
    }

    #[test]
    fn build_msg_with_blocks() {
        use ipld_traversal::{
            blockstore::MemoryBlockstore,
            link_system::{LinkSystem, Prefix},
        };
        use libipld::ipld;

        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        let leaf = ipld!({
            "value": "world",
        });
        let (leaf_cid, leaf_blk) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &leaf)
            .expect("link system should store the ipld node and return a CID");

        let parent = ipld!({
            "hello": leaf_cid,
        });
        let (root, parent_blk) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &parent)
            .expect("link system should store the parent and return a CID");

        let blocks = vec![(leaf_cid, leaf_blk), (root, parent_blk)];

        let res = Response {
            id: Uuid::new_v4(),
            status: StatusCode::PartialResponse,
            metadata: None,
            extensions: None,
        };
        let msg = Message::res_with_blk(res, blocks);
        assert_eq!(msg.get_blocks().unwrap().len(), 2);
        assert_eq!(msg.get_res().unwrap().len(), 1);
    }

    #[async_std::test]
    async fn msg_stream() {
        use async_std::net::{Shutdown, TcpListener, TcpStream};
        use futures::prelude::*;
        use ipld_traversal::{
            blockstore::MemoryBlockstore,
            link_system::{LinkSystem, Prefix},
            selector::RecursionLimit,
            BlockIterator,
        };
        use libipld::{ipld, Ipld};
        use rand::prelude::*;
        use std::sync::Arc;

        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        const CHUNK_SIZE: usize = 250 * 1024;

        let mut bytes = vec![0u8; 3 * CHUNK_SIZE];
        thread_rng().fill(&mut bytes[..]);

        let mut chunks = bytes.chunks(CHUNK_SIZE);

        let chunk1 = chunks.next().unwrap();
        let chunk2 = chunks.next().unwrap();
        let chunk3 = chunks.next().unwrap();

        let leaf1 = ipld!({
            "Data": Ipld::Bytes(chunk1.to_vec()),
        });
        let (leaf1_cid, _) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &leaf1)
            .expect("link system should store leaf node 1");
        let link1 = ipld!({
            "Hash": leaf1_cid,
            "Tsize": CHUNK_SIZE,
        });
        let leaf2 = ipld!({
            "Data": Ipld::Bytes(chunk2.to_vec()),
        });
        let (leaf2_cid, _) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &leaf2)
            .expect("link system should store leaf node 2");
        let link2 = ipld!({
            "Hash": leaf2_cid,
            "Tsize": CHUNK_SIZE,
        });
        let leaf3 = ipld!({
            "Data": Ipld::Bytes(chunk3.to_vec()),
        });
        let (leaf3_cid, _) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &leaf3)
            .expect("link system should store leaf node 3");
        let link3 = ipld!({
            "Hash": leaf3_cid,
            "Tsize": CHUNK_SIZE,
        });
        let rootn = ipld!({
            "Links": vec![link1, link2, link3],
        });
        let (root_cid, _) = lsys
            .store_plus_raw(Prefix::new(0x71, 0x13), &rootn)
            .expect("link system should store root node");

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = async move {
            let mut incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            let msg = Message::from_net(&mut incoming).await.unwrap();
            let req = msg.get_reqs().unwrap().iter().next().unwrap();
            let (root, selector) = req
                .root()
                .zip(req.selector())
                .expect("request should have a root and selector");
            let it = BlockIterator::new(lsys, *root, selector.clone());
            let mut writer = MsgWriter::new(&mut incoming);
            writer
                .write(*req.id(), it)
                .await
                .expect("writer should write all messages");
        };

        let client = async move {
            let req = Request::builder()
                .root(root_cid)
                .selector(selector)
                .build()
                .expect("request should build correctly");
            let msg = Message::builder().insert_request(req).build();
            let mut stream = TcpStream::connect(&listener_addr).await.unwrap();
            msg.to_net(&mut stream).await.unwrap();
            let reader = MsgReader::new(stream);
            let mut events = Box::pin(reader.events());
            while let Some((req_id, evt)) = futures::StreamExt::next(&mut events).await {
                match evt {
                    InboundEvent::Partial(cid, block) => {
                        println!("partial");
                    }
                    InboundEvent::Completed => {
                        println!("completed");
                    }
                    _ => {
                        println!("event {:?}", evt);
                    }
                };
            }
        };

        futures::join!(server, client);
    }
}
