use futures::io::{AsyncRead, AsyncWrite};
use ipld_traversal::{link_system::Prefix, selector::Selector};
use libipld::{Cid, Ipld};
use libp2p::core::upgrade;
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_slice, to_vec};
use serde_repr::*;
use serde_tuple::*;
use std::collections::BTreeMap;
use std::io;
use uuid::Uuid;

pub type RequestId = Uuid;

pub type Priority = i32;

pub type Extensions = BTreeMap<String, Ipld>;

#[derive(PartialEq, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RequestType {
    #[serde(rename = "n")]
    New,
    #[serde(rename = "c")]
    Cancel,
    #[serde(rename = "u")]
    Update,
}

// The order of fields on this struct match the ordering of IPLD prime Map node key ordering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSyncRequest {
    pub id: RequestId,

    #[serde(rename = "sel", skip_serializing_if = "Option::is_none")]
    pub selector: Option<Selector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<Cid>,

    #[serde(rename = "pri", skip_serializing_if = "Option::is_none")]
    pub priority: Option<Priority>,

    #[serde(rename = "type")]
    pub request_type: RequestType,

    #[serde(rename = "ext", skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Extensions>,
}

#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct GraphSyncBlock {
    #[serde(with = "serde_bytes")]
    pub prefix: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct GraphSyncLinkData {
    pub link: Cid,
    pub action: LinkAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LinkAction {
    #[serde(rename = "p")]
    Present,
    #[serde(rename = "d")]
    DuplicateNotSent,
    #[serde(rename = "m")]
    Missing,
    #[serde(rename = "s")]
    DuplicateDAGSkipped,
}

#[derive(PartialEq, Clone, Copy, Eq, Debug, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ResponseStatusCode {
    RequestAcknowledged = 10,
    PartialResponse = 14,
    RequestPaused = 15,
    RequestCompletedFull = 20,
    RequestCompletedPartial = 21,
    RequestRejected = 30,
    RequestFailedBusy = 31,
    RequestFailedUnknown = 32,
    RequestFailedLegal = 33,
    RequestFailedContentNotFound = 34,
    RequestCancelled = 35,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSyncResponse {
    #[serde(rename = "ext", skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Extensions>,

    #[serde(rename = "meta", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<GraphSyncLinkData>>,

    #[serde(rename = "stat")]
    pub status: ResponseStatusCode,

    #[serde(rename = "reqid")]
    pub id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphSyncMessage {
    // V2 is the default, other message formats could be easily added for future upgrades
    #[serde(rename = "gs2")]
    V2 {
        #[serde(rename = "req", skip_serializing_if = "Option::is_none")]
        requests: Option<Vec<GraphSyncRequest>>,
        #[serde(rename = "rsp", skip_serializing_if = "Option::is_none")]
        responses: Option<Vec<GraphSyncResponse>>,
        #[serde(rename = "blk", skip_serializing_if = "Option::is_none")]
        blocks: Option<Vec<GraphSyncBlock>>,
    },
}

// util for deriving a message for a single response without blocks.
impl From<GraphSyncResponse> for GraphSyncMessage {
    fn from(res: GraphSyncResponse) -> Self {
        Self::V2 {
            requests: None,
            responses: Some(vec![res]),
            blocks: None,
        }
    }
}

// util for deriving a message for a single request.
impl From<GraphSyncRequest> for GraphSyncMessage {
    fn from(req: GraphSyncRequest) -> Self {
        Self::V2 {
            requests: Some(vec![req]),
            responses: None,
            blocks: None,
        }
    }
}

// read and write length prefixed messages from a network stream.
impl GraphSyncMessage {
    pub async fn from_net<T>(stream: &mut T) -> io::Result<Self>
    where
        T: AsyncRead + Send + Unpin,
    {
        let buf = upgrade::read_length_prefixed(stream, 1024 * 1024).await?;
        if buf.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let msg: GraphSyncMessage =
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
impl GraphSyncMessage {
    pub fn res_with_blk<D, I>(res: GraphSyncResponse, blocks: I) -> Self
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
                    .map(|(k, v)| GraphSyncBlock {
                        prefix: Prefix::from(k).to_bytes(),
                        data: v.as_ref().to_vec(),
                    })
                    .collect(),
            ),
        }
    }
}

// getters
impl GraphSyncMessage {
    pub fn get_reqs(&self) -> Option<&Vec<GraphSyncRequest>> {
        let Self::V2 { requests, .. } = self;
        requests.as_ref()
    }
    pub fn get_res(&self) -> Option<&Vec<GraphSyncResponse>> {
        let Self::V2 { responses, .. } = self;
        responses.as_ref()
    }
    pub fn get_blocks(&self) -> Option<&Vec<GraphSyncBlock>> {
        let Self::V2 { blocks, .. } = self;
        blocks.as_ref()
    }
    pub fn into_inner(
        self,
    ) -> (
        Option<Vec<GraphSyncRequest>>,
        Option<Vec<GraphSyncResponse>>,
        Option<Vec<GraphSyncBlock>>,
    ) {
        let Self::V2 {
            requests,
            responses,
            blocks,
        } = self;
        (requests, responses, blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;

    #[async_std::test]
    async fn request_compat() {
        let msg_data = hex::decode("73a163677332a16372657181a462696450326a3f8c261e47ca8b4ea121ba8085676373656ca16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a064726f6f74d82a582500017012209f35d67c4183fdfa4401b5fdc185cf26a0ec18dcf929e3cdb6cdcea09c853a786474797065616e").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = GraphSyncMessage::from_net(&mut buf).await.unwrap();

        let mut buf = Vec::new();
        msg.to_net(&mut buf).await.unwrap();
        assert_eq!(buf, input);
    }

    #[async_std::test]
    async fn response_compat() {
        let msg_data = hex::decode("ff02a163677332a16372737082a3646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e616d82d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a6164647374617418226572657169645023f795f246c445588a2a4cb970a525fda463657874a16f486970706974792b486f70706974795864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e35646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e617382d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a617064737461740e657265716964508a63ea3fd29141f48ce56ef043a17c23").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = GraphSyncMessage::from_net(&mut buf).await.unwrap();

        let mut buf = Vec::new();
        msg.to_net(&mut buf).await.unwrap();
        assert_eq!(buf, input);
    }

    #[async_std::test]
    async fn blocks_compat() {
        let msg_data = hex::decode("e301a163677332a163626c6b828244015512205864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3582440155122058644204cb9a1e34c5f08e9b20aa76090e70020bb56c0ca3d3af7296cd1058a5112890fed218488f084d8df9e4835fb54ad045ffd936e3bf7261b0426c51352a097816ed74482bb9084b4a7ed8adc517f3371e0e0434b511625cd1a41792243dccdcfe88094b").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let msg = GraphSyncMessage::from_net(&mut buf).await.unwrap();

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

        let res = GraphSyncResponse {
            id: Uuid::new_v4(),
            status: ResponseStatusCode::PartialResponse,
            metadata: None,
            extensions: None,
        };
        let msg = GraphSyncMessage::res_with_blk(res, blocks);
        assert_eq!(msg.get_blocks().unwrap().len(), 2);
        assert_eq!(msg.get_res().unwrap().len(), 1);
    }
}
