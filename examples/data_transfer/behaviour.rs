use futures::prelude::*;
use graphsync::Request;
use ipld_traversal::Selector;
use libipld::{
    serde::{from_ipld, to_ipld},
    Cid,
};
use libp2p::core::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p::swarm::{
    dial_opts::DialOpts, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler,
    PollParameters,
};
use libp2p::{core::connection::ConnectionId, core::ConnectedPoint, Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_slice, to_vec};
use serde_repr::*;
use serde_tuple::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::{
    io, iter,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Debug, PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct ChannelId {
    pub initiator: String,
    pub responder: String,
    pub id: u64,
}

impl Default for ChannelId {
    fn default() -> Self {
        ChannelId {
            initiator: String::new(),
            responder: String::new(),
            id: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u64)]
pub enum MessageType {
    New = 0,
    Update,
    Cancel,
    Complete,
    Voucher,
    VoucherResult,
    Restart,
    RestartExistingChannelRequest,
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::New
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferRequest<V> {
    #[serde(rename = "BCid")]
    pub root: Cid,
    #[serde(rename = "Type")]
    pub mtype: MessageType,
    #[serde(rename = "Paus")]
    pub pause: bool,
    #[serde(rename = "Pull")]
    pub pull: bool,
    #[serde(rename = "Part")]
    pub partial: bool,
    #[serde(rename = "Stor")]
    pub selector: Option<Selector>,
    #[serde(rename = "Vouch")]
    pub voucher: Option<V>,
    #[serde(rename = "VTyp")]
    pub voucher_type: String,
    #[serde(rename = "XferID")]
    pub transfer_id: u64,
    #[serde(rename = "RestartChannel")]
    pub restart_channel: ChannelId,
}

pub fn pull_request(id: u64, root: Cid, selector: Selector) -> anyhow::Result<Request> {
    let mut treq = TransferRequest::default();
    treq.root = root;
    treq.pull = true;
    treq.selector = Some(selector.clone());
    treq.voucher = Some("dummy".into());
    treq.voucher_type = "BasicVoucher".into();
    treq.transfer_id = id;

    let tmsg: TransferMessage<BasicVoucher, BasicVoucher> = TransferMessage {
        is_request: true,
        request: Some(treq),
        response: None,
    };

    Request::builder()
        .root(root)
        .selector(selector)
        .extension("fil/data-transfer/1.1".into(), to_ipld(&tmsg)?)
        .build()
}

pub fn pull_response(request: &TransferRequest<BasicVoucher>) -> anyhow::Result<Request> {
    let tres = TransferResponse::accept(request.transfer_id);
    let tmsg: TransferMessage<BasicVoucher, BasicVoucher> = TransferMessage {
        is_request: false,
        request: None,
        response: Some(tres),
    };

    Request::builder()
        .root(request.root)
        .selector(
            request
                .selector
                .as_ref()
                .expect("to have a selector")
                .clone(),
        )
        .extension("fil/data-transfer/1.1".into(), to_ipld(&tmsg)?)
        .build()
}

impl<V> Default for TransferRequest<V> {
    fn default() -> Self {
        TransferRequest {
            root: Default::default(),
            mtype: MessageType::New,
            pause: false,
            partial: false,
            pull: false,
            selector: None,
            voucher: None,
            voucher_type: String::new(),
            transfer_id: 1,
            restart_channel: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferResponse<VR> {
    #[serde(rename = "Type")]
    pub mtype: MessageType,
    #[serde(rename = "Acpt")]
    pub accepted: bool,
    #[serde(rename = "Paus")]
    pub paused: bool,
    #[serde(rename = "XferID")]
    pub transfer_id: u64,
    #[serde(rename = "VRes")]
    pub voucher: Option<VR>,
    #[serde(rename = "VTyp")]
    pub voucher_type: String,
}

impl<VR> TransferResponse<VR> {
    pub fn is_complete(&self) -> bool {
        self.accepted && matches!(self.mtype, MessageType::Complete)
    }
    pub fn complete(id: u64) -> Self {
        TransferResponse {
            mtype: MessageType::Complete,
            accepted: true,
            paused: false,
            transfer_id: id,
            voucher: None,
            voucher_type: String::new(),
        }
    }
    pub fn accept(id: u64) -> Self {
        TransferResponse {
            mtype: MessageType::New,
            accepted: true,
            paused: false,
            transfer_id: id,
            voucher: None,
            voucher_type: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferMessage<V, VR> {
    #[serde(rename = "IsRq")]
    pub is_request: bool,
    #[serde(rename = "Request")]
    pub request: Option<TransferRequest<V>>,
    #[serde(rename = "Response")]
    pub response: Option<TransferResponse<VR>>,
}

impl TryFrom<&Request> for TransferMessage<BasicVoucher, BasicVoucher> {
    type Error = anyhow::Error;

    fn try_from(request: &Request) -> Result<Self, Self::Error> {
        request
            .get_extension("fil/data-transfer/1.1".into())
            .ok_or(anyhow::format_err!("no extension in graphsync request"))
            .and_then(|nd| {
                from_ipld(nd.clone())
                    .map_err(|e| anyhow::format_err!("failed to parse from ipld: {}", e))
            })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct BasicVoucher {
    data: String,
}

impl From<&str> for BasicVoucher {
    fn from(data: &str) -> Self {
        BasicVoucher {
            data: data.to_string(),
        }
    }
}

const DT_PROTOCOL: &[u8; 23] = b"/fil/datatransfer/1.2.0";

#[derive(Debug, Clone, Default)]
pub struct DataTransferProtocol<T>(T);
#[derive(Debug, Clone, Default)]
pub struct InboundMsg();
#[derive(Debug, Clone)]
pub struct OutboundMsg(TransferMessage<BasicVoucher, BasicVoucher>);

impl DataTransferProtocol<InboundMsg> {
    pub fn inbound() -> Self {
        DataTransferProtocol(InboundMsg())
    }
}

impl DataTransferProtocol<OutboundMsg> {
    pub fn outbound(msg: TransferMessage<BasicVoucher, BasicVoucher>) -> Self {
        DataTransferProtocol(OutboundMsg(msg))
    }
}

#[derive(Debug)]
pub enum TransferMsgEvent {
    Request(TransferRequest<BasicVoucher>),
    Response(TransferResponse<BasicVoucher>),
    Sent,
    InvalidMessage,
}

impl From<TransferMessage<BasicVoucher, BasicVoucher>> for TransferMsgEvent {
    fn from(msg: TransferMessage<BasicVoucher, BasicVoucher>) -> Self {
        if let Some(req) = msg.request {
            TransferMsgEvent::Request(req)
        } else if let Some(res) = msg.response {
            TransferMsgEvent::Response(res)
        } else {
            TransferMsgEvent::InvalidMessage
        }
    }
}

impl From<()> for TransferMsgEvent {
    fn from(_: ()) -> Self {
        TransferMsgEvent::Sent
    }
}

#[derive(Debug)]
pub enum TransferEvent {
    Message {
        peer: PeerId,
        message: TransferMsgEvent,
    },
    OutboundFailure {
        error: String,
    },
}

#[derive(Default)]
pub struct DataTransfer {
    addresses: HashMap<PeerId, Vec<Multiaddr>>,
    connected: HashSet<PeerId>,
    pending_requests: HashMap<PeerId, Vec<TransferRequest<BasicVoucher>>>,
    events: VecDeque<
        NetworkBehaviourAction<
            TransferEvent,
            OneShotHandler<
                DataTransferProtocol<InboundMsg>,
                DataTransferProtocol<OutboundMsg>,
                TransferMsgEvent,
            >,
        >,
    >,
}

impl DataTransfer {
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses.entry(*peer).or_default().push(address);
    }

    pub fn send_request(&mut self, peer: &PeerId, request: TransferRequest<BasicVoucher>) {
        if self.connected.contains(peer) {
            self.events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer,
                    handler: NotifyHandler::Any,
                    event: DataTransferProtocol::outbound(TransferMessage {
                        is_request: true,
                        request: Some(request),
                        response: None,
                    }),
                });
        } else {
            let handler = self.new_handler();
            self.events.push_back(NetworkBehaviourAction::Dial {
                opts: DialOpts::peer_id(*peer).build(),
                handler,
            });
            self.pending_requests
                .entry(*peer)
                .or_default()
                .push(request);
        }
    }

    pub fn send_response(&mut self, peer: &PeerId, response: TransferResponse<BasicVoucher>) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: DataTransferProtocol::outbound(TransferMessage {
                    is_request: false,
                    request: None,
                    response: Some(response),
                }),
            });
    }
}

impl NetworkBehaviour for DataTransfer {
    type ConnectionHandler = OneShotHandler<
        DataTransferProtocol<InboundMsg>,
        DataTransferProtocol<OutboundMsg>,
        TransferMsgEvent,
    >;
    type OutEvent = TransferEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        Default::default()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _conn: &ConnectionId,
        _endpoint: &ConnectedPoint,
        _failed_addresses: Option<&Vec<Multiaddr>>,
        _other_established: usize,
    ) {
        self.connected.insert(*peer_id);
        if let Some(pending) = self.pending_requests.remove(peer_id) {
            for request in pending {
                self.events
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: *peer_id,
                        handler: NotifyHandler::Any,
                        event: DataTransferProtocol::outbound(TransferMessage {
                            is_request: true,
                            request: Some(request),
                            response: None,
                        }),
                    });
            }
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _conn: &ConnectionId,
        _: &ConnectedPoint,
        _: Self::ConnectionHandler,
        _remaining_established: usize,
    ) {
        self.connected.remove(peer_id);
    }

    fn inject_event(&mut self, peer: PeerId, _connection: ConnectionId, event: TransferMsgEvent) {
        self.events.push_back(NetworkBehaviourAction::GenerateEvent(
            TransferEvent::Message {
                message: event,
                peer,
            },
        ));
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(more) = self.addresses.get(peer) {
            addresses.extend(more.into_iter().cloned());
        }
        addresses
    }
}

impl<T> UpgradeInfo for DataTransferProtocol<T> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(DT_PROTOCOL)
    }
}

impl<C> InboundUpgrade<C> for DataTransferProtocol<InboundMsg>
where
    C: AsyncRead + Unpin + Send + 'static,
{
    type Output = TransferMessage<BasicVoucher, BasicVoucher>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, mut socket: C, _: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await?;

            if buf.is_empty() {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }

            from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
        })
    }
}

impl<C> OutboundUpgrade<C> for DataTransferProtocol<OutboundMsg>
where
    C: AsyncWrite + Unpin + Send + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, mut socket: C, _: Self::Info) -> Self::Future {
        Box::pin(async move {
            let buf =
                to_vec(&self.0 .0).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            socket.write_all(&buf[..]).await?;
            socket.close().await?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex;

    #[derive(Serialize, Deserialize)]
    struct DealProposal {
        #[serde(rename = "PayloadCID")]
        cid: Cid,
        #[serde(rename = "ID")]
        id: u64,
    }

    #[test]
    fn decode_request() {
        let msg_data = hex::decode("a36449735271f56752657175657374aa6442436964d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486454797065006450617573f46450617274f46450756c6cf56453746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a065566f756368a36a5061796c6f6164434944d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486249440166506172616d73a66853656c6563746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0685069656365434944f66c5072696365506572427974654200646f5061796d656e74496e74657276616c192710775061796d656e74496e74657276616c496e6372656173651903e86b556e7365616c50726963654064565479707752657472696576616c4465616c50726f706f73616c2f3166586665724944016e526573746172744368616e6e656c8360600068526573706f6e7365f6").unwrap();

        let msg: TransferMessage<DealProposal, DealProposal> =
            from_slice(&msg_data).expect("message to decode");

        assert_eq!(msg.is_request, true);

        let request = msg.request.unwrap();

        let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
            .unwrap();
        // We can recover the CID
        assert_eq!(request.root, cid);
    }
}
