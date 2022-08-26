use crate::messages::{Message, MsgWriter};
use crate::request::{Request, RequestId};
use crate::response::StatusCode;
use futures::channel::{mpsc, oneshot};
use futures::sink::SinkExt;
use futures::{future::BoxFuture, prelude::*, stream::unfold, stream::BoxStream};
use ipld_traversal::{BlockIterator, BlockLoader, Prefix, Selector};
use libipld::Cid;
use libp2p::core::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p::swarm::{
    dial_opts::DialOpts, ConnectionHandler, ConnectionHandlerEvent, ConnectionHandlerUpgrErr,
    DialError, IntoConnectionHandler, KeepAlive, NegotiatedSubstream, NetworkBehaviour,
    NetworkBehaviourAction, NotifyHandler, PollParameters, SubstreamProtocol,
};
use libp2p::{
    core::connection::ConnectionId, core::transport::ListenerId, core::ConnectedPoint, Multiaddr,
    PeerId,
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    io, iter,
    pin::Pin,
    task::Context,
    task::Poll,
};

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Listen for incoming connections on the given address
    pub async fn start(&mut self, addr: Multiaddr) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Start { addr, sender })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }
    /// Pull the content for the given root from the given peer
    pub async fn pull(
        &mut self,
        addr: Multiaddr,
        root: Cid,
        selector: Selector,
    ) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Pull {
                addr,
                root,
                selector,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }
}

#[derive(Debug)]
enum Command {
    Start {
        addr: Multiaddr,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    Pull {
        addr: Multiaddr,
        root: Cid,
        selector: Selector,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
}

#[derive(Debug)]
pub enum GraphSyncEvent {
    Accepted { peer_id: PeerId, request: Request },
    Received { peer_id: PeerId },
    Sent { peer_id: PeerId },
    Error { peer_id: PeerId },
}

enum Tx {
    PendingDial {
        request: Request,
    },
    PendingSubstream {
        request: Request,
    },
    PendingResponseSubstream {
        req_id: RequestId,
        root: Cid,
        selector: Selector,
    },
    Open {
        observed: Multiaddr,
        writer: MsgWriter<NegotiatedSubstream>,
    },
    Sending {
        fut: BoxFuture<'static, io::Result<()>>,
    },
}

pub struct Behaviour<L> {
    connected: HashMap<PeerId, HashMap<ConnectionId, Multiaddr>>,
    addresses: HashMap<PeerId, SmallVec<[Multiaddr; 4]>>,
    requests: HashMap<PeerId, Tx>,
    events: VecDeque<NetworkBehaviourAction<GraphSyncEvent, HandlerProto>>,
    blocks: L,
}

impl<L> Behaviour<L>
where
    L: BlockLoader + Send + Clone + 'static,
{
    pub fn new(blocks: L) -> Self {
        Behaviour {
            connected: HashMap::new(),
            addresses: HashMap::new(),
            events: VecDeque::new(),
            requests: HashMap::new(),
            blocks,
        }
    }

    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses.entry(*peer).or_default().push(address);
    }

    pub fn request(&mut self, peer: PeerId, request: Request) {
        if self
            .requests
            .insert(peer, Tx::PendingDial { request })
            .is_none()
            && !self.connected.contains_key(&peer)
        {
            let handler = self.new_handler();
            self.events.push_back(NetworkBehaviourAction::Dial {
                opts: DialOpts::peer_id(peer).build(),
                handler,
            });
        }
    }
}

impl<L> NetworkBehaviour for Behaviour<L>
where
    L: BlockLoader + Send + Clone + 'static,
{
    type ConnectionHandler = HandlerProto;
    type OutEvent = GraphSyncEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        HandlerProto
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        _failed_addresses: Option<&Vec<Multiaddr>>,
        _other_established: usize,
    ) {
        let addr = match endpoint {
            ConnectedPoint::Dialer { address, .. } => address.clone(),
            ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
        };

        self.connected
            .entry(*peer_id)
            .or_default()
            .insert(*conn, addr);
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        _: &ConnectedPoint,
        _: <Self::ConnectionHandler as IntoConnectionHandler>::Handler,
        remaining_established: usize,
    ) {
        if remaining_established == 0 {
            self.connected.remove(peer_id);
        } else if let Some(addrs) = self.connected.get_mut(peer_id) {
            addrs.remove(conn);
        }
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        _: Self::ConnectionHandler,
        error: &DialError,
    ) {
    }

    fn inject_new_listen_addr(&mut self, _id: ListenerId, _addr: &Multiaddr) {}

    fn inject_expired_listen_addr(&mut self, _id: ListenerId, _addr: &Multiaddr) {}

    // messages propagated by the handlers
    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: <<Self::ConnectionHandler as IntoConnectionHandler>::Handler as ConnectionHandler>::OutEvent,
    ) {
        match event {
            HandlerEvent::NewRequest(request) => {
                if let Some((root, selector)) = request.root().zip(request.selector()) {
                    self.requests.insert(
                        peer_id,
                        Tx::PendingResponseSubstream {
                            req_id: *request.id(),
                            root: *root,
                            selector: selector.clone(),
                        },
                    );

                    self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                        GraphSyncEvent::Accepted { peer_id, request },
                    ));
                    self.events
                        .push_back(NetworkBehaviourAction::NotifyHandler {
                            peer_id,
                            event: GraphSyncTask(),
                            handler: NotifyHandler::Any,
                        });
                }
            }
            HandlerEvent::NewResponse(writer) => {
                // let observed = self
                //     .connected
                //     .get(&peer_id)
                //     .and_then(|addrs| addrs.get(&connection))
                //     .expect("to be a connection when inject_event is called");
                match self.requests.remove(&peer_id).expect("to be a writer") {
                    Tx::PendingSubstream { request } => {
                        let fut = writer.send_request(request).boxed();
                        self.requests.insert(peer_id, Tx::Sending { fut });
                    }
                    Tx::PendingResponseSubstream {
                        req_id,
                        root,
                        selector,
                    } => {
                        let it = BlockIterator::new(self.blocks.clone(), root, selector);
                        let fut = writer.write(req_id, it).boxed();
                        self.requests.insert(peer_id, Tx::Sending { fut });
                    }
                    _ => (),
                }
            }
            HandlerEvent::Completed => {
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                    GraphSyncEvent::Received { peer_id },
                ));
            }
            _ => (),
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        // Get the first peer for which we have an open connection and a pending request
        let peer = self.requests.keys().find_map(|peer| {
            if self.connected.contains_key(peer) {
                Some(*peer)
            } else {
                None
            }
        });
        if let Some(peer_id) = peer {
            // Remove the current state
            let tx = self.requests.remove(&peer_id).expect("to be a request");
            match tx {
                Tx::PendingSubstream { .. } => {
                    self.requests.insert(peer_id, tx);
                }
                Tx::PendingResponseSubstream { .. } => {
                    self.requests.insert(peer_id, tx);
                }
                Tx::PendingDial { request } => {
                    // Our request is waiting for a connection. We can now open a new
                    // substream and wait for it.
                    self.requests
                        .insert(peer_id, Tx::PendingSubstream { request });
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        event: GraphSyncTask(),
                        handler: NotifyHandler::Any,
                    });
                }
                // A substream has been open and we are waiting for the transfer to complete.
                Tx::Sending { mut fut } => match Future::poll(Pin::new(&mut fut), cx) {
                    Poll::Ready(Ok(())) => {
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            GraphSyncEvent::Sent { peer_id },
                        ));
                    }
                    Poll::Pending => {
                        self.requests.insert(peer_id, Tx::Sending { fut });
                    }
                    Poll::Ready(Err(_err)) => {
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            GraphSyncEvent::Error { peer_id },
                        ));
                    }
                },
                _ => (),
            };
        }
        Poll::Pending
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(connections) = self.connected.get(peer) {
            addresses.extend(connections.values().cloned())
        }
        if let Some(more) = self.addresses.get(peer) {
            addresses.extend(more.into_iter().cloned());
        }
        addresses
    }
}

pub struct HandlerProto;

impl IntoConnectionHandler for HandlerProto {
    type Handler = GraphSyncHandler;

    fn into_handler(self, remote_peer_id: &PeerId, _endpoint: &ConnectedPoint) -> Self::Handler {
        GraphSyncHandler::new(*remote_peer_id)
    }

    fn inbound_protocol(&self) -> <Self::Handler as ConnectionHandler>::InboundProtocol {
        GraphSyncProtocol::inbound()
    }
}

pub struct GraphSyncHandler {
    remote_peer: PeerId,
    keep_alive: KeepAlive,
    events: SmallVec<
        [ConnectionHandlerEvent<GraphSyncProtocol<OutboundSubstream>, (), HandlerEvent, io::Error>;
            4],
    >,
    inbound: Option<BoxStream<'static, HandlerEvent>>,
}

#[derive(Debug)]
pub enum HandlerEvent {
    NewRequest(Request),
    NewResponse(MsgWriter<NegotiatedSubstream>),
    Partial(Cid, Vec<u8>),
    Missing(Cid),
    NotFound,
    Rejected,
    Completed,
}

#[derive(Debug)]
pub struct GraphSyncTask();

impl GraphSyncHandler {
    pub fn new(remote_peer: PeerId) -> Self {
        GraphSyncHandler {
            remote_peer,
            events: SmallVec::new(),
            keep_alive: KeepAlive::Yes,
            inbound: None,
        }
    }
}

impl ConnectionHandler for GraphSyncHandler {
    type InEvent = GraphSyncTask;
    type OutEvent = HandlerEvent;
    type Error = io::Error;
    type InboundProtocol = GraphSyncProtocol<InboundMsg>;
    type OutboundProtocol = GraphSyncProtocol<OutboundSubstream>;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(GraphSyncProtocol::inbound(), ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        output: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo,
    ) {
        if self.inbound.replace(output).is_some() {
            println!(
                "new inbound graphsync stream from {} while still upgrading previous one.",
                self.remote_peer
            );
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        output: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
        self.events
            .push(ConnectionHandlerEvent::Custom(HandlerEvent::NewResponse(
                output,
            )));
    }

    fn inject_event(&mut self, _task: Self::InEvent) {
        self.events
            .push(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(GraphSyncProtocol::outbound_substream(), ()),
            });
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        err: ConnectionHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error,
        >,
    ) {
        self.keep_alive = KeepAlive::No;
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            HandlerEvent,
            Self::Error,
        >,
    > {
        if !self.events.is_empty() {
            return Poll::Ready(self.events.remove(0));
        }

        while let Some(Poll::Ready(Some(event))) =
            self.inbound.as_mut().map(|f| f.poll_next_unpin(cx))
        {
            self.keep_alive = KeepAlive::Yes;
            return Poll::Ready(ConnectionHandlerEvent::Custom(event));
        }

        Poll::Pending
    }
}

pub const PROTOCOL_NAME: &[u8; 21] = b"/ipfs/graphsync/2.0.0";

#[derive(Debug, Clone)]
pub struct GraphSyncProtocol<T>(T);
pub struct InboundMsg();
pub struct OutboundMsg(Message);
pub struct OutboundSubstream();

impl GraphSyncProtocol<InboundMsg> {
    pub fn inbound() -> Self {
        GraphSyncProtocol(InboundMsg())
    }
}

impl GraphSyncProtocol<OutboundMsg> {
    pub fn outbound(msg: Message) -> Self {
        GraphSyncProtocol(OutboundMsg(msg))
    }
}

impl GraphSyncProtocol<OutboundSubstream> {
    pub fn outbound_substream() -> Self {
        GraphSyncProtocol(OutboundSubstream())
    }
}

impl<T> UpgradeInfo for GraphSyncProtocol<T> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl<C> InboundUpgrade<C> for GraphSyncProtocol<InboundMsg>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = BoxStream<'static, HandlerEvent>;
    type Error = anyhow::Error;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        future::ok(recv(socket).boxed())
    }
}

impl<C> OutboundUpgrade<C> for GraphSyncProtocol<OutboundMsg>
where
    C: AsyncWrite + Unpin + Send + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        send(socket, self.0 .0).boxed()
    }
}

impl<C> OutboundUpgrade<C> for GraphSyncProtocol<OutboundSubstream>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = MsgWriter<C>;
    type Error = io::Error;
    type Future = future::Ready<io::Result<Self::Output>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        future::ok(MsgWriter::new(socket))
    }
}
//
// filter out events to bubble up to the client notifying why a request is finished.
fn final_event(status: StatusCode) -> Option<HandlerEvent> {
    match status {
        StatusCode::RequestCompletedFull | StatusCode::RequestCompletedPartial => {
            Some(HandlerEvent::Completed)
        }
        StatusCode::RequestFailedContentNotFound => Some(HandlerEvent::NotFound),
        StatusCode::RequestRejected => Some(HandlerEvent::Rejected),
        _ => None,
    }
}

fn recv<T>(socket: T) -> impl Stream<Item = HandlerEvent>
where
    T: AsyncWrite + AsyncRead + Unpin + Send,
{
    unfold(socket, |mut r| async move {
        if let Ok(msg) = Message::from_net(&mut r).await {
            match msg.into_inner() {
                (Some(requests), _, _) => {
                    let mut events = Vec::new();
                    for req in requests {
                        events.push(HandlerEvent::NewRequest(req.clone()));
                    }
                    return Some((events, r));
                }
                (_, Some(responses), Some(blocks)) => {
                    let mut blk_map: HashMap<Cid, Vec<u8>> = HashMap::new();
                    let mut events = Vec::new();
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
                                    Some(blk) => HandlerEvent::Partial(data.link, blk),
                                    None => HandlerEvent::Missing(data.link),
                                };
                                events.push(event);
                            }
                        }
                        if let Some(event) = final_event(res.status) {
                            events.push(event);
                        }
                    }
                    // if we still have blocks in there, the provider is faulty.
                    debug_assert!(blk_map.is_empty());

                    return Some((events, r));
                }
                _ => (),
            };
        }
        None
    })
    .flat_map(stream::iter)
    .fuse()
}

async fn send<T>(mut io: T, msg: Message) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    msg.to_net(&mut io).await?;
    io.close().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::Request;
    use futures::pin_mut;
    use ipld_traversal::{blockstore::MemoryBlockstore, link_system::LinkSystem};
    use libp2p::core::{
        identity,
        muxing::StreamMuxerBox,
        transport,
        upgrade::{self, apply_inbound, apply_outbound},
        Transport,
    };
    use libp2p::mplex::MplexConfig;
    use libp2p::noise;
    use libp2p::swarm::{Swarm, SwarmEvent};
    use libp2p::tcp::{GenTcpConfig, TcpTransport};

    #[test]
    fn test_protocol() {
        let (tx, rx) = oneshot::channel();

        let req = Request::new();

        let reqn = req.clone();

        let bg_task = async_std::task::spawn(async move {
            let mut transport = TcpTransport::default().boxed();

            transport
                .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
                .unwrap();

            let addr = transport
                .next()
                .await
                .expect("new address event")
                .into_new_address()
                .expect("listen address");
            // send listener address to client
            tx.send(addr).unwrap();

            let socket = transport
                .next()
                .await
                .expect("request event")
                .into_incoming()
                .unwrap()
                .0
                .await
                .unwrap();

            let req = match apply_inbound(socket, GraphSyncProtocol::inbound())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
            {
                HandlerEvent::NewRequest(req) => req,
                _ => panic!("Unexpected event"),
            };

            assert_eq!(req.id(), reqn.id());
        });
        async_std::task::block_on(async move {
            let mut transport = TcpTransport::default();

            let msg = Message::from(req);

            let socket = transport.dial(rx.await.unwrap()).unwrap().await.unwrap();
            let response = apply_outbound(
                socket,
                GraphSyncProtocol::outbound(msg),
                upgrade::Version::V1,
            )
            .await
            .unwrap();

            bg_task.await;
        });
    }

    fn transport() -> (
        identity::PublicKey,
        transport::Boxed<(PeerId, StreamMuxerBox)>,
    ) {
        let id_keys = identity::Keypair::generate_ed25519();
        let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&id_keys)
            .unwrap();
        let pubkey = id_keys.public();
        let transport = TcpTransport::new(GenTcpConfig::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(MplexConfig::new())
            .boxed();
        (pubkey, transport)
    }

    #[test]
    fn behaviour_pull() {
        use ipld_traversal::{link_system::Prefix, selector::RecursionLimit};
        use libipld::{ipld, Ipld};
        use rand::prelude::*;

        let (mut swarm1, peer1, root) = {
            let (pubkey, transport) = transport();
            let peer_id = pubkey.to_peer_id();
            let store = MemoryBlockstore::new();
            let lsys = LinkSystem::new(store);

            const CHUNK_SIZE: usize = 250 * 1024;

            let mut bytes = vec![0u8; 3 * CHUNK_SIZE];
            thread_rng().fill(&mut bytes[..]);

            let mut chunks = bytes.chunks(CHUNK_SIZE);

            let links: Vec<Ipld> = chunks
                .map(|chunk| {
                    let leaf = ipld!({
                        "Data": Ipld::Bytes(chunk.to_vec()),
                    });
                    let cid = lsys
                        .store(Prefix::new(0x71, 0x13), &leaf)
                        .expect("link system should store leaf node");
                    let link = ipld!({
                        "Hash": cid,
                        "Tsize": CHUNK_SIZE,
                    });
                    link
                })
                .collect();

            let root_node = ipld!({
                "Links": links,
            });

            let root = lsys
                .store(Prefix::new(0x71, 0x13), &root_node)
                .expect("link system to store root node");

            let swarm = Swarm::new(transport, Behaviour::new(lsys), peer_id);
            (swarm, peer_id, root)
        };
        let (mut swarm2, peer2) = {
            let (pubkey, transport) = transport();
            let peer_id = pubkey.to_peer_id();
            let store = MemoryBlockstore::new();
            let lsys = LinkSystem::new(store);
            let swarm = Swarm::new(transport, Behaviour::new(lsys), peer_id);
            (swarm, peer_id)
        };

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let listener_addr = async_std::task::block_on(async {
            loop {
                let swarm1_fut = swarm1.select_next_some();
                pin_mut!(swarm1_fut);
                match swarm1_fut.await {
                    SwarmEvent::NewListenAddr { address, .. } => return address,
                    _ => {}
                }
            }
        });

        let client = swarm2.behaviour_mut();
        client.add_address(&peer1, listener_addr);

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let req = Request::builder()
            .root(root)
            .selector(selector)
            .build()
            .unwrap();
        client.request(peer1, req.clone());

        async_std::task::block_on(async move {
            loop {
                let swarm1_fut = swarm1.select_next_some();
                let swarm2_fut = swarm2.select_next_some();

                pin_mut!(swarm1_fut);
                pin_mut!(swarm2_fut);

                match future::select(swarm1_fut, swarm2_fut)
                    .await
                    .factor_second()
                    .0
                {
                    future::Either::Left(SwarmEvent::Behaviour(GraphSyncEvent::Accepted {
                        request,
                        ..
                    })) => {
                        assert_eq!(request.id(), req.id());
                        println!("request accepted");
                    }
                    future::Either::Right(SwarmEvent::ConnectionEstablished { .. }) => {
                        //
                        println!("connection established");
                    }
                    future::Either::Right(SwarmEvent::Behaviour(GraphSyncEvent::Received {
                        ..
                    })) => {
                        return;
                    }
                    _ => continue,
                }
            }
        });
    }
}
