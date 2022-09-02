/// # Example: Filecoin Data Transfer Protocol
///
/// This is an example integrating graphsync with other behaviours to implement the Filecoin
/// data-transfer protocol. In this case, an additional messaging protocol is used to send push
/// requests and completion responses. Transfer payloads are also attached as graphsync extensions.
///
/// This implementation is not production ready although close in feature parity and fully
/// compatible with the Go and JS implementations.
///
/// ## Usage
///
/// First run a provider node with the following command:
///
/// ```
/// cargo run --example cmd_api \
///     provide \
///     --path <path-to-a-file-to-provide>
/// ```
///
/// To run a client push operation, replace the values with your own:
///
/// ```
/// cargo run --example cmd_api \
///     push \
///     --path <path-to-file-to-push> \
///     --peer "/ip4/127.0.0.1/tcp/53870/p2p/12D3KooWC2E2mnp5x3CfJG4n9vFXabTwSrc2PfbNWzSZhCjCN3rr"
/// ```
///
/// To run a client pull operation (replace with your values):
///
/// ```
/// cargo run --example cmd_api \
///     pull \
///     --path bafyreidh5nz7eoupru3o2bk4xrf4x7xcukaicfvh5xucksdfq3ara4yuja \
///     --peer "/ip4/127.0.0.1/tcp/53870/p2p/12D3KooWC2E2mnp5x3CfJG4n9vFXabTwSrc2PfbNWzSZhCjCN3rr"
/// ```
///
///
use crate::data_transfer::{
    pull_request, pull_response, BasicVoucher, DataTransfer, TransferEvent, TransferMessage,
    TransferMsgEvent, TransferRequest, TransferResponse,
};
use async_std::task;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use graphsync::{GraphSync, GraphSyncEvent, RequestId};
use ipld_traversal::{
    blockstore::{Blockstore, MemoryBlockstore},
    selector::RecursionLimit,
    unixfs::unixfs_path_selector,
    LinkSystem, Prefix, Selector,
};
use libipld::{ipld, Cid, Ipld};
use libp2p::{
    core,
    core::either::EitherError,
    dns,
    identify::{Identify, IdentifyConfig, IdentifyEvent},
    identity, mplex,
    multiaddr::Protocol,
    noise,
    swarm::{ConnectionHandlerUpgrErr, Swarm, SwarmBuilder, SwarmEvent},
    tcp, websocket, Multiaddr, NetworkBehaviour, PeerId, Transport,
};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    path::PathBuf,
    time::{Duration, Instant},
};

mod data_transfer;

#[async_std::main]
async fn main() -> Result<(), anyhow::Error> {
    let opt = Opt::parse();

    let keys = identity::Keypair::generate_ed25519();
    let transport = {
        let dns_tcp = dns::DnsConfig::system(tcp::TcpTransport::new(
            tcp::GenTcpConfig::new().nodelay(true),
        ))
        .await?;
        let ws_dns_tcp = websocket::WsConfig::new(
            dns::DnsConfig::system(tcp::TcpTransport::new(
                tcp::GenTcpConfig::new().nodelay(true),
            ))
            .await?,
        );
        dns_tcp.or_transport(ws_dns_tcp)
    };
    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&keys)
            .expect("noise keygen failed");
        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };
    let mut mplex_config = mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(usize::MAX);

    let transport = transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed();

    let peer_id = keys.public().to_peer_id();

    let store = MemoryBlockstore::new();

    let swarm = SwarmBuilder::new(
        transport,
        DataTransferBehaviour::new(store.clone(), keys.public()),
        peer_id,
    )
    .build();

    let (cmd_sender, cmd_receiver) = mpsc::channel(0);
    let (evt_sender, mut evt_receiver) = mpsc::channel(0);

    let ev_loop = EventLoop::new(swarm, cmd_receiver, evt_sender);

    task::spawn(ev_loop.run());

    let mut client = Client::new(cmd_sender);

    client
        .start("/ip4/0.0.0.0/tcp/0".parse()?)
        .await
        .expect("swarm to start listening");

    match opt.argument {
        // Provide adds the file at the given path into the blockstore
        // and waits for inbound requests.
        CliArgument::Provide { path } => {
            let root = add_file(store.clone(), path)?;
            println!("==> Storing file with root CID: {}", root);
            loop {
                match evt_receiver.next().await {
                    Some(Event::InboundPullRequest { root }) => {
                        println!("==> handling pull request for root {:?}", root);
                    }
                    Some(Event::InboundPushRequest { root }) => {
                        println!("==> handling push request for root {:?}", root);
                    }
                    Some(Event::InboundPullCompleted { peer_id }) => {
                        println!("==> completed pull request with {:?}", peer_id);
                    }
                    Some(Event::InboundPushCompleted { peer_id }) => {
                        println!("==> completed push request with {:?}", peer_id);
                    }
                    _ => (),
                }
            }
        }
        // Pull the given IPLD path from the given peer and return when we're done.
        CliArgument::Pull { peer, path } => {
            if let Some((root, selector)) = unixfs_path_selector(path) {
                client.pull(peer, root, selector).await?;
            } else {
                return Err(anyhow::format_err!("invalid pull params"));
            }
        }
        // Push adds the file at the given path into the blockstore and replicates it
        // on the given remote peer.
        CliArgument::Push { peer, path } => {
            let root = add_file(store.clone(), path)?;
            println!("==> Storing file with root CID: {}", root);
            let selector = Selector::ExploreRecursive {
                limit: RecursionLimit::None,
                sequence: Box::new(Selector::ExploreAll {
                    next: Box::new(Selector::ExploreRecursiveEdge),
                }),
                current: None,
            };
            client.push(peer, root, selector).await?;
        }
    };

    Ok(())
}

// Super low-fi unixfs split chunking ;)
fn add_file<BS>(store: BS, path: PathBuf) -> io::Result<Cid>
where
    BS: Blockstore + 'static + Clone,
{
    let lsys = LinkSystem::new(store);
    let mut file = fs::File::open(path)?;
    let info = file.metadata()?;
    if info.is_file() {
        let mut links = Vec::new();
        loop {
            let mut chunk = [0; 256 * 1024];
            match file.read(&mut chunk[..]) {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    let cid = lsys
                        .store(Prefix::new(0x55, 0x12), &Ipld::Bytes(chunk.to_vec()))
                        .expect("link system should store chunk");
                    links.push(ipld!({
                        "Hash": cid,
                        "Tsize": size,
                    }));
                }
                Err(err) => {
                    return Err(err.into());
                }
            };
        }
        let root_node = ipld!({
            "Links": links,
        });
        let root = lsys
            .store(Prefix::new(0x71, 0x12), &root_node)
            .expect("link system to store root node");
        Ok(root)
    } else {
        unimplemented!();
    }
}

#[derive(Debug, Parser)]
#[clap(name = "GraphSync example")]
struct Opt {
    #[clap(subcommand)]
    argument: CliArgument,
}

#[derive(Debug, Parser)]
enum CliArgument {
    Provide {
        #[clap(long)]
        path: PathBuf,
    },
    Pull {
        #[clap(long)]
        path: String,
        #[clap(long)]
        peer: Multiaddr,
    },
    Push {
        #[clap(long)]
        path: PathBuf,
        #[clap(long)]
        peer: Multiaddr,
    },
}

#[derive(Clone)]
struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Client { sender }
    }

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

    /// Push the content to the given peer
    pub async fn push(
        &mut self,
        addr: Multiaddr,
        root: Cid,
        selector: Selector,
    ) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Push {
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
    Push {
        addr: Multiaddr,
        root: Cid,
        selector: Selector,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
}

pub struct EventLoop<BS: Blockstore + Send + Clone + 'static> {
    swarm: Swarm<DataTransferBehaviour<BS>>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    transfer_channels: HashMap<u64, ChannelState>,
    transfer_ids: HashMap<RequestId, u64>,
    next_request_id: u64,
}

impl<BS> EventLoop<BS>
where
    BS: Blockstore + Send + Clone + 'static,
{
    fn new(
        swarm: Swarm<DataTransferBehaviour<BS>>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            transfer_channels: HashMap::new(),
            transfer_ids: HashMap::new(),
            next_request_id: 1,
        }
    }

    pub async fn run(mut self) {
        loop {
            futures::select! {
                event = self.swarm.next() => self.handle_event(event.expect("swarm stream to be infinite.")).await,
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c).await,
                    None => return,
                },
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: SwarmEvent<
            DataTransferEvent,
            EitherError<
                EitherError<std::io::Error, ConnectionHandlerUpgrErr<std::io::Error>>,
                std::io::Error,
            >,
        >,
    ) {
        match event {
            // anounce when the swarm knows the addresses we are listening on.
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                println!(
                    "==> node listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id.into()))
                );
            }
            // Graphsync (Local node) has accepted a request
            SwarmEvent::Behaviour(DataTransferEvent::GraphSync(GraphSyncEvent::Accepted {
                request,
                ..
            })) => {
                if let Ok(msg) = TransferMessage::<BasicVoucher, BasicVoucher>::try_from(&request) {
                    if msg.is_request {
                        let req = msg.request.expect("msg to be a request");
                        println!("got a request {:?}", req);
                        self.transfer_ids.insert(*request.id(), req.transfer_id);
                        self.event_sender
                            .send(Event::InboundPullRequest {
                                root: *request.root().expect("to have a root CID"),
                            })
                            .await
                            .expect("receiver not to be dropped");
                    } else {
                        let res = msg.response.expect("msg to be a response");
                        println!("got a response {:?}", res);
                        self.transfer_ids.insert(*request.id(), res.transfer_id);
                    }
                }
            }
            // A transfer pulling from a remote peer was successfully completed
            SwarmEvent::Behaviour(DataTransferEvent::GraphSync(GraphSyncEvent::Completed {
                id,
                peer_id,
                received,
            })) => {
                let req_id = self
                    .transfer_ids
                    .remove(&id)
                    .expect("to have an open channel");
                if let Some(state) = self.transfer_channels.remove(&req_id) {
                    match state.graphsync_completed(received) {
                        ChannelState::Completed {
                            tduration,
                            sender,
                            received,
                        } => {
                            println!(
                                "==> completed request {}, received {} bytes ({}Mbps)",
                                id,
                                received,
                                calc_rate(received, tduration),
                            );
                            let _ = sender.send(Ok(()));
                        }
                        e => {
                            self.transfer_channels.insert(req_id, e);
                        }
                    };
                } else {
                    // Else we send a completion message as we just finished fulfilling a push
                    // request
                    self.swarm
                        .behaviour_mut()
                        .request_response_mut()
                        .send_response(&peer_id, TransferResponse::complete(req_id));

                    self.event_sender
                        .send(Event::InboundPushCompleted { peer_id })
                        .await
                        .expect("receiver not to be dropped");
                }
            }
            SwarmEvent::Behaviour(DataTransferEvent::GraphSync(
                GraphSyncEvent::SentAllBlocks { id, peer_id, sent },
            )) => {
                println!("graphsync sent");
                let tid = self
                    .transfer_ids
                    .remove(&id)
                    .expect("graphsync response to be tracked");
                if let Some(state) = self.transfer_channels.remove(&tid) {
                    // if we have a channel, it's a push request we're sending data for.
                    match state.graphsync_completed(sent) {
                        ChannelState::Completed { sender, .. } => {
                            let _ = sender.send(Ok(()));
                        }
                        e => {
                            self.transfer_channels.insert(tid, e);
                        }
                    }
                } else {
                    // Else we send a completion message as we just finished fullfilling a pull
                    // request
                    self.swarm
                        .behaviour_mut()
                        .request_response_mut()
                        .send_response(&peer_id, TransferResponse::complete(tid));

                    self.event_sender
                        .send(Event::InboundPullCompleted { peer_id })
                        .await
                        .expect("receiver not to be dropped");
                }
            }
            SwarmEvent::Behaviour(DataTransferEvent::RequestResponse(TransferEvent::Message {
                message,
                peer,
            })) => match message {
                TransferMsgEvent::Request(request) => {
                    // requests sent directly should be push only, pull requests are attached as
                    // graphsync extensions.
                    debug_assert!(!request.pull);

                    let req = pull_response(&request).expect("request to be ok");

                    self.event_sender
                        .send(Event::InboundPushRequest {
                            root: *req.root().expect("to have a root CID"),
                        })
                        .await
                        .expect("receiver not to be dropped");

                    self.transfer_ids.insert(*req.id(), request.transfer_id);
                    self.swarm
                        .behaviour_mut()
                        .graphsync_mut()
                        .request(peer, req);
                }
                TransferMsgEvent::Response(response) => {
                    if response.is_complete() {
                        if let Some(state) = self.transfer_channels.remove(&response.transfer_id) {
                            match state.transfer_completed() {
                                ChannelState::Completed {
                                    tduration,
                                    sender,
                                    received,
                                } => {
                                    println!(
                                        "==> completed request {}, received {} bytes ({}Mbps)",
                                        &response.transfer_id,
                                        received,
                                        calc_rate(received, tduration),
                                    );
                                    let _ = sender.send(Ok(()));
                                }
                                e => {
                                    self.transfer_channels.insert(response.transfer_id, e);
                                }
                            }
                        }
                    }
                }
                _ => {}
            },
            SwarmEvent::Behaviour(DataTransferEvent::RequestResponse(
                TransferEvent::OutboundFailure { error },
            )) => {
                println!("failed to send message {}", error);
            }
            SwarmEvent::Dialing(pid) => println!("==> dialing peer {}", pid),
            SwarmEvent::ConnectionEstablished { .. } => {
                println!("==> connection established");
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                println!("==> connection error {}", error);
            }
            _ => {}
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Start { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(e.into())),
                };
            }
            Command::Pull {
                mut addr,
                root,
                selector,
                sender,
            } => {
                let id = self.next_request_id();
                let req = pull_request(id, root, selector).expect("request to be ok");

                if let Some(Protocol::P2p(ma)) = addr.pop() {
                    let peer_id = PeerId::try_from(ma).unwrap();

                    self.transfer_channels
                        .insert(id, ChannelState::start(sender));
                    self.transfer_ids.insert(*req.id(), id);

                    let gs = self.swarm.behaviour_mut().graphsync_mut();
                    gs.add_address(&peer_id, addr);
                    gs.request(peer_id, req);
                } else {
                    let _ = sender.send(Err(anyhow::format_err!("invalid p2p address")));
                }
            }
            Command::Push {
                mut addr,
                root,
                selector,
                sender,
            } => {
                if let Some(Protocol::P2p(ma)) = addr.pop() {
                    let id = self.next_request_id();
                    let peer_id = PeerId::try_from(ma).unwrap();
                    let rr = self.swarm.behaviour_mut().request_response_mut();
                    rr.add_address(&peer_id, addr);

                    let mut req = TransferRequest::<BasicVoucher>::default();
                    req.transfer_id = id;
                    req.root = root;
                    req.selector = Some(selector);
                    req.voucher_type = "BasicVoucher".into();
                    req.voucher = Some("fake data".into());

                    self.transfer_channels
                        .insert(id, ChannelState::start(sender));

                    rr.send_request(&peer_id, req);
                }
            }
        }
    }

    /// Returns the next request ID.
    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }
}

#[derive(Debug)]
pub enum Event {
    InboundPullRequest { root: Cid },
    InboundPullCompleted { peer_id: PeerId },
    InboundPushRequest { root: Cid },
    InboundPushCompleted { peer_id: PeerId },
}

enum ChannelState {
    Started {
        sender: oneshot::Sender<anyhow::Result<()>>,
        start: Instant,
    },
    PendingLastBlocks {
        sender: oneshot::Sender<anyhow::Result<()>>,
        start: Instant,
    },
    PendingCompletionMsg {
        received: usize,
        tduration: Duration,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    Completed {
        received: usize,
        tduration: Duration,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
}

impl ChannelState {
    fn start(sender: oneshot::Sender<anyhow::Result<()>>) -> Self {
        ChannelState::Started {
            start: Instant::now(),
            sender,
        }
    }
    fn graphsync_completed(self, received: usize) -> Self {
        match self {
            ChannelState::Started { sender, start } => ChannelState::PendingCompletionMsg {
                sender,
                received,
                tduration: start.elapsed(),
            },
            ChannelState::PendingLastBlocks { sender, start } => ChannelState::Completed {
                sender,
                received,
                tduration: start.elapsed(),
            },
            _ => self,
        }
    }
    fn transfer_completed(self) -> Self {
        match self {
            ChannelState::Started { sender, start } => {
                ChannelState::PendingLastBlocks { sender, start }
            }
            ChannelState::PendingCompletionMsg {
                sender,
                tduration,
                received,
            } => ChannelState::Completed {
                sender,
                tduration,
                received,
            },
            _ => self,
        }
    }
}

fn calc_rate(size: usize, duration: Duration) -> f64 {
    (((size as f64 / 1e+6) / (duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9))
        * 100.0)
        .round()
        / 100.0
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "DataTransferEvent")]
struct DataTransferBehaviour<BS> {
    identify: Identify,
    request_response: DataTransfer,
    graphsync: GraphSync<BS>,
}

impl<BS> DataTransferBehaviour<BS>
where
    BS: Blockstore + 'static + Clone + Send,
{
    fn new(store: BS, pubkey: identity::PublicKey) -> Self {
        DataTransferBehaviour {
            identify: Identify::new(IdentifyConfig::new("/ipfs/id/1.0.0".into(), pubkey)),
            request_response: Default::default(),
            graphsync: GraphSync::new(store),
        }
    }

    fn graphsync_mut(&mut self) -> &mut GraphSync<BS> {
        &mut self.graphsync
    }

    fn request_response_mut(&mut self) -> &mut DataTransfer {
        &mut self.request_response
    }
}

#[derive(Debug)]
enum DataTransferEvent {
    Identify(IdentifyEvent),
    RequestResponse(TransferEvent),
    GraphSync(GraphSyncEvent),
}

impl From<TransferEvent> for DataTransferEvent {
    fn from(event: TransferEvent) -> Self {
        DataTransferEvent::RequestResponse(event)
    }
}

impl From<GraphSyncEvent> for DataTransferEvent {
    fn from(event: GraphSyncEvent) -> Self {
        DataTransferEvent::GraphSync(event)
    }
}

impl From<IdentifyEvent> for DataTransferEvent {
    fn from(event: IdentifyEvent) -> Self {
        DataTransferEvent::Identify(event)
    }
}
