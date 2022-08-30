use async_std::task;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use graphsync::{GraphSyncEvent, Request, RequestId};
use ipld_traversal::{
    blockstore::{Blockstore, MemoryBlockstore},
    unixfs::unixfs_path_selector,
    LinkSystem, Prefix, Selector,
};
use libipld::{ipld, Cid, Ipld};
use libp2p::{
    core, identity, mplex,
    multiaddr::Protocol,
    noise,
    swarm::{Swarm, SwarmBuilder, SwarmEvent},
    tcp, Multiaddr, PeerId, Transport,
};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    path::PathBuf,
    time::{Duration, Instant},
};

#[async_std::main]
async fn main() -> Result<(), anyhow::Error> {
    let opt = Opt::parse();

    let keys = identity::Keypair::generate_ed25519();
    let tcp_trans = tcp::TcpTransport::new(tcp::GenTcpConfig::new().nodelay(true));
    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&keys)
            .expect("noise keygen failed");
        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };
    let mut mplex_config = mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(usize::MAX);

    let transport = tcp_trans
        .upgrade(core::upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed();

    let peer_id = keys.public().to_peer_id();

    let store = MemoryBlockstore::new();

    let swarm =
        SwarmBuilder::new(transport, graphsync::Behaviour::new(store.clone()), peer_id).build();

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
        CliArgument::Provide { path } => {
            let lsys = LinkSystem::new(store);
            let mut file = fs::File::open(path)?;
            let info = file.metadata()?;
            if info.is_file() {
                // Super low-fi unixfs split chunking ;)
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

                println!("==> Storing file with root CID: {}", root);
            } else {
                unimplemented!();
            }

            loop {
                match evt_receiver.next().await {
                    Some(Event::InboundRequest { root }) => {
                        println!("handling request for root {:?}", root);
                    }
                    _ => (),
                }
            }
        }
        CliArgument::Pull { peer, path } => {
            if let Some((root, selector)) = unixfs_path_selector(path) {
                client.pull(peer, root, selector).await?;
            } else {
                return Err(anyhow::format_err!("invalid pull params"));
            }
        }
    };

    Ok(())
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

pub struct EventLoop<BS: Blockstore + Send + Clone + 'static> {
    swarm: Swarm<graphsync::Behaviour<BS>>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_pulls: HashMap<RequestId, PendingPull>,
}

impl<BS> EventLoop<BS>
where
    BS: Blockstore + Send + Clone + 'static,
{
    fn new(
        swarm: Swarm<graphsync::Behaviour<BS>>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_pulls: HashMap::new(),
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

    async fn handle_event(&mut self, event: SwarmEvent<GraphSyncEvent, io::Error>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                println!(
                    "==> node listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id.into()))
                );
            }
            SwarmEvent::Behaviour(GraphSyncEvent::Accepted { request, .. }) => {
                self.event_sender
                    .send(Event::InboundRequest {
                        root: *request.root().expect("to have a root CID"),
                    })
                    .await
                    .expect("receiver not to be dropped");
            }
            SwarmEvent::Behaviour(GraphSyncEvent::Completed { id, received }) => {
                if let Some(pp) = self.pending_pulls.remove(&id) {
                    let elapsed = pp.start.elapsed();
                    println!(
                        "==> completed request {}, received {} bytes ({}Mbps)",
                        id,
                        received,
                        (((received as f64 / 1e+6)
                            / (elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 * 1e-9))
                            * 100.0)
                            .round()
                            / 100.0
                    );
                    let _ = pp.sender.send(Ok(()));
                }
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
                let req = Request::builder()
                    .root(root)
                    .selector(selector)
                    .build()
                    .expect("request to be ok");
                if let Some(Protocol::P2p(ma)) = addr.pop() {
                    let peer_id = PeerId::try_from(ma).unwrap();
                    self.pending_pulls
                        .insert(*req.id(), PendingPull::new(sender));
                    let gs = self.swarm.behaviour_mut();
                    gs.add_address(&peer_id, addr);
                    println!("==> queuing request");
                    gs.request(peer_id, req);
                } else {
                    let _ = sender.send(Err(anyhow::format_err!("invalid p2p address")));
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum Event {
    InboundRequest { root: Cid },
}

struct PendingPull {
    sender: oneshot::Sender<anyhow::Result<()>>,
    start: Instant,
}

impl PendingPull {
    fn new(sender: oneshot::Sender<anyhow::Result<()>>) -> Self {
        PendingPull {
            sender,
            start: Instant::now(),
        }
    }
}
