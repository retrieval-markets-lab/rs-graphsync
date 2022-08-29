use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use ipld_traversal::Selector;
use libipld::Cid;
use libp2p::Multiaddr;
use std::error::Error;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Ok(())
}

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
