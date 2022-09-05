# GraphSync

![](https://img.shields.io/badge/made%20by-Myel-blue)
![](https://img.shields.io/github/license/myelnet/js-graphsync?color=green)

> Rust implementation of the GraphSync v2 wire protocol.

## Background

GraphSync is an IPFS data transfer protocol used across the IPFS and Web3 ecosystem for exchanging
IPLD data. It is used by Filecoin for syncing the blockchain and transfering DAGified content
in a trustless fashion.

## Usage

To add this crate to your repo, either add `graphsync` to your `Cargo.toml` or run `cargo add graphsync`.
You might also need selectors from `ipld_traversal` crate.

```rust
use graphsync::{GraphSync, resolve_raw_bytes};
use ipld_traversal::blockstore::MemoryBlockstore;
use libp2p::{Swarm, multiaddr::{Multiaddr, Protocol}, PeerId};

// See Libp2p examples for more info on transports.
let (peer_id, transport) = gen_transport();

let store = MemoryBlockstore::new();

let mut swarm = Swarm::new(transport, GraphSync::new(store), peer_id);

let client = swarm.behaviour_mut();

let mut maddr: Multiaddr = "/ip4/127.0.0.1/tcp/53870/p2p/12D3KooWC2E2mnp5x3CfJG4n9vFXabTwSrc2PfbNWzSZhCjCN3rr".parse().unwrap();

if let Some(Protocol::P2p(ma)) = maddr.pop() {

let peer_id = PeerId::try_from(ma).unwrap();

client.add_address(&peer_id, maddr);
client.request(
  peer_id, 
  "bafybeihq3wo4u27amukm36i7vbpirym4y2lvy53uappzhl3oehcm4ukphu/dir/file.ext".parse().unwrap(),
);

// See `/src/resolver.rs` for examples on how to make a resolver for your custom DAG...
let file = resolve_raw_bytes(&mut swarm).await;

```

## Benchmarks

To benchmark IPLD traversals for different tree sizes run
```sh
cargo bench traversal
```
To benchmark end to end transfers run
```sh
cargo bench graphsync
```

## Examples

- [Data Transfer](/examples/data-transfer/main.rs)
Demonstrate how to integrate GraphSync in a larger app or into a wrapper
protocol using the Filecoin data transfer protocol.
See the main.rs file for info on how to run.
