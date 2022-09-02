use criterion::async_executor::AsyncStdExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use futures::{pin_mut, prelude::*};
use graphsync::{GraphSync, GraphSyncEvent, Request};
use ipld_traversal::{
    blockstore::MemoryBlockstore,
    selector::{RecursionLimit, Selector},
    LinkSystem, Prefix,
};
use libipld::{ipld, Cid, Ipld};
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::{GenTcpConfig, TcpTransport};
use libp2p::{identity, mplex, multiaddr, Multiaddr};
use libp2p::{PeerId, Transport};
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;

fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
    let id_key = identity::Keypair::generate_ed25519();
    let peer_id = id_key.public().to_peer_id();
    let dh_key = Keypair::<X25519Spec>::new()
        .into_authentic(&id_key)
        .unwrap();
    let noise = NoiseConfig::xx(dh_key).into_authenticated();

    let transport = TcpTransport::new(GenTcpConfig::new().nodelay(true))
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise)
        .multiplex(mplex::MplexConfig::new())
        .timeout(Duration::from_secs(20))
        .boxed();
    (peer_id, transport)
}

fn prepare_blocks(size: usize) -> (MemoryBlockstore, Cid) {
    let mut data = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut data);

    const CHUNK_SIZE: usize = 250 * 1024;

    let store = MemoryBlockstore::new();
    let lsys = LinkSystem::new(store.clone());
    let chunks = data.chunks(CHUNK_SIZE);

    let links: Vec<Ipld> = chunks
        .map(|chunk| {
            let leaf = Ipld::Bytes(chunk.to_vec());
            // encoding as raw
            let cid = lsys
                .store(Prefix::new(0x55, 0x13), &leaf)
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

    (store, root)
}

fn bench_transfer(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("graphsync");
    for size in [MB, 4 * MB, 15 * MB, 60 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("remote", size), size, move |b, &size| {
            b.to_async(AsyncStdExecutor).iter_batched(
                || prepare_blocks(size),
                |(store, root)| async move {
                    let (peer1, trans) = mk_transport();
                    let mut swarm1 = Swarm::new(trans, GraphSync::new(store), peer1);

                    Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

                    let listener_addr = async {
                        loop {
                            let swarm1_fut = swarm1.select_next_some();
                            pin_mut!(swarm1_fut);
                            match swarm1_fut.await {
                                SwarmEvent::NewListenAddr { address, .. } => return address,
                                _ => {}
                            }
                        }
                    }
                    .await;

                    let (peer2, trans) = mk_transport();
                    let mut swarm2 =
                        Swarm::new(trans, GraphSync::new(MemoryBlockstore::new()), peer2);

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
                            future::Either::Right(SwarmEvent::Behaviour(
                                GraphSyncEvent::Completed { .. },
                            )) => {
                                return;
                            }
                            _ => continue,
                        }
                    }
                },
                BatchSize::SmallInput,
            );
        });

        // group.bench_with_input(BenchmarkId::new("local", size), size, move |b, &size| {
        //     b.to_async(AsyncStdExecutor).iter_batched(
        //         || prepare_blocks(size),
        //         |(store, root)| async move {
        //             let (peer1, trans) = mk_transport();
        //             let mut swarm = Swarm::new(trans, Behaviour::new(store), peer1);

        //             let id_key = identity::Keypair::generate_ed25519();
        //             let peer_id = id_key.public().to_peer_id();

        //             let client = swarm.behaviour_mut();

        //             let selector = Selector::ExploreRecursive {
        //                 limit: RecursionLimit::None,
        //                 sequence: Box::new(Selector::ExploreAll {
        //                     next: Box::new(Selector::ExploreRecursiveEdge),
        //                 }),
        //                 current: None,
        //             };

        //             let req = Request::builder()
        //                 .root(root)
        //                 .selector(selector)
        //                 .build()
        //                 .unwrap();

        //             client.request(peer_id, req.clone());

        //             loop {
        //                 match swarm.next().await {
        //                     Some(SwarmEvent::Behaviour(GraphSyncEvent::Completed { .. })) => {
        //                         return;
        //                     }
        //                     _ => continue,
        //                 }
        //             }
        //         },
        //         BatchSize::SmallInput,
        //     );
        // });
    }
}

// criterion_group! {
//     name = benches;
//     config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
//     targets = bench_traversal
// }
criterion_group!(benches, bench_transfer);
criterion_main!(benches);
