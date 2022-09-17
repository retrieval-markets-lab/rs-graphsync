use criterion::async_executor::AsyncStdExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use futures::prelude::*;
use futures::{
    channel::oneshot,
    sink::{self, SinkExt},
};
use graphsync::{
    messages::{Message, MsgWriter},
    network::{HandlerEvent, MsgReader},
    Request,
};
use ipld_traversal::{
    blockstore::MemoryBlockstore,
    selector::{RecursionLimit, Selector},
    BlockTraversal, LinkSystem, Prefix,
};
use libipld::{ipld, Cid, Ipld};
use libp2p::{tcp::TcpTransport, Transport};
use rand::prelude::*;

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

fn bench_traversal(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("traversal");
    for size in [MB, 4 * MB, 15 * MB, 60 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("data", size), size, |b, &size| {
            b.iter_batched(
                || prepare_blocks(size),
                |(store, root)| {
                    let selector = Selector::ExploreRecursive {
                        limit: RecursionLimit::None,
                        sequence: Box::new(Selector::ExploreAll {
                            next: Box::new(Selector::ExploreRecursiveEdge),
                        }),
                        current: None,
                    };
                    BlockTraversal::new(LinkSystem::new(store), root, selector).count();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_encoding(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("encoding");
    for size in [MB, 4 * MB, 15 * MB, 60 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("data", size), size, |b, &size| {
            b.to_async(AsyncStdExecutor).iter_batched(
                || prepare_blocks(size),
                |(store, root)| async move {
                    let req = Request::new();
                    let selector = Selector::ExploreRecursive {
                        limit: RecursionLimit::None,
                        sequence: Box::new(Selector::ExploreAll {
                            next: Box::new(Selector::ExploreRecursiveEdge),
                        }),
                        current: None,
                    };

                    let (tx, rx) = oneshot::channel();

                    let write = async {
                        let mut transport = TcpTransport::default();

                        let mut socket = transport.dial(rx.await.unwrap()).unwrap().await.unwrap();

                        let it = BlockTraversal::new(LinkSystem::new(store), root, selector);
                        let writer = MsgWriter::new(&mut socket);
                        let _ = writer.write(*req.id(), it).await.unwrap();
                    };
                    let read = async {
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

                        let mut stream =
                            Box::pin(MsgReader::new(socket).into_stream().map(|ev| Ok(ev)));

                        let mut drain = sink::drain();
                        drain.send_all(&mut stream).await.unwrap();
                    };
                    futures::join!(write, read);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(encoding, bench_encoding);
criterion_group!(traversal, bench_traversal);
criterion_main!(traversal, encoding);
