use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use ipld_traversal::{
    blockstore::MemoryBlockstore,
    selector::{RecursionLimit, Selector},
    BlockIterator, LinkSystem, Prefix,
};
use libipld::{ipld, Cid, Ipld};
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
                    BlockIterator::new(LinkSystem::new(store), root, selector).count();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_traversal);
criterion_main!(benches);
