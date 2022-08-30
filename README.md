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
/// TODO
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
