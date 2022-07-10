use crate::blockstore::Blockstore;
use anyhow::{anyhow, Result};
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::multihash::{Code, Multihash, MultihashDigest};
use libipld::cid::Version;
use libipld::codec::{Codec, Decode, Encode};
use libipld::codec_impl::IpldCodec;
use libipld::{Cid, Ipld};
use std::io::Cursor;

pub struct LinkSystem<BS> {
    bstore: BS,
}

impl<BS> LinkSystem<BS>
where
    BS: Blockstore,
{
    pub fn new(bstore: BS) -> Self {
        Self { bstore }
    }
    pub fn load(&self, cid: Cid) -> Result<Ipld> {
        let codec = IpldCodec::try_from(cid.codec())?;
        if let Some(blk) = self.bstore.get(&cid)? {
            let mut reader = Cursor::new(blk);
            return Ipld::decode(codec, &mut reader);
        }
        Err(anyhow!("not found"))
    }
    pub fn store(&self, p: Prefix, n: Ipld) -> Result<Cid> {
        let codec = IpldCodec::try_from(p.codec)?;
        let mut buf = Vec::new();
        Ipld::encode(&n, codec, &mut buf)?;
        let cid = p.to_cid(&buf)?;
        self.bstore.put_keyed(&cid, &buf)?;
        Ok(cid)
    }
    pub fn store_plus_raw(&self, p: Prefix, n: Ipld) -> Result<(Cid, Vec<u8>)> {
        let codec = IpldCodec::try_from(p.codec)?;
        let mut buf = Vec::new();
        Ipld::encode(&n, codec, &mut buf)?;
        let cid = p.to_cid(&buf)?;
        self.bstore.put_keyed(&cid, &buf)?;
        Ok((cid, buf))
    }
}

pub trait BlockLoader {
    fn load_plus_raw(&self, cid: Cid) -> Result<(Ipld, Vec<u8>)>;
}

impl<BS> BlockLoader for LinkSystem<BS>
where
    BS: Blockstore,
{
    fn load_plus_raw(&self, cid: Cid) -> Result<(Ipld, Vec<u8>)> {
        if let Some(blk) = self.bstore.get(&cid)? {
            let codec = IpldCodec::try_from(cid.codec())?;
            let node = codec.decode(&blk)?;
            return Ok((node, blk));
        }
        Err(anyhow!("not found"))
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub struct Prefix {
    pub version: Version,
    pub codec: u64,
    pub mh_type: u64,
    pub mh_len: usize,
}

impl Prefix {
    pub fn new_from_bytes(data: &[u8]) -> Result<Prefix> {
        let mut cur = Cursor::new(data);

        let raw_version: u64 = cur.read_varint()?;
        let codec = cur.read_varint()?;
        let mh_type: u64 = cur.read_varint()?;
        let mh_len: usize = cur.read_varint()?;

        let version = Version::try_from(raw_version)?;

        Ok(Prefix {
            version,
            codec,
            mh_type,
            mh_len,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(4);

        res.write_varint(u64::from(self.version)).unwrap();
        res.write_varint(self.codec).unwrap();
        res.write_varint(self.mh_type).unwrap();
        res.write_varint(self.mh_len).unwrap();

        res
    }

    pub fn to_cid(&self, data: &[u8]) -> Result<Cid> {
        let hash: Multihash = Code::try_from(self.mh_type)?.digest(data);
        let cid = Cid::new(self.version, self.codec, hash)?;
        Ok(cid)
    }
}

impl From<Cid> for Prefix {
    fn from(cid: Cid) -> Self {
        Self {
            version: cid.version(),
            codec: cid.codec(),
            mh_type: cid.hash().code(),
            mh_len: cid.hash().size().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::{Blockstore, MemoryBlockstore};
    use libipld::ipld;

    // same test as go-ipld-prime
    #[test]
    fn setup_from_blockstore() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store);

        let value = ipld!({
            "hello": "world",
        });
        lsys.store(
            Prefix {
                version: Version::V1,
                codec: 0x71,   // dag-cbor
                mh_type: 0x13, // sha-512
                mh_len: 64,
            },
            value.clone(),
        )
        .unwrap();
        let key: Cid = "bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk".parse().unwrap();
        let n = lsys.load(key).unwrap();
        assert_eq!(n, value);
    }
}
