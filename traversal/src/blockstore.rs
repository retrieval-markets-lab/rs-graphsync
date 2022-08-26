use anyhow::Result;
use libipld::{
    cid::multihash::{self, MultihashDigest},
    Cid,
};
use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
};

// Same block/blockstore implementation as the fvm.
// Will import from there once it gets published separately.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Block<D>
where
    D: AsRef<[u8]> + ?Sized,
{
    pub codec: u64,
    pub data: D,
}

impl<D> Block<D>
where
    D: AsRef<[u8]> + ?Sized,
{
    pub fn new(codec: u64, data: D) -> Self
    where
        Self: Sized,
        D: Sized,
    {
        Self { codec, data }
    }

    pub fn cid(&self, mh_code: multihash::Code) -> Cid {
        Cid::new_v1(self.codec, mh_code.digest(self.data.as_ref()))
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().len()
    }
}

impl<D> AsRef<[u8]> for Block<D>
where
    D: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl<'a, D> From<&'a Block<D>> for Block<&'a [u8]>
where
    D: AsRef<[u8]>,
{
    fn from(b: &'a Block<D>) -> Self {
        Block {
            codec: b.codec,
            data: b.data.as_ref(),
        }
    }
}

/// An IPLD blockstore suitable for injection into the FVM.
///
/// The cgo blockstore adapter implements this trait.
pub trait Blockstore {
    /// Gets the block from the blockstore.
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>>;

    /// Put a block with a pre-computed cid.
    ///
    /// If you don't yet know the CID, use put. Some blockstores will re-compute the CID internally
    /// even if you provide it.
    ///
    /// If you _do_ already know the CID, use this method as some blockstores _won't_ recompute it.
    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()>;

    /// Checks if the blockstore has the specified block.
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.get(k)?.is_some())
    }

    /// Puts the block into the blockstore, computing the hash with the specified multicodec.
    ///
    /// By default, this defers to put.
    fn put<D>(&self, mh_code: multihash::Code, block: &Block<D>) -> Result<Cid>
    where
        Self: Sized,
        D: AsRef<[u8]>,
    {
        let k = block.cid(mh_code);
        self.put_keyed(&k, block.as_ref())?;
        Ok(k)
    }

    /// Bulk put blocks into the blockstore.
    fn put_many<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (multihash::Code, Block<D>)>,
    {
        self.put_many_keyed(blocks.into_iter().map(|(mc, b)| (b.cid(mc), b)))?;
        Ok(())
    }

    /// Bulk-put pre-keyed blocks into the blockstore.
    ///
    /// By default, this defers to put_keyed.
    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        for (c, b) in blocks {
            self.put_keyed(&c, b.as_ref())?
        }
        Ok(())
    }
}

impl<BS> Blockstore for &BS
where
    BS: Blockstore,
{
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (*self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (*self).put_keyed(k, block)
    }

    fn has(&self, k: &Cid) -> Result<bool> {
        (*self).has(k)
    }

    fn put<D>(&self, mh_code: multihash::Code, block: &Block<D>) -> Result<Cid>
    where
        Self: Sized,
        D: AsRef<[u8]>,
    {
        (*self).put(mh_code, block)
    }

    fn put_many<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (multihash::Code, Block<D>)>,
    {
        (*self).put_many(blocks)
    }

    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        (*self).put_many_keyed(blocks)
    }
}

impl<BS> Blockstore for Rc<BS>
where
    BS: Blockstore,
{
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        (**self).get(k)
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        (**self).put_keyed(k, block)
    }

    fn has(&self, k: &Cid) -> Result<bool> {
        (**self).has(k)
    }

    fn put<D>(&self, mh_code: multihash::Code, block: &Block<D>) -> Result<Cid>
    where
        Self: Sized,
        D: AsRef<[u8]>,
    {
        (**self).put(mh_code, block)
    }

    fn put_many<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (multihash::Code, Block<D>)>,
    {
        (**self).put_many(blocks)
    }

    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        (**self).put_many_keyed(blocks)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemoryBlockstore {
    blocks: Arc<Mutex<HashMap<Cid, Vec<u8>>>>,
}

impl MemoryBlockstore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Blockstore for MemoryBlockstore {
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.blocks.lock().unwrap().contains_key(k))
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.lock().unwrap().get(k).cloned())
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        self.blocks.lock().unwrap().insert(*k, block.into());
        Ok(())
    }
}
