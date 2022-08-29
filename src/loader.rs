use ipld_traversal::{blockstore::Blockstore, IpldLoader};
use libipld::{codec::Codec, codec_impl::IpldCodec, Cid, Ipld};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Clone)]
pub struct ReconciledLoader<BS> {
    bstore: BS,
    state: Rc<RefCell<LoaderState>>,
}

struct LoaderState {
    queue: VecDeque<(Cid, Vec<u8>)>,
    online: bool,
}

impl LoaderState {
    fn new() -> Self {
        LoaderState {
            queue: VecDeque::new(),
            online: false,
        }
    }
}

impl Default for LoaderState {
    fn default() -> Self {
        LoaderState::new()
    }
}

impl<BS> ReconciledLoader<BS>
where
    BS: Blockstore,
{
    pub fn new(bstore: BS) -> Self {
        ReconciledLoader {
            bstore,
            state: Default::default(),
        }
    }

    pub fn injest(&self, k: Cid, block: Vec<u8>) {
        self.state.borrow_mut().queue.push_back((k, block));
    }

    pub fn set_online(&mut self, online: bool) {
        self.state.borrow_mut().online = online;
    }
}

impl<BS> IpldLoader for ReconciledLoader<BS>
where
    BS: Blockstore,
{
    fn load(&self, cid: Cid) -> anyhow::Result<Ipld> {
        let codec = IpldCodec::try_from(cid.codec())?;
        let mut state = self.state.borrow_mut();
        if state.online {
            if let Some((key, blk)) = state.queue.pop_front() {
                if key != cid {
                    return Err(anyhow::format_err!("invalid block"));
                }
                self.bstore.put_keyed(&key, &blk[..])?;
                let node = codec.decode(&blk)?;
                return Ok(node);
            }
        }
        if let Some(blk) = self.bstore.get(&cid)? {
            let node = codec.decode(&blk)?;
            return Ok(node);
        }
        Err(anyhow::format_err!("not found"))
    }
}
