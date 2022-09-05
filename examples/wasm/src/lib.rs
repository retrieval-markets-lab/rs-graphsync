use crate::stream::JsStream;
use futures::StreamExt;
use ipld_traversal::{blockstore::MemoryBlockstore, LinkSystem, Prefix};
use js_sys::{AsyncIterator, Promise, Uint8Array};
use libipld::{ipld, Ipld};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

mod stream;

#[wasm_bindgen]
pub struct Client {
    store: MemoryBlockstore,
}

#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            store: MemoryBlockstore::new(),
        }
    }

    #[wasm_bindgen]
    pub fn import_content(&self, content: AsyncIterator) -> Result<Promise, JsValue> {
        let store = self.store.clone();
        let mut reader = JsStream::from(content);

        Ok(future_to_promise(async move {
            let lsys = LinkSystem::new(store);
            let mut links = Vec::new();
            while let Some(result) = reader.next().await {
                match result {
                    Ok(value) => {
                        let arr: Uint8Array = value.into();
                        let cid = lsys
                            .store(Prefix::new(0x55, 0x12), &Ipld::Bytes(arr.to_vec()))
                            .expect("link system to store chunk");
                        links.push(ipld!({
                            "Hash": cid,
                            "Tsize": arr.length(),
                        }));
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            let root_node = ipld!({
                "Links": links,
            });
            let root = lsys
                .store(Prefix::new(0x71, 0x12), &root_node)
                .expect("link system to store root node");
            Ok(JsValue::from_str(&(String::from(root))))
        }))
    }
}
