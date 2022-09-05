use crate::stream::JsStream;
use futures::StreamExt;
use graphsync::{resolver::resolve_raw_bytes, GraphSync, Request};
use ipld_traversal::{
    blockstore::MemoryBlockstore, unixfs::unixfs_path_selector, LinkSystem, Prefix,
};
use js_sys::{AsyncIterator, Promise, Uint8Array};
use libipld::{ipld, Ipld};
use libp2p::{
    core::muxing::StreamMuxerBox,
    core::transport::{Boxed, Transport},
    core::upgrade,
    identity, mplex, noise, Multiaddr, PeerId, Swarm,
};
use libp2p_wasm_ws::WsTransport;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

mod stream;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestParams {
    pub maddress: String,
    pub peer_id: String,
    pub cid: String,
}

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

    #[wasm_bindgen]
    pub fn request(&self, js_params: JsValue) -> Result<Promise, JsValue> {
        let params: RequestParams = js_params.into_serde().map_err(js_err)?;

        let maddr: Multiaddr = params.maddress.parse().map_err(js_err)?;
        let peer_id = PeerId::from_str(&params.peer_id).map_err(js_err)?;

        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());

        let transport = build_transport(local_key);

        let mut swarm = Swarm::new(transport, GraphSync::new(self.store.clone()), local_peer_id);

        let (root, selector) =
            unixfs_path_selector(params.cid).ok_or(JsValue::from_str("invalid unixfs path"))?;
        let req = Request::builder()
            .root(root)
            .selector(selector)
            .build()
            .map_err(js_err)?;

        let gs = swarm.behaviour_mut();
        gs.add_address(&peer_id, maddr);

        let req_id = *req.id();
        gs.request(peer_id, req);

        Ok(future_to_promise(async move {
            let _ = resolve_raw_bytes(req_id, swarm.by_ref()).await;
            Ok(JsValue::undefined())
        }))
    }
}

fn js_err<E: ToString + Send + Sync + 'static>(e: E) -> JsValue {
    JsValue::from_str(&e.to_string())
}

pub fn build_transport(local_key: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&local_key)
            .expect("Noise key generation failed");

        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };

    let mut mplex_config = mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(usize::MAX);

    WsTransport
        .upgrade(upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}
