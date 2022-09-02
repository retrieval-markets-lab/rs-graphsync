use anyhow::Result;
use ipld_traversal::{unixfs::unixfs_path_selector, Selector};
use libipld::{Cid, Ipld};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, str::FromStr};
use uuid::Uuid;

pub type RequestId = Uuid;

pub type Priority = i32;

pub type Extensions = BTreeMap<String, Ipld>;

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RequestType {
    #[serde(rename = "n")]
    New,
    #[serde(rename = "c")]
    Cancel,
    #[serde(rename = "u")]
    Update,
}

/// Represents a GraphSync request.
///
/// A GraphSync Request consists of a root CID, an IPLD selector and
/// some optional extensions.
///
/// # Examples
///
/// Creating a GraphSync `Request` with the builder pattern.
///
/// ```
/// use graphsync::Request;
/// use libipld::Cid;
///
/// let root: Cid = "bafybeihq3wo4u27amukm36i7vbpirym4y2lvy53uappzhl3oehcm4ukphu".parse().unwrap();
///
/// let request = Request::builder()
///     .root(root)
///     .build()
///     .unwrap();
/// ```
///
/// Creating a GraphSync `Request` from an IPFS unixfs path, the selector will use a unixfs
/// reifier.
///
/// ```
/// use graphsync::Request;
///
/// let request: Request =
/// "bafybeihq3wo4u27amukm36i7vbpirym4y2lvy53uappzhl3oehcm4ukphu/dir/file.ext".parse().unwrap();
/// ```
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    id: RequestId,

    /// The request's IPLD selector
    #[serde(rename = "sel", skip_serializing_if = "Option::is_none")]
    selector: Option<Selector>,

    /// The request's root CID
    #[serde(skip_serializing_if = "Option::is_none")]
    root: Option<Cid>,

    #[serde(rename = "pri", skip_serializing_if = "Option::is_none")]
    priority: Option<Priority>,

    /// The type of request operation
    #[serde(rename = "type")]
    request_type: RequestType,

    /// The request's extensions
    #[serde(rename = "ext", skip_serializing_if = "Option::is_none")]
    extensions: Option<Extensions>,
}

impl Request {
    pub fn new() -> Request {
        Request {
            id: Uuid::new_v4(),
            selector: None,
            root: None,
            priority: None,
            request_type: RequestType::New,
            extensions: None,
        }
    }

    #[inline]
    pub fn builder() -> Builder {
        Builder::new()
    }

    #[inline]
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    #[inline]
    pub fn req_type(&self) -> &RequestType {
        &self.request_type
    }

    #[inline]
    pub fn root(&self) -> Option<&Cid> {
        self.root.as_ref()
    }

    #[inline]
    pub fn selector(&self) -> Option<&Selector> {
        self.selector.as_ref()
    }

    #[inline]
    pub fn extensions(&self) -> Option<&Extensions> {
        self.extensions.as_ref()
    }

    #[inline]
    pub fn get_extension(&self, key: String) -> Option<&Ipld> {
        self.extensions.as_ref().and_then(|exts| exts.get(&key))
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::new()
    }
}

/// A GraphSync request builder
///
/// This type can be used to construct an instance of `Request`
/// through a builder-like pattern.
#[derive(Debug)]
pub struct Builder {
    inner: Result<Request>,
}

impl Default for Builder {
    #[inline]
    fn default() -> Builder {
        Builder {
            inner: Ok(Request::new()),
        }
    }
}

impl Builder {
    #[inline]
    pub fn new() -> Builder {
        Builder::default()
    }

    /// Set the Root CID for this request.
    ///
    /// # Examples
    ///
    /// ```
    /// use graphsync::Request;
    ///
    /// let req = Request::builder()
    ///     .root("bafybeihq3wo4u27amukm36i7vbpirym4y2lvy53uappzhl3oehcm4ukphu")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn root<T>(self, root: T) -> Builder
    where
        Cid: TryFrom<T>,
        <Cid as TryFrom<T>>::Error: Into<anyhow::Error>,
    {
        self.and_then(move |mut parts| {
            parts
                .root
                .replace(TryFrom::try_from(root).map_err(Into::into)?);
            Ok(parts)
        })
    }

    pub fn selector(self, selector: Selector) -> Builder {
        self.and_then(move |mut parts| {
            parts.selector.replace(selector);
            Ok(parts)
        })
    }

    pub fn extension(self, key: String, value: Ipld) -> Builder {
        self.and_then(move |mut parts| {
            parts
                .extensions
                .get_or_insert_with(Default::default)
                .insert(key, value);
            Ok(parts)
        })
    }

    pub fn build(self) -> Result<Request> {
        self.inner
    }

    fn and_then<F>(self, func: F) -> Self
    where
        F: FnOnce(Request) -> Result<Request>,
    {
        Builder {
            inner: self.inner.and_then(func),
        }
    }
}

impl FromStr for Request {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Some((root, selector)) = unixfs_path_selector(s.into()) {
            Request::builder().root(root).selector(selector).build()
        } else {
            Err(anyhow::format_err!("invalid unixfs path"))
        }
    }
}
