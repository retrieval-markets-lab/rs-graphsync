use crate::request::{Extensions, RequestId};
use anyhow::Result;
use libipld::{Cid, Ipld};
use serde::{Deserialize, Serialize};
use serde_repr::*;
use serde_tuple::*;

/// Represents a GraphSync block.
///
/// A GraphSync block consists of an encoded prefix and byte data.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct Block {
    #[serde(with = "serde_bytes")]
    pub prefix: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct LinkData {
    pub link: Cid,
    pub action: LinkAction,
}

impl LinkData {
    pub fn present(link: Cid) -> LinkData {
        LinkData {
            link,
            action: LinkAction::Present,
        }
    }
    pub fn missing(link: Cid) -> LinkData {
        LinkData {
            link,
            action: LinkAction::Missing,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkAction {
    #[serde(rename = "p")]
    Present,
    #[serde(rename = "d")]
    DuplicateNotSent,
    #[serde(rename = "m")]
    Missing,
    #[serde(rename = "s")]
    DuplicateDAGSkipped,
}

#[derive(PartialEq, Clone, Copy, Eq, Debug, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum StatusCode {
    RequestAcknowledged = 10,
    PartialResponse = 14,
    RequestPaused = 15,
    RequestCompletedFull = 20,
    RequestCompletedPartial = 21,
    RequestRejected = 30,
    RequestFailedBusy = 31,
    RequestFailedUnknown = 32,
    RequestFailedLegal = 33,
    RequestFailedContentNotFound = 34,
    RequestCancelled = 35,
}

impl TryFrom<i32> for StatusCode {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            10 => Ok(StatusCode::RequestAcknowledged),
            14 => Ok(StatusCode::PartialResponse),
            15 => Ok(StatusCode::RequestPaused),
            20 => Ok(StatusCode::RequestCompletedFull),
            21 => Ok(StatusCode::RequestCompletedPartial),
            30 => Ok(StatusCode::RequestRejected),
            31 => Ok(StatusCode::RequestFailedBusy),
            32 => Ok(StatusCode::RequestFailedUnknown),
            33 => Ok(StatusCode::RequestFailedLegal),
            34 => Ok(StatusCode::RequestFailedContentNotFound),
            35 => Ok(StatusCode::RequestCancelled),
            _ => Err(anyhow::anyhow!("unknown status code")),
        }
    }
}

/// Represents a GraphSync response.
///
/// A GraphSync response consists of params and a stream of messages containing blocks
/// and status updates.
///
/// # Examples
///
/// Creating a `Response` to return
///
/// ```
/// use graphsync::{Request, Response, StatusCode};
///
/// fn respond_to(req: Request) -> Result<Response, String> {
///     Response::builder(*req.id(), StatusCode::PartialResponse)
///         .build()
///         .map_err(|e| e.to_string())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    /// The response's extensions attached to the first message.
    #[serde(rename = "ext", skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Extensions>,

    /// The link metadata for each block attached to this response.
    #[serde(rename = "meta", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<LinkData>>,

    /// The response's status code attached to the first message. The status code
    /// can be updated if something happen while the blocks are streamed.
    #[serde(rename = "stat")]
    pub status: StatusCode,

    /// The request ID this response is for
    #[serde(rename = "reqid")]
    pub id: RequestId,
}

impl Response {
    pub fn new(id: RequestId, status: StatusCode) -> Response {
        Response {
            extensions: None,
            metadata: None,
            status,
            id,
        }
    }
    /// Creates a new builder-style object to manufacture a `Response`.
    /// A request ID and status code must be provided.
    #[inline]
    pub fn builder(id: RequestId, status: StatusCode) -> Builder {
        Builder::new(id, status)
    }
}

/// A GraphSync response builder
///
/// This type can be used to construct an instance of `Response` through a
/// builer-like pattern.
#[derive(Debug)]
pub struct Builder {
    inner: Result<Response>,
}

impl Builder {
    #[inline]
    pub fn new(id: RequestId, status: StatusCode) -> Builder {
        Builder {
            inner: Ok(Response::new(id, status)),
        }
    }

    /// Update the status code of the staged response object.
    pub fn status(self, code: StatusCode) -> Builder {
        self.and_then(move |mut parts| {
            parts.status = code;
            Ok(parts)
        })
    }

    /// Insert extension data in this response for the given key.
    pub fn extension(self, key: String, value: Ipld) -> Builder {
        self.and_then(move |mut parts| {
            parts
                .extensions
                .get_or_insert_with(Default::default)
                .insert(key, value);
            Ok(parts)
        })
    }

    /// Insert link action for a given CID.
    pub fn metadata(self, link: Cid, action: LinkAction) -> Builder {
        self.and_then(move |mut parts| {
            parts
                .metadata
                .get_or_insert_with(Default::default)
                .push(LinkData { link, action });
            Ok(parts)
        })
    }

    /// Directly set metadata vec.
    pub fn set_metadata(self, data: Vec<LinkData>) -> Builder {
        self.and_then(move |mut parts| {
            parts.metadata = Some(data);
            Ok(parts)
        })
    }

    /// "Consumes" this builder to return a constructed `Response`.
    ///
    /// # Errors
    ///
    /// This function may return an error if any of the previously configured argument
    /// failed to parse of get converted to the internal representation.
    ///
    pub fn build(self) -> Result<Response> {
        self.inner
    }

    fn and_then<F>(self, func: F) -> Self
    where
        F: FnOnce(Response) -> Result<Response>,
    {
        Builder {
            inner: self.inner.and_then(func),
        }
    }
}
