use crate::empty_map;
use crate::path_segment::PathSegment;
use indexmap::IndexMap;
use libipld::error::SerdeError;
use libipld::serde::{from_ipld, to_ipld};
use libipld::Ipld;
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_slice, to_vec, Error as CborError};
use Selector::*;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Selector {
    #[serde(rename = "a")]
    ExploreAll {
        #[serde(rename = ">")]
        next: Box<Selector>,
    },
    #[serde(rename = "R")]
    ExploreRecursive {
        #[serde(rename = "l")]
        limit: RecursionLimit,
        #[serde(rename = ":>")]
        sequence: Box<Selector>,
        #[serde(skip)]
        /// Used to index current
        current: Option<Box<Selector>>,
    },
    #[serde(rename = "@", with = "empty_map")]
    ExploreRecursiveEdge,
    #[serde(rename = "f")]
    ExploreFields {
        #[serde(rename = "f>")]
        fields: IndexMap<String, Selector>,
    },
    #[serde(rename = "~")]
    ExploreInterpretAs {
        #[serde(rename = "as")]
        reifier: String,
        #[serde(rename = ">")]
        next: Box<Selector>,
    },
    #[serde(rename = ".", with = "empty_map")]
    Matcher,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
pub enum RecursionLimit {
    #[serde(rename = "none", with = "empty_map")]
    None,
    #[serde(rename = "depth")]
    Depth(u64),
}

impl Selector {
    pub fn interests(&self) -> Option<Vec<PathSegment>> {
        match self {
            ExploreAll { .. } => None,
            ExploreFields { fields } => Some(
                fields
                    .keys()
                    .map(|k| PathSegment::from(k.to_string()))
                    .collect(),
            ),
            ExploreRecursive {
                current, sequence, ..
            } => {
                if let Some(selector) = current {
                    selector.interests()
                } else {
                    sequence.interests()
                }
            }
            ExploreRecursiveEdge => {
                // Should never be called on this variant
                Some(vec![])
            }
            ExploreInterpretAs { next, .. } => next.interests(),
            Matcher => Some(vec![]),
        }
    }
    pub fn explore(self, ipld: &Ipld, p: &PathSegment) -> Option<Selector> {
        match self {
            ExploreAll { next } => Some(*next),
            ExploreFields { mut fields } => {
                ipld.get(p.ipld_index()).ok()?;
                fields.remove(&p.to_string())
            }
            ExploreRecursive {
                current,
                sequence,
                limit,
            } => {
                let next = current
                    .unwrap_or_else(|| sequence.clone())
                    .explore(ipld, p)?;

                if !has_recursive_edge(&next) {
                    return Some(ExploreRecursive {
                        sequence,
                        current: Some(next.into()),
                        limit,
                    });
                }

                match limit {
                    RecursionLimit::Depth(depth) => {
                        if depth < 2 {
                            return replace_recursive_edge(next, None);
                        }
                        Some(ExploreRecursive {
                            current: replace_recursive_edge(next, Some(*sequence.clone()))
                                .map(Box::new),
                            sequence,
                            limit: RecursionLimit::Depth(depth - 1),
                        })
                    }
                    RecursionLimit::None => Some(ExploreRecursive {
                        current: replace_recursive_edge(next, Some(*sequence.clone()))
                            .map(Box::new),
                        sequence,
                        limit,
                    }),
                }
            }
            ExploreRecursiveEdge => None,
            ExploreInterpretAs { next, .. } => Some(*next),
            Matcher => None,
        }
    }
    pub fn decide(&self) -> bool {
        match self {
            Matcher => true,
            ExploreRecursive {
                current, sequence, ..
            } => {
                if let Some(curr) = current {
                    curr.decide()
                } else {
                    sequence.decide()
                }
            }
            _ => false,
        }
    }
    /// Marshalls selector into cbor bytes
    pub fn marshal_cbor(&self) -> Result<Vec<u8>, CborError> {
        to_vec(&self)
    }

    /// Unmarshals cbor encoded bytes to selector
    pub fn unmarshal_cbor(bz: &[u8]) -> Result<Self, CborError> {
        from_slice(bz)
    }
}

fn has_recursive_edge(next_sel: &Selector) -> bool {
    matches!(next_sel, ExploreRecursiveEdge { .. })
}

fn replace_recursive_edge(next_sel: Selector, replace: Option<Selector>) -> Option<Selector> {
    match next_sel {
        ExploreRecursiveEdge => replace,
        _ => Some(next_sel),
    }
}

impl TryFrom<Selector> for Ipld {
    type Error = SerdeError;

    fn try_from(value: Selector) -> Result<Self, Self::Error> {
        to_ipld(value)
    }
}

impl TryFrom<Ipld> for Selector {
    type Error = SerdeError;

    fn try_from(value: Ipld) -> Result<Self, Self::Error> {
        from_ipld(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cbor::DagCborCodec;
    use libipld::codec::Codec;

    #[test]
    fn encoding() {
        let sel_data = hex::decode("a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0").unwrap();
        let selector = Selector::unmarshal_cbor(&sel_data).unwrap();

        // verify that we encode it to the same output.
        let out = selector.marshal_cbor().unwrap();
        assert_eq!(out, sel_data);

        let bytes = DagCborCodec
            .encode::<Ipld>(&selector.try_into().unwrap())
            .unwrap();
        assert_eq!(bytes, sel_data);
    }
}
