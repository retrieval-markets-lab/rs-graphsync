use libipld::ipld::IpldIndex;
use std::fmt;

/// Represents either a key in a map or an index in a list.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathSegment {
    /// Key in a map
    String(String),
    /// Index in a list
    Int(usize),
}

impl PathSegment {
    pub fn ipld_index<'a>(&self) -> IpldIndex<'a> {
        match self {
            PathSegment::String(string) => IpldIndex::Map(string.clone()),
            PathSegment::Int(int) => IpldIndex::List(*int),
        }
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Int(i)
    }
}

impl From<String> for PathSegment {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for PathSegment {
    fn from(s: &str) -> Self {
        match s.parse::<usize>() {
            Ok(u) => PathSegment::Int(u),
            Err(_) => PathSegment::String(s.to_owned()),
        }
    }
}

impl fmt::Display for PathSegment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PathSegment::String(s) => write!(f, "{}", s),
            PathSegment::Int(i) => write!(f, "{}", i),
        }
    }
}
