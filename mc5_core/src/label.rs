use crate::errors::McError;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Label {
    key: String,
    value: String,
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

impl Label {
    pub fn new(k: &str, v: &str) -> Self {
        Self {
            key: k.to_string(),
            value: v.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn swap_key_value(&mut self) {
        std::mem::swap(&mut self.key, &mut self.value)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        format!("{}={}", self.key(), self.value())
            .as_bytes()
            .to_vec()
    }

    pub fn as_bytes_rev(&self) -> Vec<u8> {
        format!("{}={}", self.value(), self.key())
            .as_bytes()
            .to_vec()
    }

    pub fn from_bytes(s: &[u8]) -> Result<Self, McError> {
        let s = std::str::from_utf8(s)?;
        if let Some((lhs, rhs)) = s.split_once('=') {
            Ok(Self::new(lhs, rhs))
        } else {
            Err(McError::Etc(format!("invalid label {s}")))
        }
    }
}

#[macro_export]
macro_rules! mclabel {
    ($k:expr => $v:expr) => {{
        Label::new($k, $v)
    }};
}

#[macro_export]
macro_rules! mclabels {
    ($k:expr => $v:expr) => (vec![mclabel!($k => $v)]);

    ($k:expr => $v:expr, $($kk:expr => $vv:expr),+) => {{
        let mut labels = vec![];
        labels.push(mclabel!($k => $v));
        $(labels.push(mclabel!($kk => $vv));)*
        labels
    }}
}
