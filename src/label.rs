use std::fmt::Display;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::errors::McError;


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
            key: format!("{k}"),
            value: format!("{v}")
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn as_kev_key(&self) -> Result<IVec, McError> {
        Ok(IVec::from(format!("{}={}", self.key(), self.value()).as_bytes()))
    }

    pub(crate) fn as_vek_key(&self) -> Result<IVec, McError> {
        Ok(IVec::from(format!("{}={}", self.value(), self.key()).as_bytes()))
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