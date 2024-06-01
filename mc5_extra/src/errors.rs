use std::fmt::Display;

use mc5_core::errors::McError;
use serde::{Deserialize, Serialize};

use crate::Mc5;


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Mc5Error {
    pub message: String,
}

impl Display for Mc5Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<McError> for Mc5Error {
    fn from(value: McError) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}