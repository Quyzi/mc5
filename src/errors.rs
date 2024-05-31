use std::time::SystemTimeError;
use flexbuffers::{DeserializationError, ReaderError, SerializationError};
use sled::transaction::{TransactionError, UnabortableTransactionError};
use thiserror::Error;


#[derive(Error, Debug)]
pub enum McError {

    #[error("Time travel is illegal: {0}")]
    Time(#[from] SystemTimeError),

    #[error("Serialization error: {0}")]
    SerdeSer(#[from] SerializationError),

    #[error("Flexbuffer deserialization error: {0}")]
    FlexDe(#[from] DeserializationError),

    #[error("Flexbuffer read error: {0}")]
    FlexRead(#[from] ReaderError),
    
    #[error("Formatting error: {0}")]
    Format(#[from] std::fmt::Error),

    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("Sled transaction error: {0}")]
    SledTx(#[from] TransactionError),

    #[error("Unabortable sled transaction error: {0}")]
    SledUnabortable(#[from] UnabortableTransactionError),
}

