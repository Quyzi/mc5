pub mod errors;
pub mod server;

use errors::Mc5Error;
use mc5_core::label::Label;

#[tarpc::service]
pub trait Mc5 {
    async fn bucket_names() -> Result<Vec<String>, Mc5Error>;
    async fn drop_bucket(bucket: String) -> Result<(), Mc5Error>;
    
    async fn insert_document(bucket: String, document: Vec<u8>, labels: Vec<Label>) -> Result<u128, Mc5Error>;
    async fn get_document(bucket: String, id: u128) -> Result<Option<Vec<u8>>, Mc5Error>;
    async fn delete_document(bucket: String, id: u128) -> Result<Option<Vec<u8>>, Mc5Error>;

    async fn get_document_labels(bucket: String, id: u128) -> Result<Vec<Label>, Mc5Error>;
    async fn add_document_labels(bucket: String, id: u128, labels: Vec<Label>) -> Result<Vec<Label>, Mc5Error>;
    async fn remove_document_labels(bucket: String, id: u128, labels: Vec<Label>) -> Result<Vec<Label>, Mc5Error>;

    async fn search_docs_exact(bucket: String, labels: Vec<Label>) -> Result<Vec<u128>, Mc5Error>;
    
    async fn get_label_docs(bucket: String, label: Label) -> Result<Vec<u128>, Mc5Error>;
    async fn get_labels_by_key(bucket: String, key: String) -> Result<Vec<Label>, Mc5Error>;
    async fn get_labels_by_val(bucket: String, val: String) -> Result<Vec<Label>, Mc5Error>;
}

