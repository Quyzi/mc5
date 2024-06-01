use mc5_core::mc::Mc;
use mc5_core::label::Label;
use tracing::instrument;
use uuid::Uuid;
use crate::Mc5Error;
use crate::Mc5;


pub struct Mc5Server {
    db: Mc,
}

impl Mc5 for Mc5Server {
    #[instrument(skip(self))]
    async fn bucket_names(self, _context: tarpc::context::Context) -> Result<Vec<String> ,Mc5Error>  {
        let names = self.db.list_buckets()?;
        Ok(names)
    }

    #[instrument(skip(self))]
    async fn drop_bucket(self, _context: tarpc::context::Context, bucket: String) -> Result<(),Mc5Error>  {
        self.db.drop_bucket(&bucket)?;
        Ok(())
    }

    #[instrument(skip(self, document))]
    async fn insert_document(self, _context: tarpc::context::Context, bucket: String, document: Vec<u8>, labels: Vec<Label>) -> Result<u128,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let id = bucket.insert(document, labels)?;
        Ok(id.as_u128())
    }

    #[instrument(skip(self))]
    async fn get_document(self, _context: tarpc::context::Context, bucket: String, id: u128) -> Result<Option<Vec<u8> > ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let res = bucket.get(Uuid::from_u128(id))?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn delete_document(self, _context: tarpc::context::Context, bucket: String, id: u128) -> Result<Option<Vec<u8>> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let res = bucket.delete(Uuid::from_u128(id))?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn get_document_labels(self, _context: tarpc::context::Context, bucket: String, id: u128) -> Result<Vec<Label> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        if let Some(labels) = bucket.get_document_labels(Uuid::from_u128(id))? {
            Ok(labels)
        } else {
            Ok(vec![])
        }
    }

    #[instrument(skip(self))]
    async fn get_label_docs(self, _context: tarpc::context::Context, bucket: String, label: Label) -> Result<Vec<u128> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        if let Some(ids) = bucket.get_label(label)? {
            Ok(ids.into_iter().map(|id| id.as_u128()).collect())
        } else {
            Ok(vec![])
        }
    }

    #[instrument(skip(self))]
    async fn add_document_labels(self, _context: tarpc::context::Context, bucket:String, id: u128, labels: Vec<Label>) -> Result<Vec<Label> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        bucket.add_document_labels(Uuid::from_u128(id), labels)?;
        if let Some(labels) = bucket.get_document_labels(Uuid::from_u128(id))? {
            Ok(labels)
        } else {
            Ok(vec![])
        }
    }

    #[instrument(skip(self))]
    async fn remove_document_labels(self, _context: tarpc::context::Context, bucket: String, id: u128, labels: Vec<Label>) -> Result<Vec<Label> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        bucket.remove_document_labels(Uuid::from_u128(id), labels)?;
        if let Some(labels) = bucket.get_document_labels(Uuid::from_u128(id))? {
            Ok(labels)
        } else {
            Ok(vec![])
        }
    }

    #[instrument(skip(self))]
    async fn search_docs_exact(self, _context: tarpc::context::Context, bucket: String, labels: Vec<Label>) -> Result<Vec<u128> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let ids = bucket.search_inclusive(labels)?;
        Ok(ids.into_iter().map(|id| id.as_u128()).collect())
    }

    #[instrument(skip(self))]
    async fn get_labels_by_key(self, _context: tarpc::context::Context, bucket: String, key: String) -> Result<Vec<Label> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let labels = bucket.label_name_search(&key)?;
        Ok(labels)
    }

    #[instrument(skip(self))]
    async fn get_labels_by_val(self, _context: tarpc::context::Context, bucket: String, val: String) -> Result<Vec<Label> ,Mc5Error>  {
        let bucket = self.db.get_bucket(&bucket)?;
        let labels = bucket.label_value_search(&val)?;
        Ok(labels)
    }
}