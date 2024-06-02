use crate::{errors::MangoChainsawError, label::Label, mango::MangoChainsaw};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::transaction::UnabortableTransactionError;
use sled::Transactional;
use std::cell::RefCell;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct McBucket {
    parent: MangoChainsaw,
    name: String,

    documents: sled::Tree,
    labels_kev: sled::Tree,
    labels_vek: sled::Tree,
    docs_labels: sled::Tree,
}

impl McBucket {
    /// Create a new Bucket
    #[instrument(skip(parent))]
    pub fn new(parent: &MangoChainsaw, name: &str) -> Result<Self, MangoChainsawError> {
        Ok(Self {
            parent: parent.clone(),
            name: name.to_string(),
            documents: parent.get_tree(&format!("{name}::doc"))?,
            labels_kev: parent.get_tree(&format!("{name}::kev"))?,
            labels_vek: parent.get_tree(&format!("{name}::vek"))?,
            docs_labels: parent.get_tree(&format!("{name}::labels"))?,
        })
    }

    /// Get the current bucket name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a document by id
    #[instrument(skip(self))]
    pub fn get<T>(&self, id: Uuid) -> Result<Option<T>, MangoChainsawError>
    where
        T: DeserializeOwned,
    {
        match self.documents.get(MangoChainsaw::ser(id.as_u64_pair())?) {
            Ok(Some(thing)) => {
                info!("Found object");
                let out: T = MangoChainsaw::de(thing)?;
                info!("Deserialized object");
                Ok(Some(out))
            }
            Ok(None) => {
                info!("Object not found");
                Ok(None)
            }
            Err(e) => {
                error!(error = format!("{e}"), "Failed to lookup object");
                Err(e.into())
            }
        }
    }

    /// Get many documents by id
    #[instrument(skip(self))]
    pub fn get_many<T>(&self, ids: Vec<Uuid>) -> Result<Vec<(Uuid, Option<T>)>, MangoChainsawError>
    where
        T: DeserializeOwned,
    {
        let mut results = vec![];
        for id in &ids {
            results.push((*id, self.get(*id)?));
        }
        Ok(results)
    }

    /// Get labels for a given document id
    #[instrument(skip(self))]
    pub fn get_document_labels(&self, id: Uuid) -> Result<Option<Vec<Label>>, MangoChainsawError> {
        match self.docs_labels.get(MangoChainsaw::ser(id.as_u64_pair())?) {
            Ok(Some(thing)) => {
                let labels: Vec<Label> = MangoChainsaw::de(thing)?;
                info!("Found {} labels for document", labels.len());
                Ok(Some(labels))
            }
            Ok(None) => {
                info!("Document not found or document has no labels");
                Ok(None)
            }
            Err(e) => {
                error!("Error looking up document labels");
                Err(e.into())
            }
        }
    }

    /// Insert a new document with a given set of identifying labels
    #[instrument(skip(self, doc), fields(id))]
    pub fn insert<T>(&self, doc: T, labels: Vec<Label>) -> Result<Uuid, MangoChainsawError>
    where
        T: Serialize,
    {
        let id = self.parent.next_id()?;
        let id_ivec = MangoChainsaw::ser(id.as_u64_pair())?;
        info!(id = id.to_string(), "Preparing document");

        let document = (id_ivec.clone(), MangoChainsaw::ser(&doc)?);
        let doclbl = (id_ivec.clone(), MangoChainsaw::ser(&labels)?);

        let mut all_labels = vec![];
        for label in &labels {
            all_labels.push((label.as_bytes(), label.as_bytes_rev()));
        }
        info!(id = id.to_string(), "Prepared {} labels", all_labels.len());

        (
            &self.documents,
            &self.docs_labels,
            &self.labels_kev,
            &self.labels_vek,
        )
            .transaction(|(docs, docs_labels, kev, vek)| {
                docs.insert(&document.0, &document.1)?;
                docs_labels.insert(&doclbl.0, &doclbl.1)?;
                info!(
                    id = id.to_string(),
                    "Inserted document and document labels pending transaction completion"
                );

                for (label_kev, label_vek) in &all_labels {
                    self.upsert_label(kev, label_kev, id.as_u64_pair())?;
                    self.upsert_label(vek, label_vek, id.as_u64_pair())?;
                    info!(id = id.to_string(), "Upserted label");
                }

                info!(id = id.to_string(), "Transaction complete");
                Ok(())
            })?;

        Ok(id)
    }

    /// Delete a document from the bucket
    #[instrument(skip(self))]
    pub fn delete<T>(&self, id: Uuid) -> Result<Option<T>, MangoChainsawError>
    where
        T: DeserializeOwned,
    {
        let output: RefCell<Option<T>> = RefCell::new(None);
        let idb = MangoChainsaw::ser(id.as_u64_pair())?;
        (
            &self.documents,
            &self.labels_kev,
            &self.labels_vek,
            &self.docs_labels,
        )
            .transaction(|(docs, kev, vek, labels)| {
                info!("deleting document");
                if let Some(raw_doc) = docs.remove(&idb)? {
                    let result: T = MangoChainsaw::de(raw_doc).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    *output.borrow_mut() = Some(result);
                }
                info!("deleting document labels");
                if let Some(raw_labels) = labels.remove(&idb)? {
                    let labels: Vec<Label> = MangoChainsaw::de(raw_labels).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    info!("downserting id from labels");
                    for label in labels {
                        self.downsert_label(kev, &label.as_bytes(), id.as_u64_pair())?;
                        self.downsert_label(vek, &label.as_bytes_rev(), id.as_u64_pair())?;
                    }
                }
                Ok(())
            })?;
        info!("transaction complete");
        Ok(RefCell::into_inner(output))
    }

    /// Get the ID's for all documents matching all given labels
    #[instrument(skip(self), ret)]
    pub fn search_inclusive(&self, labels: Vec<Label>) -> Result<Vec<Uuid>, MangoChainsawError> {
        let mut results = vec![];

        let mut middle = vec![];
        for label in labels {
            match self.labels_kev.get(&label.as_bytes()) {
                Ok(Some(thing)) => {
                    let ids: Vec<(u64, u64)> = MangoChainsaw::de(thing)?;
                    let ids: Vec<Uuid> = ids
                        .into_iter()
                        .map(|id| Uuid::from_u64_pair(id.0, id.1))
                        .collect();
                    info!(
                        label = format!("{label}"),
                        "Found {} ids for label",
                        ids.len()
                    );
                    results.extend(&ids);
                    middle.push(ids);
                }
                Ok(None) => {
                    info!(label = format!("{label}"), "Label does not exist");
                }
                Err(_e) => {
                    error!(label = format!("{label}"), "Error looking up label");
                }
            }
        }
        results.sort();
        results.dedup();

        for list in middle {
            // Keep only the id's that are in all of the labels
            results.retain(|id| list.contains(id))
        }

        Ok(results)
    }

    /// Get all labels matching a given key
    #[instrument(skip(self), ret)]
    pub fn label_name_search(&self, key: &str) -> Result<Vec<Label>, MangoChainsawError> {
        let mut results = vec![];
        for result in self.labels_kev.scan_prefix(key) {
            let (key, val) = result?;
            info!(
                label = format!("{key:?}"),
                value = format!("{val:?}"),
                "Found label with prefix"
            );
            let label = Label::from_bytes(&key)?;
            results.push(label);
        }
        Ok(results)
    }

    /// Get all labels matching a given value
    #[instrument(skip(self), ret)]
    pub fn label_value_search(&self, value: &str) -> Result<Vec<Label>, MangoChainsawError> {
        let mut results = vec![];
        for result in self.labels_vek.scan_prefix(value) {
            let (key, val) = result?;
            info!(
                label = format!("{key:?}"),
                value = format!("{val:?}"),
                "Found label with prefix"
            );
            let mut label = Label::from_bytes(&key)?;
            label.swap_key_value();
            results.push(label);
        }
        Ok(results)
    }

    /// Get all document id's with a given label
    #[instrument(skip(self), ret)]
    pub fn get_label(&self, label: Label) -> Result<Option<Vec<Uuid>>, MangoChainsawError> {
        match self.labels_kev.get(label.as_bytes())? {
            Some(raw_labels) => {
                let ids: Vec<(u64, u64)> = MangoChainsaw::de(raw_labels)?;
                Ok(Some(
                    ids.into_iter()
                        .map(|id| Uuid::from_u64_pair(id.0, id.1))
                        .collect(),
                ))
            }
            None => Ok(None),
        }
    }

    /// Add labels to an existing document
    #[instrument(skip(self), ret)]
    pub fn add_document_labels(&self, id: Uuid, labels: Vec<Label>) -> Result<(), MangoChainsawError> {
        let idbytes = MangoChainsaw::ser(id.as_u64_pair())?;
        (&self.labels_kev, &self.labels_vek, &self.docs_labels).transaction(
            |(kev, vek, doc_labels)| {
                // Update the docs_labels tree with the new labels
                if let Some(raw_labels) = doc_labels.remove(&idbytes)? {
                    let mut has_labels: Vec<Label> = MangoChainsaw::de(raw_labels).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    has_labels.extend(labels.clone());
                    has_labels.sort();
                    has_labels.dedup();
                    let new = MangoChainsaw::ser(has_labels).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    doc_labels.insert(&idbytes, new)?;
                }

                // Upsert each new label
                for label in &labels {
                    self.upsert_label(kev, &label.as_bytes(), id.as_u64_pair())?;
                    self.upsert_label(vek, &label.as_bytes_rev(), id.as_u64_pair())?;
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    /// Remove labels from a document
    #[instrument(skip(self), ret)]
    pub fn remove_document_labels(&self, id: Uuid, labels: Vec<Label>) -> Result<(), MangoChainsawError> {
        let idbytes = MangoChainsaw::ser(id.as_u64_pair())?;
        (&self.labels_kev, &self.labels_vek, &self.docs_labels).transaction(
            |(kev, vek, doc_labels)| {
                // Update the docs_labels tree with the labels removed
                if let Some(raw_labels) = doc_labels.remove(&idbytes)? {
                    let mut has_labels: Vec<Label> = MangoChainsaw::de(raw_labels).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    has_labels.retain(|l| !labels.contains(l));
                    has_labels.sort();
                    has_labels.dedup();
                    let new = MangoChainsaw::ser(has_labels).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    doc_labels.insert(&idbytes, new)?;
                }

                // Downsert each new label
                for label in &labels {
                    self.downsert_label(kev, &label.as_bytes(), id.as_u64_pair())?;
                    self.downsert_label(vek, &label.as_bytes_rev(), id.as_u64_pair())?;
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    /// Insert a new label or update existing labels with a new document id
    #[instrument(skip(self, t), fields(labels, new))]
    fn upsert_label(
        &self,
        t: &sled::transaction::TransactionalTree,
        k: &[u8],
        id: (u64, u64),
    ) -> Result<(), UnabortableTransactionError> {
        info!("Upserting label");
        match t.get(k) {
            Ok(Some(current)) => {
                info!("Label already exists, updating references");
                let mut docs: Vec<(u64, u64)> = MangoChainsaw::de(current).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                docs.push(id);
                docs.sort();
                docs.dedup();
                let new = MangoChainsaw::ser(docs).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                let _ = t.insert(k, new)?;
                Ok(())
            }
            Ok(None) => {
                info!("Label does not exist, creating");
                let new = MangoChainsaw::ser(vec![id]).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                let _ = t.insert(k, new)?;
                Ok(())
            }
            Err(e) => {
                error!("Label failed to upsert");
                Err(UnabortableTransactionError::Storage(
                    sled::Error::ReportableBug(e.to_string()),
                ))
            }
        }
    }

    /// downsert an id from a label. This will remove the label if it is unused
    #[instrument(skip(self, t))]
    fn downsert_label(
        &self,
        t: &sled::transaction::TransactionalTree,
        k: &[u8],
        id: (u64, u64),
    ) -> Result<(), UnabortableTransactionError> {
        match t.get(k) {
            Ok(Some(raw_labels)) => {
                info!("Found label");
                let mut ids: Vec<(u64, u64)> = MangoChainsaw::de(raw_labels).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                if ids.len() == 1 {
                    info!("Label has only one item, deleting");
                    t.remove(k)?;
                } else {
                    ids.retain(|i| i.0 != id.0 && i.1 != id.1);
                    t.insert(
                        k,
                        MangoChainsaw::ser(ids).map_err(|e| {
                            UnabortableTransactionError::Storage(sled::Error::ReportableBug(
                                e.to_string(),
                            ))
                        })?,
                    )?;
                }
                Ok(())
            }
            Ok(None) => {
                warn!("Label did not exist to downsert");
                Ok(())
            }
            Err(e) => {
                error!("Label failed to downsert");
                Err(UnabortableTransactionError::Storage(
                    sled::Error::ReportableBug(e.to_string()),
                ))
            }
        }
    }

    /// Drop this bucket, deleting all of its documents and labels.
    /// This can't be undone.
    #[instrument(skip(self))]
    pub fn drop_bucket(&self) -> Result<(), MangoChainsawError> {
        let name = &self.name;
        self.parent.db.drop_tree(format!("{name}::doc"))?;
        self.parent.db.drop_tree(format!("{name}::kev"))?;
        self.parent.db.drop_tree(format!("{name}::vek"))?;
        self.parent.db.drop_tree(format!("{name}::labels"))?;
        Ok(())
    }
}
