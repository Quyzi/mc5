use crate::{errors::McError, label::Label, mc::Mc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::Transactional;
use sled::{
    transaction::UnabortableTransactionError,
    IVec,
};
use tracing::{error, info, instrument};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct McBucket {
    parent: Mc,
    name: String,

    documents: sled::Tree,
    labels_kev: sled::Tree,
    labels_vek: sled::Tree,
    docs_labels: sled::Tree,
}

impl McBucket {
    #[instrument(skip(parent))]
    pub fn new(parent: &Mc, name: &str) -> Result<Self, McError> {
        Ok(Self {
            parent: parent.clone(),
            name: format!("{name}"),
            documents: parent.get_tree(&format!("{name}::doc"))?,
            labels_kev: parent.get_tree(&format!("{name}::kev"))?,
            labels_vek: parent.get_tree(&format!("{name}::vek"))?,
            docs_labels: parent.get_tree(&format!("{name}::labels"))?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[instrument(skip(self, doc), fields(id))]
    pub fn insert<T>(&self, doc: T, labels: Vec<Label>) -> Result<Uuid, McError> 
    where T: Serialize {
        let id = self.parent.next_id()?;
        let id_ivec = IVec::from(id.as_bytes());
        info!(id = id.to_string(), "Preparing document");

        let document = (id_ivec.clone(), Mc::ser(&doc)?);
        let doclbl = (id_ivec.clone(), Mc::ser(&labels)?);

        let mut all_labels = vec![];
        for label in labels {
            all_labels.push((label.as_kev_key()?, label.as_vek_key()?));
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
                info!(id = id.to_string(), "Inserted document and document labels pending transaction completion");

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

    #[instrument(skip(self))]
    pub fn get<T>(&self, key: Uuid) -> Result<Option<T>, McError>
    where T: DeserializeOwned {
        match self.documents.get(&key) {
            Ok(Some(thing)) => {
                info!("Found object");
                let out: T = Mc::de(thing)?;
                info!("Deserialized object");
                Ok(Some(out))
            },
            Ok(None) => {
                info!("Object not found");
                Ok(None)
            },
            Err(e) => {
                error!(error = format!("{e}"), "Failed to lookup object");
                Err(e.into())
            }
        }
    }

    #[instrument(skip(self, t), fields(labels, new))]
    fn upsert_label(
        &self,
        t: &sled::transaction::TransactionalTree,
        k: &IVec,
        id: (u64, u64),
    ) -> Result<(), UnabortableTransactionError> {
        info!("Upserting label");
        match t.get(&k) {
            Ok(Some(current)) => {
                info!("Label already exists, updating references");
                let mut labels: Vec<(u64, u64)> = Mc::de(current).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                labels.push(id);
                labels.sort();
                labels.dedup();
                let new = Mc::ser(labels).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                let _ = t.insert(k, new)?;
                Ok(())
            }
            Ok(None) => {
                info!("Label does not exist, creating");
                let new = Mc::ser(vec![id]).map_err(|e| {
                    UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string()))
                })?;
                let _ = t.insert(k, new)?;
                Ok(())
            }
            Err(e) => {
                error!("Label failed to upsert");
                Err(UnabortableTransactionError::Storage(sled::Error::ReportableBug(e.to_string())))
            },
        }
    }
}
