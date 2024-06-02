use crate::{bucket::McBucket, errors::MangoChainsawError};
use flexbuffers::FlexbufferSerializer;
use serde::{de::DeserializeOwned, Serialize};
use sled::{Config, IVec};
use std::cmp::min;
use tracing::debug;
use tracing::instrument;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct MangoChainsaw {
    pub(crate) db: sled::Db,
}

impl MangoChainsaw {
    /// Create or open an existing Mc5 from a Config
    #[cfg(not(test))]
    #[instrument]
    pub fn new(config: Config) -> Result<Self, MangoChainsawError> {
        debug!("Opening db");
        Ok(Self { db: config.open()? })
    }

    /// Create or open an existing Mc5 from a Config
    #[cfg(test)]
    #[instrument]
    pub fn new(_config: Config) -> Result<Self, MangoChainsawError> {
        let cfg = sled::Config::new().temporary(true);
        debug!("Opening temporary db");
        Ok(Self { db: cfg.open()? })
    }

    /// Get a named tree from the sled backend
    #[instrument(skip(self))]
    pub(crate) fn get_tree(&self, name: &str) -> Result<sled::Tree, MangoChainsawError> {
        debug!("Opening tree {name}");
        Ok(self.db.open_tree(name)?)
    }

    /// Get the next ID from the sled monotonic idgen
    #[instrument(skip(self), fields(node_id))]
    pub(crate) fn next_id(&self) -> Result<Uuid, MangoChainsawError> {
        let node_id = self
            .db
            .generate_id()?
            .to_be_bytes()
            .into_iter()
            .fold(([0u8; 6], 0), |mut acc, b| {
                (acc.0[acc.1], acc.1) = (b, min(acc.1 + 1, 5));
                acc
            })
            .0;
        debug!("Got node_id={node_id:?}");
        Ok(Uuid::now_v6(&node_id))
    }

    /// Create or open a named bucket
    #[instrument(skip(self), fields(this))]
    pub fn get_bucket(&self, name: &str) -> Result<McBucket, MangoChainsawError> {
        let this = McBucket::new(self, name)?;
        debug!("Opened bucket {name}");
        Ok(this)
    }

    /// List buckets
    #[instrument(skip(self))]
    pub fn list_buckets(&self) -> Result<Vec<String>, MangoChainsawError> {
        let mut results = vec![];
        for raw_name in self.db.tree_names() {
            let name = std::str::from_utf8(&raw_name)?;
            results.push(name.to_string());
        }
        results = results.into_iter().fold(vec![], |mut acc, r| {
            if r.contains("::") {
                if let Some((name, _suffix)) = r.split_once("::") {
                    acc.push(name.to_string());
                }
            }
            acc
        });
        results.sort();
        results.dedup();
        Ok(results)
    }

    /// Drop a bucket
    #[instrument(skip(self))]
    pub fn drop_bucket(&self, name: &str) -> Result<(), MangoChainsawError> {
        let b = McBucket::new(self, name)?;
        b.drop_bucket()?;
        Ok(())
    }

    /// Serialize a thing to be stored as a document
    #[instrument(skip(o))]
    pub(crate) fn ser<T>(o: T) -> Result<IVec, MangoChainsawError>
    where
        T: Serialize,
    {
        let mut ser = FlexbufferSerializer::new();
        o.serialize(&mut ser)?;
        debug!("serialize success");
        Ok(IVec::from(ser.take_buffer()))
    }

    /// Deserialize bytes from the backend into a document
    #[instrument(skip(b))]
    pub(crate) fn de<T>(b: IVec) -> Result<T, MangoChainsawError>
    where
        T: DeserializeOwned,
    {
        let rdr = flexbuffers::Reader::get_root(b.as_ref())?;
        debug!("deserialize success");
        Ok(T::deserialize(rdr)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::label::Label;
    use crate::{mclabel, mclabels};
    use serde::Deserialize;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tracing_subscriber::EnvFilter;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Testobj {
        pub x: u64,
        pub y: bool,
        pub z: String,
    }

    impl Testobj {
        pub fn new() -> Self {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            Self {
                x: now,
                y: now % 2 == 0,
                z: format!("{now}"),
            }
        }
    }

    fn init_tracing() {
        tracing_subscriber::fmt()
            .pretty()
            .with_ansi(true)
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    }

    #[test]
    fn test_insert_get_scan_delete() -> Result<(), MangoChainsawError> {
        init_tracing();

        let db = MangoChainsaw::new(Config::default())?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let object = Testobj::new();
        let labels = mclabels!(
            "object_type" => "test",
            "when" => &format!("{now}"),
            "okay" => "maybe?",
            "tls.howmuchmeat.com" => "status: enabled",
            "tls.howmuchmeat.org" => "status: down",
            "tls.howmuchmeat.dog" => "status: disabled",
            "tls.howmuchmeat.net" => "status: enabled",
            "tls.howmuchmeat.cat" => "status: up"
        );

        let bucket = db.get_bucket("testing_objects")?;
        let id = bucket.insert(&object, labels)?;
        println!("{id}");

        let ids =
            bucket.search_inclusive(mclabels!("object_type" => "test", "okay" => "maybe?"))?;
        println!("{ids:?}");

        let got = bucket.get::<Testobj>(ids[0])?.expect("Oops");
        assert_eq!(object, got);

        let labels = bucket.get_document_labels(id)?.expect("oops");
        println!("{labels:?}");

        let label_prefix = bucket.label_name_search("tls")?;
        println!("{label_prefix:?}");

        let label_suffix = bucket.label_value_search("status: enabled")?;
        println!("{label_suffix:?}");

        let deleted = bucket.delete::<Testobj>(ids[0])?.expect("oopsie");
        assert_eq!(object, deleted);

        let buckets = db.list_buckets()?;
        println!("{buckets:?}");

        Ok(())
    }
}
