use std::cmp::min;
use crate::{bucket::McBucket, errors::McError};
use flexbuffers::FlexbufferSerializer;
use serde::{de::DeserializeOwned, Serialize};
use sled::{Config, IVec};
use tracing::instrument;
use tracing::debug;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Mc {
    pub(crate) db: sled::Db,
}

impl Mc {
    #[cfg(not(test))]
    #[instrument]
    pub fn new(config: Config) -> Result<Self, McError> {
        debug!("Opening db");
        Ok(Self { db: config.open()? })
    }
    
    #[cfg(test)]
    #[instrument]
    pub fn new(_config: Config) -> Result<Self, McError> {
        let cfg = sled::Config::new()
            .temporary(true);
        debug!("Opening temporary db");
        Ok(Self { db: cfg.open()? })
    }

    #[instrument(skip(self))]
    pub(crate) fn get_tree(&self, name: &str) -> Result<sled::Tree, McError> {
        debug!("Opening tree {name}");
        Ok(self.db.open_tree(format!("{name}"))?)
    }
    
    #[instrument(skip(self), fields(node_id))]
    pub(crate) fn next_id(&self) -> Result<Uuid, McError> {
        let node_id = self.db.generate_id()?
            .to_be_bytes().into_iter()
            .fold(([0u8; 6], 0), |mut acc, b| {
                (acc.0[acc.1], acc.1) = (b, min(acc.1 + 1, 5));
                acc
        }).0;
        debug!("Got node_id={node_id:?}");
        Ok(Uuid::now_v6(&node_id))
    }
    
    #[instrument(skip(self), fields(this))]
    pub fn get_bucket(&self, name: &str) -> Result<McBucket, McError> {
        let this = McBucket::new(&self, name)?;
        debug!("Opened bucket {name}");
        Ok(this)
    }

    #[instrument(skip(self), fields(id))]
    pub(crate) fn next_id_ivec(&self) -> Result<IVec, McError> {
        let id = self.next_id()?;
        debug!("Id={id}");
        Ok(IVec::from(id.as_bytes()))
    }

    #[instrument(skip(o))]
    pub(crate) fn ser<T>(o: T) -> Result<IVec, McError> 
    where T: Serialize {
        let mut ser = FlexbufferSerializer::new();
        o.serialize(&mut ser)?;
        debug!("serialize success");
        Ok(IVec::from(ser.take_buffer()))
    }
    
    #[instrument(skip(b))]
    pub(crate) fn de<T>(b: IVec) -> Result<T, McError>
    where T: DeserializeOwned {
        let rdr = flexbuffers::Reader::get_root(b.as_ref())?;
        debug!("deserialize success");
        Ok(T::deserialize(rdr)?)
    }
}
 
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use crate::{mclabel, mclabels};
    use crate::label::Label;
    use serde::Deserialize;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Testobj {
        pub x: u64,
        pub y: bool,
        pub z: String,
    }

    impl Testobj {
        pub fn new() -> Self {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
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
            .init();
    }

    #[test]
    fn test_id() -> Result<(), McError> {
        init_tracing();

        let db = Mc::new(Config::default())?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let object = Testobj::new();
        let labels = mclabels!(
            "object_type" => "test",
            "when" => &format!("{now}"),
            "okay" => "maybe?",
            "tls.howmuchmeat.com" => "status: disabled"
        );

        let bucket = db.get_bucket("testing_objects")?;
        let id = bucket.insert(&object, labels)?;
        println!("{id}");

        let got = bucket.get::<Testobj>(id)?
        .expect("Oops");
        assert_eq!(object, got);

        Ok(())
    }
}