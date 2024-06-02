use figment::{
    providers::{Format, YamlExtended}, value::{Dict, Map}, Figment, Metadata, Profile, Provider
};
use serde::{Deserialize, Serialize};
use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, path::{Path, PathBuf}, str::FromStr};


#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum BackendMode {
    Fast,
    Small,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub temporary: bool,
    pub data_path: PathBuf,
    pub backend_mode: BackendMode,
    pub idgen_interval: u64,
    pub compression_factor: i32,
    pub listen: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1420),
            temporary: false,
            data_path: PathBuf::from_str("mc5_data/").expect("dafuq?"),
            backend_mode: BackendMode::Fast,
            idgen_interval: 1_000_000,
            compression_factor: 0, 
        }
    }
}

impl Provider for Config {
    fn metadata(&self) -> figment::Metadata {
        Metadata::named("Mc5 Config")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        figment::providers::Serialized::defaults(Config::default()).data()
    }
}

impl Into<sled::Config> for Config {
    fn into(self) -> sled::Config {
        let config = sled::Config::new()
            .mode(match self.backend_mode {
                BackendMode::Fast => sled::Mode::HighThroughput,
                BackendMode::Small => sled::Mode::LowSpace,
            })
            .temporary(self.temporary)
            .idgen_persist_interval(self.idgen_interval)
            .path(self.data_path)
            .use_compression(self.compression_factor > 0)
            .compression_factor(self.compression_factor);
        config
    }
}

impl Config {
    /// Load config profile
    pub fn load<P: AsRef<Path>>(path: P, profile: &str) -> Result<Self, figment::Error> {
        Figment::new()
            .merge(YamlExtended::file(path).nested())
            .select(profile)
            .extract()
            
    }
}