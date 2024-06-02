use figment::{
    providers::{Format, YamlExtended},
    Figment, Metadata, Provider,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum BackendMode {
    Fast,
    Small,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MangoChainsawConfig {
    pub temporary: bool,
    pub listen: SocketAddr,
    pub data_path: PathBuf,
    pub backend_mode: BackendMode,
    pub idgen_interval: u64,
    pub compression_factor: i32,
}

impl Default for MangoChainsawConfig {
    fn default() -> Self {
        Self {
            temporary: true,
            listen: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1420)),
            data_path: Default::default(),
            backend_mode: BackendMode::Fast,
            idgen_interval: 420_069,
            compression_factor: 3,
        }
    }
}

impl Provider for MangoChainsawConfig {
    fn metadata(&self) -> figment::Metadata {
        Metadata::named("McConfig")
    }

    fn data(
        &self,
    ) -> Result<figment::value::Map<figment::Profile, figment::value::Dict>, figment::Error> {
        figment::providers::Serialized::defaults(MangoChainsawConfig::default()).data()
    }
}

impl MangoChainsawConfig {
    pub fn load<P: AsRef<Path>>(path: P, profile: &str) -> Result<Self, figment::Error> {
        info!(path = format!("{:?}", path.as_ref()), profile = profile, "Loading config");
        Figment::new()
            .merge(YamlExtended::file(path.as_ref()).nested())
            .select(profile)
            .extract()
    }

    pub fn to_sled_config(&self) -> sled::Config {
        sled::Config::new()
            .mode(match self.backend_mode {
                BackendMode::Fast => sled::Mode::HighThroughput,
                BackendMode::Small => sled::Mode::LowSpace,
            })
            .temporary(self.temporary)
            .idgen_persist_interval(self.idgen_interval)
            .path(&self.data_path)
            .use_compression(self.compression_factor > 0)
            .compression_factor(self.compression_factor)
    }
}
