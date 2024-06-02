use std::{
    hash::{DefaultHasher, Hash, Hasher},
    os::windows::fs::MetadataExt,
    path::PathBuf,
    str::FromStr,
};

use anyhow::{anyhow, Result};
use mc5_core::{
    config::MangoChainsawConfig, label::Label, mango::MangoChainsaw, mclabel, mclabels,
};
use memmap2::Mmap;
use tracing::{error, info, instrument};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;
use walkdir::{DirEntry, WalkDir};

#[derive(Clone, Debug)]
struct TestFile {
    pub data: Vec<u8>,
    pub hash: u64,
    pub filename: String,
    pub size: u64,
    pub _attrs: u32,
    pub path: PathBuf,
    pub filetype: String,
    pub is_code: bool,
}

impl TryFrom<DirEntry> for TestFile {
    type Error = anyhow::Error;

    fn try_from(entry: DirEntry) -> std::prelude::v1::Result<Self, Self::Error> {
        let filename = entry.file_name().to_str().unwrap().to_string();
        let (contents, hash) = {
            let mut hasher = DefaultHasher::new();
            let file = match std::fs::File::open(entry.path()) {
                Ok(f) => f,
                Err(e) => {
                    error!("fuck {filename} {e}");
                    return Err(anyhow!(e));
                }
            };
            let contents = unsafe { Mmap::map(&file)? }.to_vec();
            contents.hash(&mut hasher);
            (contents, hasher.finish())
        };
        let path = entry.path().to_path_buf();
        let attrs = entry.metadata()?.file_attributes();
        let size = contents.len() as u64;
        let mut is_code = false;
        let mut filetype = "something_else".to_string();

        if filename.ends_with(".rs") {
            is_code = true;
            filetype = "rust_code".to_string();
        } else if filename.ends_with(".toml") {
            is_code = true;
            filetype = "toml_config".to_string();
        } else if filename.ends_with(".yaml") {
            is_code = true;
            filetype = "application_config".to_string();
        } else if filename.ends_with(".gitignore") {
            filetype = "git_config".to_string();
        } else if filename.ends_with(".lock") {
            filetype = "cargo_lock".to_string();
        }

        Ok(Self {
            data: contents,
            hash,
            filename,
            size,
            _attrs: attrs,
            path,
            filetype,
            is_code,
        })
    }
}

impl TestFile {
    pub fn to_labels(&self) -> Vec<Label> {
        mclabels!(
            "document_hash" => &self.hash.to_string(),
            "document_size" => &self.size.to_string(),
            "filename" => &self.filename,
            "filetype" => &self.filetype,
            "code_file" => &self.is_code.to_string(),
            "path" => self.path.to_str().unwrap()
        )
    }
}

#[tokio::test]
#[instrument()]
async fn core_test() -> Result<()> {
    let backend = setup()?;
    build_dataset(backend.clone())?;
    let file_ids = find_code_files(backend.clone())?;
    check_code_files(backend, file_ids)?;
    Ok(())
}

#[instrument()]
fn setup() -> Result<MangoChainsaw> {
    tracing_subscriber::fmt()
        .pretty()
        .with_ansi(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg_path = PathBuf::from_str("../mango_chainsaw.default.yaml")?.canonicalize()?;
    info!("{cfg_path:?}");
    let config = MangoChainsawConfig::load(cfg_path, "integration_test")?;
    let backend = MangoChainsaw::new(config.clone())?;
    Ok(backend)
}

#[instrument(skip(backend))]
fn build_dataset(backend: MangoChainsaw) -> Result<()> {
    let path = PathBuf::from_str("../")?.canonicalize()?;

    let test_bucket = backend.get_bucket("testing")?;

    // Walk the repo adding all files into the bucket.
    let wd = WalkDir::new(path)
        .follow_links(false)
        .same_file_system(true)
        .max_depth(10);

    for entry in wd {
        let entry = entry?;
        let path = entry.path().to_str().unwrap();
        if entry.path().is_dir() || path.contains("target\\") || path.contains(".git\\") {
            continue;
        }
        let tf: TestFile = entry.try_into()?;
        let (labels, doc) = (tf.to_labels(), tf.data);
        let id = test_bucket.insert(doc, labels)?;
        info!(id = id.to_string(), "Inserted Object");
    }

    Ok(())
}

#[instrument(skip(backend))]
fn find_code_files(backend: MangoChainsaw) -> Result<Vec<Uuid>> {
    let bucket = backend.get_bucket("testing")?;

    let ids = bucket.search_inclusive(mclabels!("code_file" => "true"))?;
    info!("found ids {ids:?}");
    Ok(ids)
}

#[instrument(skip(backend))]
fn check_code_files(backend: MangoChainsaw, ids: Vec<Uuid>) -> Result<()> {
    let bucket = backend.get_bucket("testing")?;

    for id in ids {
        let doc: Vec<u8> = bucket.get(id)?.unwrap();
        let labels = bucket.get_document_labels(id)?.unwrap();
        let mut path = PathBuf::new();
        for label in &labels {
            if label.key() == "path" {
                path = PathBuf::from_str(label.value())?;
                break;
            }
        }
        info!(
            "document {id} exists at {path:?} exists={:?}",
            path.exists()
        );
        let file = std::fs::File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? }.to_vec();
        info!("doc size = {}; mmap size = {}", doc.len(), mmap.len());
        assert_eq!(doc, mmap, "{id}::{path:?} does not match disk");
    }

    Ok(())
}
