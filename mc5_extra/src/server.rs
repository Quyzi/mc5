use anyhow::anyhow;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{delete, get};
use axum::Router;
use mc5_core::label::Label;
use mc5_core::mango::MangoChainsaw;
use mc5_core::mclabel;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::{info, instrument};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct MangoChainsawServer {}

impl MangoChainsawServer {
    #[instrument(skip(backend))]
    pub async fn run(backend: MangoChainsaw) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route("/buckets", get(Self::list_buckets))
            .route(
                "/buckets/:bucket",
                get(Self::stat_bucket)
                    .post(Self::insert_document)
                    .delete(Self::drop_bucket),
            )
            .route("/buckets/:bucket/:id", get(Self::get_document))
            .route("/query/:bucket", get(Self::find_documents))
            .with_state(backend);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:1420").await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    #[instrument(skip(backend), ret)]
    async fn list_buckets(
        headers: HeaderMap,
        State(backend): State<MangoChainsaw>,
    ) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        Ok((StatusCode::OK, Json(backend.list_buckets()?)))
    }

    #[instrument(skip(backend))]
    async fn drop_bucket(
        headers: HeaderMap,
        Path(bucket): axum::extract::Path<String>,
        State(backend): State<MangoChainsaw>,
    ) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        info!("Dropping bucket");
        backend.drop_bucket(&bucket)?;
        Ok((StatusCode::OK, bucket))
    }

    #[instrument(skip(backend), ret)]
    async fn stat_bucket(
        headers: HeaderMap,
        Path(bucket): axum::extract::Path<String>,
        State(backend): State<MangoChainsaw>,
    ) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        let bucket = backend.get_bucket(&bucket)?;
        Ok((StatusCode::OK, Json(bucket.stat()?)))
    }

    #[instrument(skip(backend, body))]
    async fn insert_document(
        headers: HeaderMap,
        Path(bucket): Path<String>,
        State(backend): State<MangoChainsaw>,
        Query(params): Query<HashMap<String, String>>,
        body: Bytes,
    ) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        let bucket = backend.get_bucket(&bucket)?;
        let labels = params
            .into_iter()
            .map(|(k, v)| mclabel!(&k => &v))
            .collect();
        let id = bucket.insert(body.to_vec(), labels)?;
        Ok((StatusCode::OK, id.as_bytes().to_vec()))
    }

    #[instrument(skip(backend))]
    async fn get_document(
        headers: HeaderMap,
        Path((bucket, id)): Path<(String, String)>,
        State(backend): State<MangoChainsaw>,
    ) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        let bucket = backend.get_bucket(&bucket)?;
        let id = Uuid::from_str(&id)?;
        if let Some(doc) = bucket.get::<Vec<u8>>(id)? {
            Ok((StatusCode::OK, doc))
        } else {
            Ok((StatusCode::NOT_FOUND, vec![]))
        }
    }

    #[instrument(skip(backend), ret)]
    async fn find_documents(headers: HeaderMap, Path(bucket): Path<String>, State(backend): State<MangoChainsaw>, Query(params): Query<HashMap<String, String>>) -> Result<(StatusCode, impl IntoResponse), ServerError> {
        let bucket = backend.get_bucket(&bucket)?;
        let labels: Vec<Label> = params.into_iter().map(|(k, v)| mclabel!(&k => &v)).collect();
        let ids: Vec<String> = bucket.search_inclusive(labels)?
            .into_iter().map(|id| id.to_string()).collect();
        Ok((StatusCode::OK, Json(ids)))
    }
}

#[derive(Debug)]
struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Whoopsie {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
