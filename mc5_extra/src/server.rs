use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Router;
use axum::routing::{get};
use axum::response::{IntoResponse, Json};
use mc5_core::mango::MangoChainsaw;
use tracing::{info, instrument};

#[derive(Clone, Debug)]
pub struct MangoChainsawServer {}

impl MangoChainsawServer {
    #[instrument(skip(backend))]
    pub async fn run(backend: MangoChainsaw) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route("/hello", get(Self::hello))
            .route("/buckets", get(Self::list_buckets))
            .with_state(backend)
        ;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:1420").await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    #[instrument()]
    async fn hello(headers: HeaderMap) -> &'static str {
        "Hello"
    }

    #[instrument]
    async fn hello2(headers: HeaderMap) -> &'static str {
        "Hi"
    }

    #[instrument(skip(backend))]
    async fn list_buckets(headers: HeaderMap, State(backend): State<MangoChainsaw>) -> Result<Json<Vec<String>>, ServerError> {
        Ok(Json(backend.list_buckets()?))
    }
}


struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Whoopsie {}", self.0))
            .into_response()
    }
}

impl<E> From<E> for ServerError
where E: Into<anyhow::Error>
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}