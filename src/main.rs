use askama::Template;
use axum::{
    Router,
    http::{
        StatusCode,
        header::{AUTHORIZATION, USER_AGENT}
    },
    extract::{Query, State},
    response::{Html, IntoResponse, Response},
    routing::get
};
use chrono::{DateTime, SecondsFormat, Utc};
use glc::server::{setup_logging, serve, SpanMaker};
use reqwest::{
    Client,
    header::ACCEPT
};
use serde::Deserialize;
use std::{
    fs,
    io,
    net::IpAddr,
    sync::Arc,
    time::Duration
};
use sqlx::{
    Executor, Pool,
    sqlite::{Sqlite, SqlitePoolOptions}
};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer}
};
use tracing::{error, info, Level};

#[derive(Clone)]
pub struct AppState {
    client: Client,
    api_url: String,
    api_token: String,
    page_title: String,
    db: Pool<Sqlite>
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    RequestError(#[from] reqwest::Error),
    #[error("{0}")]
    TimeError(#[from] TimeError),
    #[error("{0}")]
    SqlxError(#[from] sqlx::Error)
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("{}", self);
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

#[derive(Template)]
#[template(path = "builds.html")]
struct BuildsTemplate {
    page_title: String,
    items: Vec<Build>,
    filter: String
}

struct HtmlTemplate<T>(T);

impl<T: Template> IntoResponse for HtmlTemplate<T> {
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {}", err),
            ).into_response(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimeError {
    #[error("{0} is out of range")]
    OutOfRangeDateTime(DateTime<Utc>),
    #[error("{0} is out of range")]
    OutOfRangeNs(i64),
    #[error("{0}")]
    Parse(#[from] chrono::format::ParseError)
}

fn rfc3339_to_nanos(s: &str) -> Result<i64, TimeError> {
    let dt = s.parse::<DateTime<Utc>>()?;
    dt.timestamp_nanos_opt()
        .ok_or(TimeError::OutOfRangeDateTime(dt))
}

fn nanos_to_rfc3339(ns: i64) -> Result<String, TimeError> {
    Ok(
        DateTime::<Utc>::from_timestamp(
            ns / 1_000_000_000,
            (ns % 1_000_000_000) as u32
        )
        .ok_or(TimeError::OutOfRangeNs(ns))?
        .to_rfc3339_opts(SecondsFormat::AutoSi, true)
    )
}

fn now_nanos() -> Result<i64, TimeError> {
    let dt = Utc::now();
    dt.timestamp_nanos_opt()
        .ok_or(TimeError::OutOfRangeDateTime(dt))
}

#[derive(Debug, Deserialize)]
struct RawBuild {
    id: i64,
    name: String,
    archive_download_url: String,
    created_at: String,
    updated_at: String,
    expires_at: String
}

#[derive(Debug, Deserialize)]
struct GitHubResult {
    artifacts: Vec<Build>
}

#[derive(Deserialize)]
struct QueryArgs {
    filter: Option<String>
}

#[derive(Debug, Deserialize)]
#[serde(try_from = "RawBuild")] 
struct Build {
    id: i64,
    name: String,
    url: String,
    created_at: i64,
    updated_at: i64,
    expires_at: i64 
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct BuildError(#[from] TimeError);

impl TryFrom<RawBuild> for Build {
    type Error = BuildError;

    fn try_from(b: RawBuild) -> Result<Self, Self::Error> {
        Ok(
            Self {
                id: b.id,
                name: b.name,              
                url: b.archive_download_url,
                created_at: rfc3339_to_nanos(&b.created_at)?,
                updated_at: rfc3339_to_nanos(&b.updated_at)?,
                expires_at: rfc3339_to_nanos(&b.expires_at)? 
            }
        )
    }
}

async fn get_builds_page(
    api_url: &str,
    page: usize,
    api_token: &str,
    client: &Client
) -> Result<Vec<Build>, AppError> 
{
    let auth = format!("token {}", api_token);
    let url = format!("{}?page={}", api_url, page);

    Ok(
        client
            .get(url)
            .header(AUTHORIZATION, &auth)
            .header(ACCEPT, "application/vnd.github.v3+json")
            .header(USER_AGENT, "builds-viewer")
            .send()
            .await?
            .error_for_status()?
            .json::<GitHubResult>()
            .await?
            .artifacts
    )
}

async fn record_build<'e, E>(
    ex: E,
    b: &Build 
) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO builds (
    id,
    name,
    url,
    created_at,
    updated_at,
    expires_at
)
VALUES (?, ?, ?, ?, ?, ?)
        ",
        b.id,
        b.name,
        b.url,
        b.created_at,
        b.updated_at,
        b.expires_at 
    )
    .execute(ex)
    .await?;

    Ok(())
}

async fn update_builds(
    api_url: &str,
    api_token: &str,
    client: &Client,
    db: &Pool<Sqlite>,
    now: i64
) -> Result<(), AppError>
{
    let mut page = 1;
    'gh: loop {
        let builds = get_builds_page(
            api_url,
            page,
            api_token,
            client
        ).await?;

        for b in builds {
            // stop if we hit an expired build
            if b.expires_at <= now {
                break;
            } 

            match record_build(db, &b).await {
                // stop if we hit a build we've seen before
                Err(sqlx::Error::Database(e))
                    if e.is_unique_violation() => break 'gh, 
                Err(e) => return Err(e.into()),
                Ok(()) => continue
            }
        }

        page += 1;
    }

    Ok(())
}

async fn get_builds<'e, E>(
    ex: E,
    now: i64
) -> Result<Vec<Build>, sqlx::Error>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_as!(
        Build,
        "
SELECT
    id,
    name,
    url,
    created_at,
    updated_at,
    expires_at
FROM builds
WHERE expires_at > ?
ORDER BY updated_at DESC
        ",
        now
    )
    .fetch_all(ex)
    .await
}

async fn get_list(
    Query(query): Query<QueryArgs>,
    State(state): State<Arc<AppState>>
) -> Result<HtmlTemplate<BuildsTemplate>, AppError>
{
    let now = now_nanos()?;

    update_builds(
        &state.api_url,
        &state.api_token,
        &state.client,
        &state.db,
        now
    ).await?;

    let items = get_builds(&state.db, now).await?;

    Ok(
        HtmlTemplate(
            BuildsTemplate {
                page_title: state.page_title.clone(),
                items,
                filter: query.filter.unwrap_or_default()
            }
        )
    )
}

fn routes(base_path: &str, log_headers: bool) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            if base_path.is_empty() { "/" } else { base_path },
            get(get_list)
        )
        .layer(
            ServiceBuilder::new()
                .layer(CompressionLayer::new())
                 // ensure requests don't block shutdown
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(10)
                ))
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(SpanMaker::new().include_headers(log_headers))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(DefaultOnFailure::new().level(Level::WARN))
        )
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub base_path: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub log_headers: bool,
    pub page_title: String,
    pub api_url: String,
    pub api_token: String,
    pub db_path: String
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("{0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("{0}")]
    Database(#[from] sqlx::Error),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Client(#[from] reqwest::Error)
}

async fn run() -> Result<(), StartupError> {
    info!("Reading config.toml");
    let config: Config = toml::from_str(&fs::read_to_string("config.toml")?)?;

    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&format!("sqlite://{}", &config.db_path))
        .await?;

    let state = Arc::new(AppState {
        client: Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
        api_url: config.api_url,
        api_token: config.api_token,
        page_title: config.page_title,
        db
    });

    // set up router
    let app = routes(
        &config.base_path,
        config.log_headers
    )
    .with_state(state);

    // serve pages
    let ip: IpAddr = config.listen_ip.parse()?;
    serve(app, ip, config.listen_port).await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    // set up logging
    let _guard = setup_logging(env!("CARGO_CRATE_NAME"), "", "viewer.log");

    info!("Starting");

    if let Err(e) = run().await {
        error!("{}", e);
    }

    info!("Exiting");
}

#[cfg(test)]
mod test {
}
