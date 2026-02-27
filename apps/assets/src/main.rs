use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io::ErrorKind,
    net::SocketAddr,
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{anyhow, Context as _, Result};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use image::imageops::FilterType;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_THUMBNAIL_WIDTH: u32 = 128;
const DEFAULT_CARD_WIDTH: u32 = 800;
const DEFAULT_HERO_WIDTH: u32 = 1260;
const DEFAULT_ASSET_WIDTH: u32 = DEFAULT_CARD_WIDTH;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;

    tokio::fs::create_dir_all(&cfg.data_dir)
        .await
        .with_context(|| format!("create assets data directory {}", cfg.data_dir.display()))?;

    let cache_capacity = NonZeroUsize::new(cfg.cache_max_entries)
        .ok_or_else(|| anyhow!("PEBBLE_ASSETS_CACHE_MAX_ENTRIES must be > 0"))?;
    let state = AppState {
        cfg: cfg.clone(),
        cache: Arc::new(Mutex::new(LruCache::new(cache_capacity))),
    };

    let addr = cfg.addr;
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/assets/:filename", get(get_asset))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "pebble-assets listening");

    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve")?;

    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    addr: SocketAddr,
    data_dir: PathBuf,
    default_quality: u8,
    max_width: u32,
    cache_max_entries: usize,
}

impl Config {
    fn from_env() -> Result<Self> {
        let addr: SocketAddr = std::env::var("PEBBLE_ASSETS_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3060".to_string())
            .parse()
            .context("parse PEBBLE_ASSETS_ADDR")?;

        let raw_data_dir =
            std::env::var("PEBBLE_ASSETS_DATA_DIR").unwrap_or_else(|_| "./data/assets".to_string());
        let data_dir = raw_data_dir.trim().to_string();
        if data_dir.is_empty() {
            return Err(anyhow!("PEBBLE_ASSETS_DATA_DIR must be non-empty"));
        }

        let default_quality = std::env::var("PEBBLE_ASSETS_DEFAULT_QUALITY")
            .unwrap_or_else(|_| "70".to_string())
            .trim()
            .parse::<u8>()
            .context("parse PEBBLE_ASSETS_DEFAULT_QUALITY")?;
        if !(1..=100).contains(&default_quality) {
            return Err(anyhow!("PEBBLE_ASSETS_DEFAULT_QUALITY must be in [1, 100]"));
        }

        let max_width = std::env::var("PEBBLE_ASSETS_MAX_WIDTH")
            .unwrap_or_else(|_| "4096".to_string())
            .trim()
            .parse::<u32>()
            .context("parse PEBBLE_ASSETS_MAX_WIDTH")?;
        if max_width == 0 {
            return Err(anyhow!("PEBBLE_ASSETS_MAX_WIDTH must be > 0"));
        }

        let cache_max_entries = std::env::var("PEBBLE_ASSETS_CACHE_MAX_ENTRIES")
            .unwrap_or_else(|_| "512".to_string())
            .trim()
            .parse::<usize>()
            .context("parse PEBBLE_ASSETS_CACHE_MAX_ENTRIES")?;
        if cache_max_entries == 0 {
            return Err(anyhow!("PEBBLE_ASSETS_CACHE_MAX_ENTRIES must be > 0"));
        }

        Ok(Self {
            addr,
            data_dir: PathBuf::from(data_dir),
            default_quality,
            max_width,
            cache_max_entries,
        })
    }
}

#[derive(Debug, Clone)]
struct AppState {
    cfg: Config,
    cache: Arc<Mutex<LruCache<AssetCacheKey, Arc<CachedAsset>>>>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct AssetCacheKey {
    market_key: String,
    filename: String,
    width: u32,
    quality: u8,
}

#[derive(Debug)]
struct CachedAsset {
    body: Vec<u8>,
    etag: String,
}

#[derive(Debug, Deserialize)]
struct AssetQuery {
    w: Option<u32>,
    q: Option<u8>,
}

#[derive(Debug, Serialize)]
struct HealthzView {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Debug)]
struct AssetError {
    status: StatusCode,
    message: String,
}

impl AssetError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for AssetError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

async fn healthz() -> Json<HealthzView> {
    Json(HealthzView { status: "ok" })
}

async fn get_asset(
    State(state): State<AppState>,
    Path(filename_raw): Path<String>,
    Query(query): Query<AssetQuery>,
) -> Result<Response, AssetError> {
    let filename = normalize_filename(&filename_raw)?;
    let width = resolve_width(query.w, &filename, state.cfg.max_width)?;
    let quality = resolve_quality(query.q, state.cfg.default_quality)?;
    let cache_key = AssetCacheKey {
        market_key: extract_market_cache_key(&filename),
        filename: filename.clone(),
        width,
        quality,
    };

    if let Some(cached) = {
        let mut cache = state.cache.lock().await;
        cache.get(&cache_key).cloned()
    } {
        return response_from_cached_asset(&cached);
    }

    let path = state.cfg.data_dir.join(&filename);
    let source = tokio::fs::read(&path)
        .await
        .map_err(|err| match err.kind() {
            ErrorKind::NotFound => AssetError::not_found("asset not found"),
            _ => {
                tracing::error!(error = ?err, path = %path.display(), "failed to read asset file");
                AssetError::internal("failed to read source asset")
            }
        })?;

    let transformed = transform_image_to_webp(&source, width, quality).map_err(|err| {
        tracing::error!(error = ?err, filename = %filename, "failed to transform asset");
        AssetError::internal("failed to transform asset")
    })?;

    let etag = build_etag(&cache_key, &transformed);
    let cached_asset = Arc::new(CachedAsset {
        body: transformed,
        etag,
    });

    {
        let mut cache = state.cache.lock().await;
        cache.put(cache_key, cached_asset.clone());
    }

    response_from_cached_asset(&cached_asset)
}

fn response_from_cached_asset(asset: &CachedAsset) -> Result<Response, AssetError> {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("image/webp"));
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    let etag_header = HeaderValue::from_str(&asset.etag)
        .map_err(|_| AssetError::internal("failed to serialize asset etag"))?;
    headers.insert(header::ETAG, etag_header);
    Ok((headers, asset.body.clone()).into_response())
}

fn normalize_filename(raw: &str) -> Result<String, AssetError> {
    let filename = raw.trim();
    if filename.is_empty() {
        return Err(AssetError::bad_request("filename must be non-empty"));
    }
    if filename.contains('/') || filename.contains('\\') {
        return Err(AssetError::bad_request(
            "filename must not contain path separators",
        ));
    }
    if filename.starts_with('.') {
        return Err(AssetError::bad_request("filename must not start with dot"));
    }
    if filename.len() > 255 {
        return Err(AssetError::bad_request("filename is too long"));
    }
    if filename
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.'))
    {
        return Err(AssetError::bad_request(
            "filename contains unsupported characters",
        ));
    }
    Ok(filename.to_string())
}

fn resolve_width(width: Option<u32>, filename: &str, max_width: u32) -> Result<u32, AssetError> {
    match width {
        Some(value) => {
            if value == 0 {
                return Err(AssetError::bad_request("w must be > 0"));
            }
            Ok(value.min(max_width))
        }
        None => Ok(default_width_for_filename(filename).min(max_width)),
    }
}

fn resolve_quality(quality: Option<u8>, default_quality: u8) -> Result<u8, AssetError> {
    let value = quality.unwrap_or(default_quality);
    if !(1..=100).contains(&value) {
        return Err(AssetError::bad_request("q must be in [1, 100]"));
    }
    Ok(value)
}

fn default_width_for_filename(filename: &str) -> u32 {
    if filename.contains("_thumbnail_") {
        return DEFAULT_THUMBNAIL_WIDTH;
    }
    if filename.contains("_card_background_") {
        return DEFAULT_CARD_WIDTH;
    }
    if filename.contains("_hero_background_") {
        return DEFAULT_HERO_WIDTH;
    }
    DEFAULT_ASSET_WIDTH
}

fn extract_market_cache_key(filename: &str) -> String {
    filename
        .split_once('_')
        .map(|(prefix, _)| prefix)
        .unwrap_or("market")
        .to_string()
}

fn transform_image_to_webp(source: &[u8], requested_width: u32, quality: u8) -> Result<Vec<u8>> {
    let image = image::load_from_memory(source).context("decode source image")?;
    let source_width = image.width();
    let source_height = image.height();
    if source_width == 0 || source_height == 0 {
        return Err(anyhow!("source image must have positive dimensions"));
    }

    let target_width = requested_width.min(source_width).max(1);
    let target_height_u64 =
        (u64::from(source_height) * u64::from(target_width)).div_ceil(u64::from(source_width));
    let target_height = u32::try_from(target_height_u64).context("target height overflow")?;

    let processed = if target_width == source_width && target_height == source_height {
        image
    } else {
        image.resize_exact(target_width, target_height, FilterType::Lanczos3)
    };
    encode_dynamic_image_as_webp_lossy(processed, quality)
}

fn encode_dynamic_image_as_webp_lossy(image: image::DynamicImage, quality: u8) -> Result<Vec<u8>> {
    let rgba = image.to_rgba8();
    let width = rgba.width();
    let height = rgba.height();
    let encoder = webp::Encoder::from_rgba(rgba.as_raw(), width, height);
    let webp = encoder.encode(f32::from(quality));
    Ok(webp.to_vec())
}

fn build_etag(key: &AssetCacheKey, payload: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    payload.hash(&mut hasher);
    format!("\"{:x}\"", hasher.finish())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = ?err, "failed to listen for ctrl+c");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        let result = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match result {
            Ok(mut signal) => {
                let _ = signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = ?err, "failed to listen for terminate signal");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{
        default_width_for_filename, extract_market_cache_key, normalize_filename, resolve_quality,
        resolve_width, DEFAULT_CARD_WIDTH, DEFAULT_HERO_WIDTH, DEFAULT_THUMBNAIL_WIDTH,
    };

    #[test]
    fn default_width_for_filename_uses_slot_conventions() {
        assert_eq!(
            default_width_for_filename("mkt-1_thumbnail_abcd.webp"),
            DEFAULT_THUMBNAIL_WIDTH
        );
        assert_eq!(
            default_width_for_filename("mkt-1_card_background_abcd.webp"),
            DEFAULT_CARD_WIDTH
        );
        assert_eq!(
            default_width_for_filename("mkt-1_hero_background_abcd.webp"),
            DEFAULT_HERO_WIDTH
        );
    }

    #[test]
    fn normalize_filename_rejects_unsafe_values() {
        assert!(normalize_filename("").is_err());
        assert!(normalize_filename("../evil.webp").is_err());
        assert!(normalize_filename("folder/evil.webp").is_err());
        assert!(normalize_filename(".hidden.webp").is_err());
        assert!(normalize_filename("asset😀.webp").is_err());
    }

    #[test]
    fn resolve_width_uses_default_when_missing_and_clamps_to_max() {
        assert_eq!(
            resolve_width(None, "mkt_thumbnail_abcd.webp", 4096).unwrap(),
            DEFAULT_THUMBNAIL_WIDTH
        );
        assert_eq!(resolve_width(Some(9999), "mkt.webp", 1024).unwrap(), 1024);
    }

    #[test]
    fn resolve_quality_enforces_bounds() {
        assert_eq!(resolve_quality(None, 70).unwrap(), 70);
        assert_eq!(resolve_quality(Some(1), 70).unwrap(), 1);
        assert_eq!(resolve_quality(Some(100), 70).unwrap(), 100);
        assert!(resolve_quality(Some(0), 70).is_err());
    }

    #[test]
    fn extract_market_cache_key_uses_filename_prefix() {
        assert_eq!(
            extract_market_cache_key("mkt-stress-001_thumbnail_a1.webp"),
            "mkt-stress-001"
        );
        assert_eq!(extract_market_cache_key("asset.webp"), "market");
    }
}
