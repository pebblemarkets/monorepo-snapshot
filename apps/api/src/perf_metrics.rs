use std::{
    collections::{HashMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const UNKNOWN_MARKET_ID: &str = "_unknown";
const OVERFLOW_MARKET_ID: &str = "_overflow";
const SUMMARY_JSONL_FILE_NAME: &str = "stage-latency-summary.jsonl";
const SUMMARY_TEXT_FILE_NAME: &str = "stage-latency-summary.log";

#[derive(Clone)]
pub(crate) struct ApiPerfMetrics {
    enabled: bool,
    window_size: usize,
    max_series: usize,
    log_dir: Option<PathBuf>,
    summary_period: Duration,
    inner: Arc<Mutex<ApiPerfInner>>,
}

struct ApiPerfInner {
    by_key: HashMap<MetricKey, RollingLatencyStats>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct MetricKey {
    endpoint: &'static str,
    stage: &'static str,
    market_id: String,
    result_class: &'static str,
}

struct RollingLatencyStats {
    samples_us: VecDeque<u64>,
    total_samples: u64,
    total_sum_us: u128,
    max_total_us: u64,
    emitted_samples: u64,
}

impl RollingLatencyStats {
    fn new() -> Self {
        Self {
            samples_us: VecDeque::new(),
            total_samples: 0,
            total_sum_us: 0,
            max_total_us: 0,
            emitted_samples: 0,
        }
    }
}

struct MetricSummaryRow {
    endpoint: &'static str,
    stage: &'static str,
    market_id: String,
    result_class: &'static str,
    avg_total_us: u64,
    avg_window_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_total_us: u64,
    max_window_us: u64,
    samples_window: usize,
    samples_total: u64,
}

impl ApiPerfMetrics {
    pub(crate) fn new(
        enabled: bool,
        window_size: usize,
        max_series: usize,
        log_dir: Option<PathBuf>,
        summary_period: Duration,
    ) -> Self {
        let bounded_window_size = if window_size == 0 { 1 } else { window_size };
        let bounded_max_series = if max_series == 0 { 1 } else { max_series };
        Self {
            enabled,
            window_size: bounded_window_size,
            max_series: bounded_max_series,
            log_dir: normalize_log_dir(log_dir),
            summary_period,
            inner: Arc::new(Mutex::new(ApiPerfInner {
                by_key: HashMap::new(),
            })),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn window_size(&self) -> usize {
        self.window_size
    }

    pub(crate) fn summary_period(&self) -> Duration {
        self.summary_period
    }

    pub(crate) fn max_series(&self) -> usize {
        self.max_series
    }

    pub(crate) fn log_dir(&self) -> Option<&Path> {
        self.log_dir.as_deref()
    }

    pub(crate) fn record_duration(
        &self,
        endpoint: &'static str,
        stage: &'static str,
        market_id: &str,
        result_class: &'static str,
        duration: Duration,
    ) {
        if !self.enabled {
            return;
        }

        let base_key = MetricKey {
            endpoint,
            stage,
            market_id: normalize_market_id(market_id),
            result_class,
        };
        let sample_us = duration_to_micros(duration);

        let mut inner = lock_inner(&self.inner);
        let key = if inner.by_key.contains_key(&base_key) || inner.by_key.len() < self.max_series {
            base_key
        } else {
            MetricKey {
                endpoint,
                stage,
                market_id: OVERFLOW_MARKET_ID.to_string(),
                result_class,
            }
        };
        let stats = inner
            .by_key
            .entry(key)
            .or_insert_with(RollingLatencyStats::new);
        if stats.samples_us.len() >= self.window_size {
            let _ = stats.samples_us.pop_front();
        }
        stats.samples_us.push_back(sample_us);
        stats.total_samples = stats.total_samples.saturating_add(1);
        stats.total_sum_us = stats.total_sum_us.saturating_add(u128::from(sample_us));
        stats.max_total_us = stats.max_total_us.max(sample_us);
    }

    pub(crate) fn emit_pending_summaries(&self) {
        if !self.enabled {
            return;
        }

        let rows = {
            let mut inner = lock_inner(&self.inner);
            build_pending_summaries(&mut inner)
        };

        for row in &rows {
            tracing::info!(
                endpoint = row.endpoint,
                stage = row.stage,
                market_id = %row.market_id,
                result_class = row.result_class,
                avg_total_us = row.avg_total_us,
                avg_window_us = row.avg_window_us,
                p50_us = row.p50_us,
                p95_us = row.p95_us,
                p99_us = row.p99_us,
                max_total_us = row.max_total_us,
                max_window_us = row.max_window_us,
                samples_window = row.samples_window,
                samples_total = row.samples_total,
                "api stage latency summary"
            );
        }

        if let Some(log_dir) = &self.log_dir {
            if let Err(err) = append_summary_rows_to_files(log_dir, &rows) {
                tracing::warn!(
                    error = ?err,
                    log_dir = %log_dir.display(),
                    "failed to append api stage latency summary logs"
                );
            }
        }
    }

    pub(crate) async fn run_summary_loop(self) {
        if !self.enabled {
            return;
        }

        loop {
            tokio::time::sleep(self.summary_period).await;
            self.emit_pending_summaries();
        }
    }
}

fn build_pending_summaries(inner: &mut ApiPerfInner) -> Vec<MetricSummaryRow> {
    let mut rows = Vec::new();
    for (key, stats) in &mut inner.by_key {
        if stats.total_samples == stats.emitted_samples {
            continue;
        }

        let row = summarize_key(key, stats);
        stats.emitted_samples = stats.total_samples;
        rows.push(row);
    }

    rows.sort_unstable_by(|left, right| {
        left.endpoint
            .cmp(right.endpoint)
            .then_with(|| left.stage.cmp(right.stage))
            .then_with(|| left.market_id.cmp(&right.market_id))
            .then_with(|| left.result_class.cmp(right.result_class))
    });

    rows
}

fn summarize_key(key: &MetricKey, stats: &RollingLatencyStats) -> MetricSummaryRow {
    let mut sorted_window = stats.samples_us.iter().copied().collect::<Vec<_>>();
    sorted_window.sort_unstable();

    let window_sum_us = sorted_window.iter().fold(0_u128, |acc, sample| {
        acc.saturating_add(u128::from(*sample))
    });
    let samples_window_u64 = u64::try_from(sorted_window.len()).unwrap_or(u64::MAX);

    let avg_window_us = average_from_sum(window_sum_us, samples_window_u64);
    let avg_total_us = average_from_sum(stats.total_sum_us, stats.total_samples);
    let p50_us = percentile_nearest_rank(&sorted_window, 50);
    let p95_us = percentile_nearest_rank(&sorted_window, 95);
    let p99_us = percentile_nearest_rank(&sorted_window, 99);
    let max_window_us = sorted_window.last().copied().unwrap_or(0);

    MetricSummaryRow {
        endpoint: key.endpoint,
        stage: key.stage,
        market_id: key.market_id.clone(),
        result_class: key.result_class,
        avg_total_us,
        avg_window_us,
        p50_us,
        p95_us,
        p99_us,
        max_total_us: stats.max_total_us,
        max_window_us,
        samples_window: sorted_window.len(),
        samples_total: stats.total_samples,
    }
}

fn percentile_nearest_rank(sorted_values: &[u64], percentile: u32) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }

    let len = sorted_values.len();
    let len_u128 = match u128::try_from(len) {
        Ok(value) => value,
        Err(_) => return sorted_values[len.saturating_sub(1)],
    };
    let pct = u128::from(percentile);

    let rank = pct.saturating_mul(len_u128).div_ceil(100_u128).max(1_u128);
    let index_u128 = rank
        .saturating_sub(1_u128)
        .min(len_u128.saturating_sub(1_u128));
    let index = match usize::try_from(index_u128) {
        Ok(value) => value,
        Err(_) => len.saturating_sub(1),
    };

    sorted_values[index]
}

fn average_from_sum(sum: u128, count: u64) -> u64 {
    if count == 0 {
        return 0;
    }

    let avg = sum / u128::from(count);
    u64::try_from(avg).unwrap_or(u64::MAX)
}

fn duration_to_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn normalize_market_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        UNKNOWN_MARKET_ID.to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_log_dir(log_dir: Option<PathBuf>) -> Option<PathBuf> {
    log_dir.and_then(|raw| {
        let as_text = raw.to_string_lossy();
        if as_text.trim().is_empty() {
            None
        } else {
            Some(raw)
        }
    })
}

fn append_summary_rows_to_files(log_dir: &Path, rows: &[MetricSummaryRow]) -> std::io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    fs::create_dir_all(log_dir)?;
    append_summary_jsonl(&log_dir.join(SUMMARY_JSONL_FILE_NAME), rows)?;
    append_summary_text(&log_dir.join(SUMMARY_TEXT_FILE_NAME), rows)?;
    Ok(())
}

fn append_summary_jsonl(path: &Path, rows: &[MetricSummaryRow]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let emitted_at_unix_ms = now_unix_ms();

    for row in rows {
        let record = MetricSummaryJsonRecord {
            emitted_at_unix_ms,
            endpoint: row.endpoint,
            stage: row.stage,
            market_id: row.market_id.as_str(),
            result_class: row.result_class,
            avg_total_us: row.avg_total_us,
            avg_window_us: row.avg_window_us,
            p50_us: row.p50_us,
            p95_us: row.p95_us,
            p99_us: row.p99_us,
            max_total_us: row.max_total_us,
            max_window_us: row.max_window_us,
            samples_window: row.samples_window,
            samples_total: row.samples_total,
        };
        let line = serde_json::to_string(&record).map_err(json_error_to_io)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
    }

    file.flush()
}

fn append_summary_text(path: &Path, rows: &[MetricSummaryRow]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let emitted_at_unix_ms = now_unix_ms();

    for row in rows {
        let line = format!(
            "emitted_at_unix_ms={} endpoint={} stage={} market_id={} result_class={} \
avg_total_us={} avg_window_us={} p50_us={} p95_us={} p99_us={} max_total_us={} \
max_window_us={} samples_window={} samples_total={}\n",
            emitted_at_unix_ms,
            row.endpoint,
            row.stage,
            row.market_id,
            row.result_class,
            row.avg_total_us,
            row.avg_window_us,
            row.p50_us,
            row.p95_us,
            row.p99_us,
            row.max_total_us,
            row.max_window_us,
            row.samples_window,
            row.samples_total
        );
        file.write_all(line.as_bytes())?;
    }

    file.flush()
}

fn now_unix_ms() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis(),
        Err(_) => 0,
    }
}

fn json_error_to_io(err: serde_json::Error) -> std::io::Error {
    std::io::Error::other(err)
}

#[derive(serde::Serialize)]
struct MetricSummaryJsonRecord<'a> {
    emitted_at_unix_ms: u128,
    endpoint: &'a str,
    stage: &'a str,
    market_id: &'a str,
    result_class: &'a str,
    avg_total_us: u64,
    avg_window_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_total_us: u64,
    max_window_us: u64,
    samples_window: usize,
    samples_total: u64,
}

fn lock_inner<'a>(inner: &'a Arc<Mutex<ApiPerfInner>>) -> std::sync::MutexGuard<'a, ApiPerfInner> {
    match inner.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("api perf metrics lock poisoned; continuing");
            poisoned.into_inner()
        }
    }
}
