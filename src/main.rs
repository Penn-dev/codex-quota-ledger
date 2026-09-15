use jiff::civil::Date;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod query;
mod usage;
mod web;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

const TASK_NAME: &str = "Codex Quota Recorder";
const DEFAULT_FALLBACK_SECONDS: u64 = 3_600;
const DEFAULT_TOKEN_SCAN_SECONDS: u64 = 30;
const MAX_LOG_BYTES: u64 = 1_048_576;
const RESET_JITTER_TOLERANCE_MS: i64 = 5 * 60 * 1_000;
const QUOTA_OBSERVATION_SCHEMA_VERSION: u32 = 2;
const ESTIMATE_ALGORITHM_VERSION: &str = "quota-estimate-v2";
const RECONCILIATION_ALGORITHM_VERSION: &str = "reconciliation-v2";

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    account_key: String,
    observed_at_ms: i64,
    used_percent: f64,
    remaining_percent: f64,
    reset_at_ms: Option<i64>,
    duration_minutes: f64,
    limit_id: Option<String>,
    plan: Option<String>,
    source: String,
    #[serde(skip_serializing)]
    raw_json: String,
    observation_schema_version: u32,
    collector_version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EstimateReport {
    algorithm_version: &'static str,
    quota: Snapshot,
    window_start_ms: i64,
    window_end_ms: i64,
    local_usage: usage::UsageReport,
    estimated_weekly_api_equivalent_usd: Option<f64>,
    estimated_weekly_range: Option<EstimateRange>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EstimateRange {
    lower_usd: f64,
    nominal_usd: f64,
    upper_usd: f64,
    assumption: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusReport {
    quota: Option<Snapshot>,
    quota_collector: CollectorHealth,
    token_ledger: usage::LedgerStatus,
}

#[derive(Serialize)]
struct HistoryData {
    history: Vec<Snapshot>,
}

#[derive(Serialize)]
struct WindowsData {
    windows: Vec<QuotaWindow>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportData {
    export_schema: &'static str,
    export_schema_version: u32,
    account_key: String,
    quota_history: Vec<Snapshot>,
    quota_windows: Vec<QuotaWindow>,
    official_usage: Vec<OfficialUsageExport>,
    official_daily_usage: Vec<OfficialDailyUsageExport>,
    local_usage: usage::UsageReport,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialUsageExport {
    observed_at_ms: i64,
    lifetime_tokens: Option<u64>,
    peak_daily_tokens: Option<u64>,
    longest_running_turn_sec: Option<u64>,
    current_streak_days: Option<u64>,
    longest_streak_days: Option<u64>,
    daily_buckets_available: bool,
    collector_version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialDailyUsageExport {
    observed_at_ms: i64,
    start_date: String,
    model: Option<String>,
    tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CollectorHealth {
    state: String,
    last_success_at_ms: Option<i64>,
    last_failure_at_ms: Option<i64>,
    consecutive_failures: u32,
    last_error_class: Option<String>,
    last_error_present: bool,
    next_retry_at_ms: Option<i64>,
    network_reads_total: u64,
    recoveries: u64,
    notifications_total: u64,
    last_notification_at_ms: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnoseData {
    database_schema_version: u32,
    active_account_available: bool,
    official_collector: CollectorHealth,
    local_ledger: usage::LedgerStatus,
    notification_live_observation: &'static str,
    privacy: PrivacyPosture,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrivacyPosture {
    stores_prompts: bool,
    stores_credentials: bool,
    telemetry_enabled: bool,
    exports_machine_paths: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RateLimitErrorClass {
    Authentication,
    RateLimited,
    Server,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionFailureKind {
    Authentication,
    RateLimited,
    Transient,
}

#[derive(Debug)]
struct SessionFailure {
    kind: SessionFailureKind,
    message: String,
    had_success: bool,
    suggested_retry_seconds: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReconciliationReport {
    algorithm_version: &'static str,
    checked_at_ms: i64,
    app_server_quota: Snapshot,
    app_server_snapshot_age_ms: i64,
    web_observation: Option<WebObservation>,
    local_usage: usage::UsageReport,
    api_equivalent_cost_per_used_percentage_point: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WebObservation {
    observed_at_ms: i64,
    used_percent: f64,
    app_server_delta_percentage_points: f64,
    same_rounded_percentage: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CalendarDayReport {
    algorithm_version: &'static str,
    date: String,
    timezone: String,
    from_ms: i64,
    to_ms: i64,
    local_usage: usage::UsageReport,
    official_usage: Option<OfficialDayObservation>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialDayObservation {
    observed_at_ms: i64,
    daily_buckets_available: bool,
    tokens: Option<u64>,
    model: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacityReport {
    algorithm_version: &'static str,
    account_key: String,
    epochs: Vec<CapacityEpoch>,
    warnings: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacityEpoch {
    limit_id: String,
    window_start_ms: i64,
    reset_at_ms: i64,
    first_used_percent: Option<f64>,
    last_used_percent: Option<f64>,
    observed_percent_change: Option<f64>,
    observed_local_tokens: u64,
    implied_full_window_local_tokens: Option<u64>,
    relative_to_previous_epoch: Option<f64>,
    confidence: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QuotaWindow {
    account_key: String,
    limit_id: String,
    reset_at_ms: i64,
    duration_minutes: f64,
    window_start_ms: i64,
    first_seen_at_ms: i64,
    last_seen_at_ms: i64,
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccountIdentity {
    account_key: String,
    auth_type: String,
    plan: Option<String>,
}

#[derive(Debug)]
enum ServerEvent {
    Message(Value),
    Closed,
    Invalid(String),
}

struct RunningServer {
    child: Child,
    stdin: ChildStdin,
    receiver: Receiver<ServerEvent>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct PidGuard {
    path: PathBuf,
    pid: u32,
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        if fs::read_to_string(&self.path)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            == Some(self.pid)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn main() {
    let command = env::args().nth(1).unwrap_or_else(|| "help".to_string());
    if let Err(error) = real_main() {
        if query::is_query_command(&command) {
            let message = if error.contains("no quota snapshot")
                || error.contains("no App Server quota snapshot")
            {
                "Required official quota evidence is unavailable."
            } else if error.contains("account identity is not confirmed")
                || error.contains("no active account partition")
            {
                "No verified active account partition is available."
            } else {
                "The query could not be completed. Run diagnose for local health details."
            };
            if query::print_unavailable(&command, now_ms(), message).is_err() {
                eprintln!("codex-quota-ledger: query unavailable");
            }
        } else {
            eprintln!("codex-quota-ledger: {error}");
        }
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_string());
    let home = app_home()?;

    match command.as_str() {
        "version" | "--version" | "-V" => {
            println!("codex-quota-ledger {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "run" => run_recorder(&home),
        "status" => print_status(&home),
        "history" => {
            let mut limit = 100_usize;
            while let Some(arg) = args.next() {
                if arg == "--limit" {
                    limit = args
                        .next()
                        .ok_or_else(|| "--limit requires a number".to_string())?
                        .parse::<usize>()
                        .map_err(|_| "invalid --limit value".to_string())?
                        .clamp(1, 10_000);
                }
            }
            print_history(&home, limit)
        }
        "windows" => print_windows(&home),
        "sync" => sync_usage(&home),
        "estimate" => print_estimate(&home),
        "day" => {
            let mut date = None;
            let mut timezone = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--date" => date = args.next(),
                    "--timezone" => timezone = args.next(),
                    _ => return Err(format!("unknown day option: {arg}")),
                }
            }
            print_calendar_day(
                &home,
                date.as_deref()
                    .ok_or_else(|| "day requires --date YYYY-MM-DD".to_string())?,
                timezone
                    .as_deref()
                    .ok_or_else(|| "day requires --timezone with an IANA name".to_string())?,
            )
        }
        "capacity" => print_capacity_history(&home),
        "reconcile" => {
            let mut web_used_percent = None;
            while let Some(arg) = args.next() {
                if arg == "--web-used-percent" {
                    let value = args
                        .next()
                        .ok_or_else(|| "--web-used-percent requires a number".to_string())?
                        .parse::<f64>()
                        .map_err(|_| "invalid --web-used-percent value".to_string())?;
                    if !value.is_finite() || !(0.0..=100.0).contains(&value) {
                        return Err("--web-used-percent must be between 0 and 100".to_string());
                    }
                    web_used_percent = Some(value);
                } else {
                    return Err(format!("unknown reconcile option: {arg}"));
                }
            }
            print_reconciliation(&home, web_used_percent)
        }
        "diagnose" => print_diagnose(&home),
        "export" => print_export(&home),
        "serve" => {
            let mut port = 48121_u16;
            let mut query_timeout_seconds = 15_u64;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--port" => {
                        port = args
                            .next()
                            .ok_or_else(|| "--port requires a number".to_string())?
                            .parse::<u16>()
                            .map_err(|_| "invalid --port value".to_string())?;
                    }
                    "--query-timeout-seconds" => {
                        query_timeout_seconds = args
                            .next()
                            .ok_or_else(|| "--query-timeout-seconds requires a number".to_string())?
                            .parse::<u64>()
                            .map_err(|_| "invalid --query-timeout-seconds value".to_string())?
                            .clamp(1, 120);
                    }
                    _ => return Err(format!("unknown serve option: {arg}")),
                }
            }
            web::serve(port, Duration::from_secs(query_timeout_seconds))
        }
        "open" => {
            let mut port = 48_121_u16;
            let mut launch_browser = true;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--port" => {
                        port = args
                            .next()
                            .ok_or_else(|| "--port requires a number".to_string())?
                            .parse::<u16>()
                            .map_err(|_| "invalid open port".to_string())?;
                    }
                    "--no-browser" => launch_browser = false,
                    _ => return Err(format!("unknown open option: {arg}")),
                }
            }
            if port == 0 {
                return Err("open requires a fixed nonzero port".to_string());
            }
            open_dashboard(&home, port, launch_browser)
        }
        "close" => close_dashboard(&home),
        "install" => install_task(&home),
        "uninstall" => uninstall_task(&home),
        "start" => run_schtasks(&["/Run", "/TN", TASK_NAME]).map(|_| ()),
        "stop" => stop_recorder(&home),
        "paths" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "home": home,
                    "database": database_path(&home),
                    "log": log_path(&home),
                    "codex": discover_codex_binary()?,
                }))
                .map_err(|error| error.to_string())?
            );
            Ok(())
        }
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => Err(format!("unknown command: {command}")),
    }
}

fn print_estimate(home: &Path) -> Result<(), String> {
    let connection = open_database(home)?;
    let quota = latest_snapshot(&connection)?
        .ok_or_else(|| "no quota snapshot is available yet".to_string())?;
    let reset_at_ms = quota
        .reset_at_ms
        .ok_or_else(|| "latest quota snapshot has no reset time".to_string())?;
    let duration_ms = (quota.duration_minutes * 60_000.0).round() as i64;
    let window_start_ms = reset_at_ms.saturating_sub(duration_ms);
    let window_end_ms = quota.observed_at_ms.min(reset_at_ms);
    let local_usage = usage::report_from_ledger(&connection, window_start_ms, window_end_ms)?;
    let estimated_weekly_api_equivalent_usd = local_usage
        .totals
        .api_equivalent_cost_usd
        .filter(|_| quota.used_percent > 0.0)
        .map(|cost| cost * 100.0 / quota.used_percent);
    let estimated_weekly_range = local_usage
        .totals
        .api_equivalent_cost_usd
        .filter(|_| quota.used_percent > 0.5)
        .map(|cost| EstimateRange {
            lower_usd: cost * 100.0 / (quota.used_percent + 0.5).min(100.0),
            nominal_usd: cost * 100.0 / quota.used_percent,
            upper_usd: cost * 100.0 / (quota.used_percent - 0.5).max(0.001),
            assumption: "usedPercent is rounded to the nearest whole percentage point",
        });
    let mut warnings = local_usage.warnings.clone();
    let observed_limit_ids = local_usage
        .groups
        .iter()
        .filter_map(|group| group.limit_id.as_deref())
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(quota_limit_id) = quota.limit_id.as_deref() {
        if !observed_limit_ids.is_empty() && !observed_limit_ids.contains(quota_limit_id) {
            warnings.push(format!(
                "Quota limit_id is {quota_limit_id}, but local token events use {}; projection is low confidence.",
                observed_limit_ids.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    warnings.push(
        "Projection covers local JSONL logs only; web, cloud, other-device, and missing logs are not recoverable."
            .to_string(),
    );
    let report = EstimateReport {
        algorithm_version: ESTIMATE_ALGORITHM_VERSION,
        quota,
        window_start_ms,
        window_end_ms,
        local_usage,
        estimated_weekly_api_equivalent_usd,
        estimated_weekly_range,
        warnings,
    };
    let issues = report
        .warnings
        .iter()
        .map(|warning| {
            query::Issue::warning(
                "estimate_evidence_limit",
                warning.clone(),
                "derived_estimate",
            )
        })
        .collect();
    query::print(
        "estimate",
        now_ms(),
        query::Availability::Partial,
        report,
        issues,
    )
}

fn print_reconciliation(home: &Path, web_used_percent: Option<f64>) -> Result<(), String> {
    let mut connection = open_database(home)?;
    let _ = usage::ingest_once(&mut connection)?;
    let quota = latest_snapshot(&connection)?
        .ok_or_else(|| "no App Server quota snapshot is available yet".to_string())?;
    let checked_at_ms = now_ms();
    let reset_at_ms = quota
        .reset_at_ms
        .ok_or_else(|| "latest App Server quota snapshot has no reset time".to_string())?;
    let duration_ms = (quota.duration_minutes * 60_000.0).round() as i64;
    let window_start_ms = reset_at_ms.saturating_sub(duration_ms);
    let report_end_ms = web_used_percent
        .map(|_| checked_at_ms)
        .unwrap_or(quota.observed_at_ms)
        .min(reset_at_ms);
    let local_usage = usage::report_from_ledger(&connection, window_start_ms, report_end_ms)?;
    let comparison_percent = web_used_percent.unwrap_or(quota.used_percent);
    let api_equivalent_cost_per_used_percentage_point = local_usage
        .totals
        .api_equivalent_cost_usd
        .filter(|_| comparison_percent > 0.0)
        .map(|cost| cost / comparison_percent);
    let web_observation = web_used_percent.map(|used_percent| WebObservation {
        observed_at_ms: checked_at_ms,
        used_percent,
        app_server_delta_percentage_points: used_percent - quota.used_percent,
        same_rounded_percentage: used_percent.round() == quota.used_percent.round(),
    });
    let age_ms = checked_at_ms.saturating_sub(quota.observed_at_ms);
    let mut warnings = local_usage.warnings.clone();
    warnings.push(
        "The App Server and web UI are server-side quota observations; local JSONL is a separate detail ledger and may omit web, cloud, other-device, or missing-log usage."
            .to_string(),
    );
    if web_observation.is_some() && age_ms > 15 * 60_000 {
        warnings.push(format!(
            "The App Server snapshot is {} minutes older than the web observation; refresh age can explain part of the percentage delta.",
            age_ms / 60_000
        ));
    }
    let value = ReconciliationReport {
        algorithm_version: RECONCILIATION_ALGORITHM_VERSION,
        checked_at_ms,
        app_server_quota: quota,
        app_server_snapshot_age_ms: age_ms,
        web_observation,
        local_usage,
        api_equivalent_cost_per_used_percentage_point,
        warnings,
    };
    let issues = value
        .warnings
        .iter()
        .map(|warning| {
            query::Issue::warning(
                "reconciliation_evidence_limit",
                warning.clone(),
                "reconciliation",
            )
        })
        .collect();
    query::print(
        "reconcile",
        now_ms(),
        query::Availability::Partial,
        value,
        issues,
    )
}

fn sync_usage(home: &Path) -> Result<(), String> {
    let mut connection = open_database(home)?;
    let summary = usage::ingest_once(&mut connection)?;
    let mut issues = Vec::new();
    if summary.malformed_lines > 0 {
        issues.push(query::Issue::warning(
            "malformed_jsonl_lines",
            format!(
                "{} malformed local JSONL lines were skipped.",
                summary.malformed_lines
            ),
            "local_jsonl",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print("sync", now_ms(), availability, summary, issues)
}

fn print_calendar_day(home: &Path, date: &str, timezone: &str) -> Result<(), String> {
    let mut connection = open_database(home)?;
    let _ = usage::ingest_once(&mut connection)?;
    let (from_ms, to_ms) = calendar_day_bounds_ms(date, timezone)?;
    let local_usage = usage::report_from_ledger(&connection, from_ms, to_ms)?;
    let official_usage = connection
        .query_row(
            "SELECT o.observed_at_ms, o.daily_buckets_available, d.tokens, d.model
             FROM official_usage_observations o
             LEFT JOIN official_daily_usage d
               ON d.observation_id=o.id AND d.start_date=?1
             WHERE o.account_key=COALESCE(
                 (SELECT account_key FROM current_account_state WHERE id=1),
                 'legacy-unknown'
             )
             ORDER BY o.observed_at_ms DESC, d.model
             LIMIT 1",
            [date],
            |row| {
                Ok(OfficialDayObservation {
                    observed_at_ms: row.get(0)?,
                    daily_buckets_available: row.get::<_, i64>(1)? != 0,
                    tokens: row.get(2)?,
                    model: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(|error| format!("unable to read official daily usage: {error}"))?;
    let report = CalendarDayReport {
        algorithm_version: "calendar-day-v1",
        date: date.to_string(),
        timezone: timezone.to_string(),
        from_ms,
        to_ms,
        local_usage,
        official_usage,
    };
    let mut issues = report
        .local_usage
        .warnings
        .iter()
        .map(|warning| {
            query::Issue::warning("local_usage_evidence_limit", warning.clone(), "local_jsonl")
        })
        .collect::<Vec<_>>();
    if report.official_usage.is_none() {
        issues.push(query::Issue::warning(
            "official_daily_usage_unavailable",
            "No official daily usage observation covers this date.",
            "official_daily_usage",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print("day", now_ms(), availability, report, issues)
}

fn implied_full_window_tokens(local_tokens: u64, percent_change: f64) -> Option<u64> {
    if !percent_change.is_finite() || percent_change <= 0.0 {
        return None;
    }
    let estimate = local_tokens as f64 * 100.0 / percent_change;
    (estimate.is_finite() && estimate <= u64::MAX as f64).then(|| estimate.round() as u64)
}

fn print_capacity_history(home: &Path) -> Result<(), String> {
    let mut connection = open_database(home)?;
    let _ = usage::ingest_once(&mut connection)?;
    let account_key: String = connection
        .query_row(
            "SELECT account_key FROM current_account_state WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| "no active account partition is available".to_string())?;
    let windows = {
        let mut statement = connection
            .prepare(
                "SELECT limit_id, reset_at_ms, duration_minutes, window_start_ms,
                        first_seen_at_ms, last_seen_at_ms
                 FROM quota_windows
                 WHERE account_key=?1
                 ORDER BY window_start_ms, reset_at_ms",
            )
            .map_err(|error| format!("unable to prepare capacity windows: {error}"))?;
        let rows = statement
            .query_map([&account_key], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|error| format!("unable to query capacity windows: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("unable to decode capacity windows: {error}"))?;
        rows
    };
    let mut epochs = Vec::new();
    let mut previous_estimate = None;
    for (limit_id, reset_at_ms, duration_minutes, window_start_ms, first_seen, last_seen) in windows
    {
        let points = {
            let mut statement = connection
                .prepare(
                    "SELECT used_percent FROM quota_snapshots
                     WHERE account_key=?1 AND limit_id=?2
                       AND ABS(duration_minutes - ?3) < 0.000001
                       AND ABS(reset_at_ms - ?4) <= ?5
                       AND observed_at_ms BETWEEN ?6 AND ?7
                     ORDER BY observed_at_ms, id",
                )
                .map_err(|error| format!("unable to prepare capacity snapshots: {error}"))?;
            let rows = statement
                .query_map(
                    params![
                        account_key,
                        limit_id,
                        duration_minutes,
                        reset_at_ms,
                        RESET_JITTER_TOLERANCE_MS,
                        first_seen,
                        last_seen
                    ],
                    |row| row.get::<_, f64>(0),
                )
                .map_err(|error| format!("unable to query capacity snapshots: {error}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("unable to decode capacity snapshots: {error}"))?;
            rows
        };
        let first_used_percent = points.first().copied();
        let last_used_percent = points.last().copied();
        let observed_percent_change = first_used_percent
            .zip(last_used_percent)
            .map(|(first, last)| last - first)
            .filter(|change| *change > 0.0);
        let local = usage::report_from_ledger(&connection, window_start_ms, reset_at_ms)?;
        let observed_local_tokens = local
            .totals
            .input_tokens
            .saturating_add(local.totals.output_tokens);
        let implied = observed_percent_change
            .and_then(|change| implied_full_window_tokens(observed_local_tokens, change));
        let relative_to_previous_epoch =
            previous_estimate
                .zip(implied)
                .and_then(|(previous, current): (u64, u64)| {
                    (previous > 0).then_some(current as f64 / previous as f64)
                });
        let confidence = if first_used_percent.is_some_and(|value| value <= 1.0)
            && observed_percent_change.is_some_and(|value| value >= 10.0)
        {
            "higher"
        } else if implied.is_some() {
            "partial-window"
        } else {
            "insufficient-observations"
        };
        if implied.is_some() {
            previous_estimate = implied;
        }
        epochs.push(CapacityEpoch {
            limit_id,
            window_start_ms,
            reset_at_ms,
            first_used_percent,
            last_used_percent,
            observed_percent_change,
            observed_local_tokens,
            implied_full_window_local_tokens: implied,
            relative_to_previous_epoch,
            confidence,
        });
    }
    let report = CapacityReport {
        algorithm_version: "historical-capacity-v1",
        account_key,
        epochs,
        warnings: vec![
            "Capacity is inferred from local raw tokens per observed official percentage point; it is not a documented OpenAI quota formula.",
            "Cloud, web, other-device and missing local logs can make epochs incomparable.",
        ],
    };
    let issues = report
        .warnings
        .iter()
        .map(|warning| {
            query::Issue::warning(
                "capacity_evidence_limit",
                (*warning).to_string(),
                "historical_capacity",
            )
        })
        .collect();
    query::print(
        "capacity",
        now_ms(),
        query::Availability::Partial,
        report,
        issues,
    )
}

fn print_help() {
    println!(
        "Codex Quota Recorder\n\n\
         Commands:\n\
           version             Print the installed version\n\
           run                 Run the recorder in the foreground\n\
           status              Print the latest snapshot as JSON\n\
           history [--limit N] Print recent snapshots as JSON\n\
           windows             Print recorded quota windows as JSON\n\
           sync                Ingest new local token events now\n\
           day --date YYYY-MM-DD --timezone IANA\n\
                               Compare a timezone-correct local day with official usage\n\
           capacity            Compare historical local-tokens-per-percent epochs\n\
           estimate            Report the current weekly window from SQLite\n\
           reconcile [--web-used-percent N]\n\
                                Compare App Server quota, optional web UI percentage, and local tokens\n\
           diagnose            Report coded collector, ledger, account, and privacy health\n\
           export              Export privacy-safe evidence for the active account as JSON\n\
           serve [--port N] [--query-timeout-seconds N]\n\
                                Serve the read-only dashboard on 127.0.0.1\n\
           open [--port N] [--no-browser]\n\
                                Start or reuse the dashboard and open it\n\
           close               Stop the on-demand dashboard\n\
           install             Install and start the Windows logon task\n\
           uninstall           Stop the recorder and remove the task\n\
           start               Start the installed task\n\
           stop                Stop the running recorder\n\
           paths               Print resolved local paths as JSON"
    );
}

fn app_home() -> Result<PathBuf, String> {
    if let Some(value) = env::var_os("CODEX_QUOTA_HOME") {
        return Ok(PathBuf::from(value));
    }
    env::current_exe()
        .map_err(|error| format!("unable to locate recorder executable: {error}"))?
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "recorder executable has no parent directory".to_string())
}

fn data_dir(home: &Path) -> PathBuf {
    home.join("data")
}

fn database_path(home: &Path) -> PathBuf {
    data_dir(home).join("quota.sqlite")
}

fn log_path(home: &Path) -> PathBuf {
    home.join("logs").join("recorder.log")
}

fn pid_path(home: &Path) -> PathBuf {
    home.join("recorder.pid")
}

fn dashboard_pid_path(home: &Path) -> PathBuf {
    home.join("dashboard.pid")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn calendar_day_bounds_ms(date: &str, timezone: &str) -> Result<(i64, i64), String> {
    if !timezone.contains('/') && timezone != "UTC" {
        return Err("timezone must be an IANA name such as Asia/Shanghai".to_string());
    }
    let date: Date = date
        .parse()
        .map_err(|error| format!("invalid calendar date: {error}"))?;
    let start = date
        .at(0, 0, 0, 0)
        .in_tz(timezone)
        .map_err(|error| format!("invalid IANA timezone or date boundary: {error}"))?;
    let end = start
        .tomorrow()
        .and_then(|value| value.start_of_day())
        .map_err(|error| format!("unable to resolve next calendar day: {error}"))?;
    Ok((
        start.timestamp().as_millisecond(),
        end.timestamp().as_millisecond(),
    ))
}

fn prepare_home(home: &Path) -> Result<(), String> {
    fs::create_dir_all(data_dir(home))
        .map_err(|error| format!("unable to create data directory: {error}"))?;
    fs::create_dir_all(home.join("logs"))
        .map_err(|error| format!("unable to create log directory: {error}"))?;
    Ok(())
}

fn append_log(home: &Path, message: &str) {
    if prepare_home(home).is_err() {
        return;
    }
    let path = log_path(home);
    if fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0) > MAX_LOG_BYTES {
        let _ = File::create(&path);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {}", now_ms(), message.replace(['\r', '\n'], " "));
    }
}

fn open_database(home: &Path) -> Result<Connection, String> {
    prepare_home(home)?;
    let connection = Connection::open(database_path(home))
        .map_err(|error| format!("unable to open quota database: {error}"))?;
    initialize_database(&connection)?;
    Ok(connection)
}

fn initialize_database(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS quota_snapshots (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 observed_at_ms INTEGER NOT NULL,
                 used_percent REAL NOT NULL,
                 remaining_percent REAL NOT NULL,
                 reset_at_ms INTEGER,
                 duration_minutes REAL NOT NULL,
                 limit_id TEXT,
                 plan TEXT,
                 source TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_quota_snapshots_time
                 ON quota_snapshots(observed_at_ms DESC, id DESC);
             CREATE TABLE IF NOT EXISTS quota_windows (
                 account_key TEXT NOT NULL DEFAULT 'legacy-unknown',
                 limit_id TEXT NOT NULL,
                 reset_at_ms INTEGER NOT NULL,
                 duration_minutes REAL NOT NULL,
                 window_start_ms INTEGER NOT NULL,
                 first_seen_at_ms INTEGER NOT NULL,
                 last_seen_at_ms INTEGER NOT NULL,
                 status TEXT NOT NULL,
                 PRIMARY KEY (account_key, limit_id, reset_at_ms, duration_minutes)
             );
             CREATE TABLE IF NOT EXISTS quota_collector_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 state TEXT NOT NULL,
                 last_success_at_ms INTEGER,
                 last_failure_at_ms INTEGER,
                 consecutive_failures INTEGER NOT NULL DEFAULT 0,
                 last_error_class TEXT,
                 last_error TEXT,
                 next_retry_at_ms INTEGER,
                 network_reads_total INTEGER NOT NULL DEFAULT 0,
                 recoveries INTEGER NOT NULL DEFAULT 0,
                 notifications_total INTEGER NOT NULL DEFAULT 0,
                 last_notification_at_ms INTEGER
             );
             CREATE TABLE IF NOT EXISTS account_partitions (
                 account_key TEXT PRIMARY KEY,
                 auth_type TEXT NOT NULL,
                 plan TEXT,
                 first_seen_at_ms INTEGER NOT NULL,
                 last_seen_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS official_usage_observations (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 account_key TEXT NOT NULL,
                 observed_at_ms INTEGER NOT NULL,
                 lifetime_tokens INTEGER,
                 peak_daily_tokens INTEGER,
                 longest_running_turn_sec INTEGER,
                 current_streak_days INTEGER,
                 longest_streak_days INTEGER,
                 daily_buckets_available INTEGER NOT NULL,
                 raw_json TEXT NOT NULL,
                 source TEXT NOT NULL DEFAULT 'account/usage/read',
                 schema_version INTEGER NOT NULL DEFAULT 1,
                 collector_version TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS idx_official_usage_observation_identity
                 ON official_usage_observations(account_key, observed_at_ms);
             CREATE TABLE IF NOT EXISTS official_daily_usage (
                 observation_id INTEGER NOT NULL,
                 account_key TEXT NOT NULL,
                 start_date TEXT NOT NULL,
                 model TEXT,
                 tokens INTEGER NOT NULL,
                 raw_json TEXT NOT NULL,
                 PRIMARY KEY (observation_id, start_date, model),
                 FOREIGN KEY (observation_id) REFERENCES official_usage_observations(id)
             );
             INSERT OR IGNORE INTO quota_collector_state (id, state) VALUES (1, 'starting');",
        )
        .map_err(|error| format!("unable to initialize quota database: {error}"))?;
    ensure_main_column(
        connection,
        "quota_snapshots",
        "account_key",
        "TEXT NOT NULL DEFAULT 'legacy-unknown'",
    )?;
    ensure_main_column(
        connection,
        "quota_snapshots",
        "raw_json",
        "TEXT NOT NULL DEFAULT '{}'",
    )?;
    ensure_main_column(
        connection,
        "quota_snapshots",
        "observation_schema_version",
        "INTEGER NOT NULL DEFAULT 1",
    )?;
    ensure_main_column(
        connection,
        "quota_snapshots",
        "collector_version",
        "TEXT NOT NULL DEFAULT 'legacy'",
    )?;
    ensure_main_column(
        connection,
        "quota_collector_state",
        "notifications_total",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_main_column(
        connection,
        "quota_collector_state",
        "last_notification_at_ms",
        "INTEGER",
    )?;
    migrate_quota_windows_account_partition(connection)?;
    usage::initialize_ledger(connection)
}

fn ensure_main_column(
    connection: &Connection,
    table_name: &str,
    column_name: &str,
    declaration: &str,
) -> Result<(), String> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table_name})"))
        .map_err(|error| format!("unable to inspect {table_name}: {error}"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| format!("unable to inspect {table_name} columns: {error}"))?;
    for column in columns {
        if column.map_err(|error| format!("unable to read {table_name} column: {error}"))?
            == column_name
        {
            return Ok(());
        }
    }
    connection
        .execute(
            &format!("ALTER TABLE {table_name} ADD COLUMN {column_name} {declaration}"),
            [],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to migrate {table_name}: {error}"))
}

fn migrate_quota_windows_account_partition(connection: &Connection) -> Result<(), String> {
    let has_account_key: bool = connection
        .prepare("PRAGMA table_info(quota_windows)")
        .and_then(|mut statement| {
            let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
            for column in columns {
                if column? == "account_key" {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .map_err(|error| format!("unable to inspect quota window schema: {error}"))?;
    if has_account_key {
        return Ok(());
    }
    connection
        .execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE quota_windows RENAME TO quota_windows_legacy;
             CREATE TABLE quota_windows (
                 account_key TEXT NOT NULL,
                 limit_id TEXT NOT NULL,
                 reset_at_ms INTEGER NOT NULL,
                 duration_minutes REAL NOT NULL,
                 window_start_ms INTEGER NOT NULL,
                 first_seen_at_ms INTEGER NOT NULL,
                 last_seen_at_ms INTEGER NOT NULL,
                 status TEXT NOT NULL,
                 PRIMARY KEY (account_key, limit_id, reset_at_ms, duration_minutes)
             );
             INSERT INTO quota_windows (
                 account_key, limit_id, reset_at_ms, duration_minutes,
                 window_start_ms, first_seen_at_ms, last_seen_at_ms, status
             ) SELECT 'legacy-unknown', limit_id, reset_at_ms, duration_minutes,
                      window_start_ms, first_seen_at_ms, last_seen_at_ms, status
               FROM quota_windows_legacy;
             DROP TABLE quota_windows_legacy;
             COMMIT;",
        )
        .map_err(|error| format!("unable to migrate quota windows by account: {error}"))
}

fn stable_account_fingerprint(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.trim().to_ascii_lowercase().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("acct-{hash:016x}")
}

fn account_identity(result: &Value) -> Result<AccountIdentity, String> {
    let account = result
        .get("account")
        .and_then(Value::as_object)
        .ok_or_else(|| "App Server returned no authenticated account".to_string())?;
    let auth_type = account
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "App Server account type is missing".to_string())?
        .to_string();
    let email = account
        .get("email")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "App Server account identity is unavailable; refusing an unpartitioned write"
                .to_string()
        })?;
    Ok(AccountIdentity {
        account_key: stable_account_fingerprint(&format!("{auth_type}:{email}")),
        auth_type,
        plan: account
            .get("planType")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn persist_account_identity(
    connection: &Connection,
    identity: &AccountIdentity,
    observed_at_ms: i64,
) -> Result<(), String> {
    connection
        .execute(
            "INSERT INTO account_partitions (
                 account_key, auth_type, plan, first_seen_at_ms, last_seen_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(account_key) DO UPDATE SET
                 auth_type=excluded.auth_type,
                 plan=excluded.plan,
                 last_seen_at_ms=excluded.last_seen_at_ms",
            params![
                identity.account_key,
                identity.auth_type,
                identity.plan,
                observed_at_ms
            ],
        )
        .map_err(|error| format!("unable to persist account partition: {error}"))?;
    connection
        .execute(
            "UPDATE current_account_state
             SET account_key=?1, updated_at_ms=?2 WHERE id=1",
            params![identity.account_key, observed_at_ms],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to activate account partition: {error}"))
}

fn optional_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64)
}

fn persist_official_usage(
    connection: &Connection,
    account_key: &str,
    observed_at_ms: i64,
    result: &Value,
) -> Result<(), String> {
    let summary = result.get("summary").unwrap_or(&Value::Null);
    let daily = result.get("dailyUsageBuckets");
    let daily_buckets_available = i64::from(daily.is_some_and(Value::is_array));
    let raw_json = serde_json::to_string(result)
        .map_err(|error| format!("unable to encode official usage observation: {error}"))?;
    connection
        .execute(
            "INSERT INTO official_usage_observations (
                 account_key, observed_at_ms, lifetime_tokens, peak_daily_tokens,
                 longest_running_turn_sec, current_streak_days, longest_streak_days,
                 daily_buckets_available, raw_json, collector_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(account_key, observed_at_ms) DO NOTHING",
            params![
                account_key,
                observed_at_ms,
                optional_u64(summary.get("lifetimeTokens")),
                optional_u64(summary.get("peakDailyTokens")),
                optional_u64(summary.get("longestRunningTurnSec")),
                optional_u64(summary.get("currentStreakDays")),
                optional_u64(summary.get("longestStreakDays")),
                daily_buckets_available,
                raw_json,
                env!("CARGO_PKG_VERSION")
            ],
        )
        .map_err(|error| format!("unable to persist official usage observation: {error}"))?;
    let observation_id: i64 = connection
        .query_row(
            "SELECT id FROM official_usage_observations
             WHERE account_key=?1 AND observed_at_ms=?2",
            params![account_key, observed_at_ms],
            |row| row.get(0),
        )
        .map_err(|error| format!("unable to resolve official usage observation: {error}"))?;
    if let Some(buckets) = daily.and_then(Value::as_array) {
        for bucket in buckets {
            let Some(start_date) = bucket.get("startDate").and_then(Value::as_str) else {
                continue;
            };
            let Some(tokens) = bucket.get("tokens").and_then(Value::as_u64) else {
                continue;
            };
            let model = bucket
                .get("model")
                .or_else(|| bucket.get("modelName"))
                .and_then(Value::as_str);
            let raw_bucket = serde_json::to_string(bucket)
                .map_err(|error| format!("unable to encode official daily bucket: {error}"))?;
            connection
                .execute(
                    "INSERT OR REPLACE INTO official_daily_usage (
                         observation_id, account_key, start_date, model, tokens, raw_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        observation_id,
                        account_key,
                        start_date,
                        model,
                        tokens,
                        raw_bucket
                    ],
                )
                .map_err(|error| format!("unable to persist official daily bucket: {error}"))?;
        }
    }
    Ok(())
}

fn latest_snapshot(connection: &Connection) -> Result<Option<Snapshot>, String> {
    let account_key = connection
        .query_row(
            "SELECT account_key FROM current_account_state WHERE id=1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|error| format!("unable to read active account partition: {error}"))?
        .unwrap_or_else(|| "legacy-unknown".to_string());
    latest_snapshot_for_account(connection, &account_key)
}

fn latest_snapshot_for_account(
    connection: &Connection,
    account_key: &str,
) -> Result<Option<Snapshot>, String> {
    connection
        .query_row(
            "SELECT account_key, observed_at_ms, used_percent, remaining_percent, reset_at_ms,
                    duration_minutes, limit_id, plan, source, raw_json,
                    observation_schema_version, collector_version
             FROM quota_snapshots
             WHERE account_key=?1
             ORDER BY observed_at_ms DESC, id DESC
             LIMIT 1",
            [account_key],
            |row| {
                Ok(Snapshot {
                    account_key: row.get(0)?,
                    observed_at_ms: row.get(1)?,
                    used_percent: row.get(2)?,
                    remaining_percent: row.get(3)?,
                    reset_at_ms: row.get(4)?,
                    duration_minutes: row.get(5)?,
                    limit_id: row.get(6)?,
                    plan: row.get(7)?,
                    source: row.get(8)?,
                    raw_json: row.get(9)?,
                    observation_schema_version: row.get(10)?,
                    collector_version: row.get(11)?,
                })
            },
        )
        .optional()
        .map_err(|error| format!("unable to read latest quota snapshot: {error}"))
}

fn persist_snapshot(connection: &Connection, mut snapshot: Snapshot) -> Result<bool, String> {
    if !snapshot.used_percent.is_finite() || !(0.0..=100.0).contains(&snapshot.used_percent) {
        return Err("invalid used percentage".to_string());
    }
    if !snapshot.duration_minutes.is_finite() || snapshot.duration_minutes <= 0.0 {
        return Err("invalid quota window duration".to_string());
    }
    snapshot.remaining_percent = (100.0 - snapshot.used_percent).clamp(0.0, 100.0);

    if let (Some(reset_at_ms), Some(limit_id)) =
        (snapshot.reset_at_ms, snapshot.limit_id.as_deref())
    {
        let canonical_reset_at_ms = connection
            .query_row(
                "SELECT reset_at_ms FROM quota_windows
                 WHERE account_key=?1 AND limit_id=?2 AND status='active'
                   AND ABS(duration_minutes - ?3) < 0.000001
                   AND ABS(reset_at_ms - ?4) <= ?5
                 ORDER BY ABS(reset_at_ms - ?4), first_seen_at_ms
                 LIMIT 1",
                params![
                    snapshot.account_key,
                    limit_id,
                    snapshot.duration_minutes,
                    reset_at_ms,
                    RESET_JITTER_TOLERANCE_MS
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|error| format!("unable to resolve quota-window jitter: {error}"))?
            .unwrap_or(reset_at_ms);
        let window_start_ms = canonical_reset_at_ms
            .saturating_sub((snapshot.duration_minutes * 60_000.0).round() as i64);
        connection
            .execute(
                "UPDATE quota_windows SET status='closed', last_seen_at_ms=?1
                 WHERE account_key=?2 AND limit_id=?3 AND status='active' AND reset_at_ms<>?4",
                params![
                    snapshot.observed_at_ms,
                    snapshot.account_key,
                    limit_id,
                    canonical_reset_at_ms
                ],
            )
            .map_err(|error| format!("unable to close previous quota window: {error}"))?;
        connection
            .execute(
                "INSERT INTO quota_windows (
                     account_key, limit_id, reset_at_ms, duration_minutes, window_start_ms,
                     first_seen_at_ms, last_seen_at_ms, status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, 'active')
                 ON CONFLICT(account_key, limit_id, reset_at_ms, duration_minutes)
                 DO UPDATE SET last_seen_at_ms=excluded.last_seen_at_ms, status='active'",
                params![
                    snapshot.account_key,
                    limit_id,
                    canonical_reset_at_ms,
                    snapshot.duration_minutes,
                    window_start_ms,
                    snapshot.observed_at_ms
                ],
            )
            .map_err(|error| format!("unable to upsert quota window: {error}"))?;
    }

    if let Some(previous) = latest_snapshot_for_account(connection, &snapshot.account_key)? {
        let unchanged = (previous.used_percent - snapshot.used_percent).abs() < 0.000_001
            && previous.reset_at_ms == snapshot.reset_at_ms
            && (previous.duration_minutes - snapshot.duration_minutes).abs() < 0.000_001
            && previous.limit_id == snapshot.limit_id
            && previous.account_key == snapshot.account_key
            && previous.plan == snapshot.plan;
        if unchanged && snapshot.observed_at_ms - previous.observed_at_ms < 86_400_000 {
            return Ok(false);
        }
    }

    connection
        .execute(
            "INSERT INTO quota_snapshots (
                 account_key, observed_at_ms, used_percent, remaining_percent, reset_at_ms,
                 duration_minutes, limit_id, plan, source, raw_json,
                 observation_schema_version, collector_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                snapshot.account_key,
                snapshot.observed_at_ms,
                snapshot.used_percent,
                snapshot.remaining_percent,
                snapshot.reset_at_ms,
                snapshot.duration_minutes,
                snapshot.limit_id,
                snapshot.plan,
                snapshot.source,
                snapshot.raw_json,
                snapshot.observation_schema_version,
                snapshot.collector_version,
            ],
        )
        .map_err(|error| format!("unable to persist quota snapshot: {error}"))?;
    Ok(true)
}

fn read_history(connection: &Connection, limit: usize) -> Result<Vec<Snapshot>, String> {
    let account_key = connection
        .query_row(
            "SELECT account_key FROM current_account_state WHERE id=1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|error| format!("unable to read active account partition: {error}"))?
        .unwrap_or_else(|| "legacy-unknown".to_string());
    let mut statement = connection
        .prepare(
            "SELECT account_key, observed_at_ms, used_percent, remaining_percent, reset_at_ms,
                    duration_minutes, limit_id, plan, source, raw_json,
                    observation_schema_version, collector_version
             FROM quota_snapshots
             WHERE account_key=?1
             ORDER BY observed_at_ms DESC, id DESC
             LIMIT ?2",
        )
        .map_err(|error| format!("unable to prepare history query: {error}"))?;
    let rows = statement
        .query_map(params![account_key, limit as i64], |row| {
            Ok(Snapshot {
                account_key: row.get(0)?,
                observed_at_ms: row.get(1)?,
                used_percent: row.get(2)?,
                remaining_percent: row.get(3)?,
                reset_at_ms: row.get(4)?,
                duration_minutes: row.get(5)?,
                limit_id: row.get(6)?,
                plan: row.get(7)?,
                source: row.get(8)?,
                raw_json: row.get(9)?,
                observation_schema_version: row.get(10)?,
                collector_version: row.get(11)?,
            })
        })
        .map_err(|error| format!("unable to query quota history: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("unable to decode quota history: {error}"))
}

fn print_status(home: &Path) -> Result<(), String> {
    let connection = open_database(home)?;
    let value = StatusReport {
        quota: latest_snapshot(&connection)?,
        quota_collector: collector_health(&connection)?,
        token_ledger: usage::ledger_status(&connection)?,
    };
    let mut issues = Vec::new();
    if value.quota.is_none() {
        issues.push(query::Issue::warning(
            "official_quota_unavailable",
            "No official quota observation is available yet.",
            "official_quota",
        ));
    }
    if value.quota_collector.state != "healthy" {
        issues.push(query::Issue::warning(
            "collector_not_healthy",
            format!(
                "The official quota collector is {}.",
                value.quota_collector.state
            ),
            "collector_health",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print("status", now_ms(), availability, value, issues)
}

fn collector_health(connection: &Connection) -> Result<CollectorHealth, String> {
    connection
        .query_row(
            "SELECT state, last_success_at_ms, last_failure_at_ms,
                    consecutive_failures, last_error_class, last_error,
                    next_retry_at_ms, network_reads_total, recoveries,
                    notifications_total, last_notification_at_ms
             FROM quota_collector_state WHERE id=1",
            [],
            |row| {
                let last_error: Option<String> = row.get(5)?;
                Ok(CollectorHealth {
                    state: row.get(0)?,
                    last_success_at_ms: row.get(1)?,
                    last_failure_at_ms: row.get(2)?,
                    consecutive_failures: row.get(3)?,
                    last_error_class: row.get(4)?,
                    last_error_present: last_error.is_some(),
                    next_retry_at_ms: row.get(6)?,
                    network_reads_total: row.get(7)?,
                    recoveries: row.get(8)?,
                    notifications_total: row.get(9)?,
                    last_notification_at_ms: row.get(10)?,
                })
            },
        )
        .map_err(|error| format!("unable to read quota collector health: {error}"))
}

fn record_rate_limit_notification(connection: &Connection) -> Result<(), String> {
    connection
        .execute(
            "UPDATE quota_collector_state
             SET notifications_total=notifications_total+1,
                 last_notification_at_ms=?1
             WHERE id=1",
            [now_ms()],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to record rate-limit notification: {error}"))
}

fn print_diagnose(home: &Path) -> Result<(), String> {
    let connection = open_database(home)?;
    let official_collector = collector_health(&connection)?;
    let local_ledger = usage::ledger_status(&connection)?;
    let active_account_available: bool = connection
        .query_row(
            "SELECT account_key IS NOT NULL FROM current_account_state WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("unable to diagnose account partition: {error}"))?;
    let notification_live_observation = if official_collector.notifications_total > 0 {
        "observed"
    } else {
        "pending"
    };
    let mut issues = Vec::new();
    if !active_account_available {
        issues.push(query::Issue::warning(
            "active_account_unavailable",
            "No verified active account partition is available.",
            "account",
        ));
    }
    if official_collector.state != "healthy" {
        issues.push(query::Issue::warning(
            "collector_not_healthy",
            format!("The official collector is {}.", official_collector.state),
            "collector_health",
        ));
    }
    if notification_live_observation == "pending" {
        issues.push(query::Issue::warning(
            "notification_live_observation_pending",
            "No real account/rateLimits/updated notification has been recorded yet.",
            "official_quota",
        ));
    }
    if local_ledger.last_error.is_some() {
        issues.push(query::Issue::warning(
            "local_ingestion_error_present",
            "The local ledger has a recorded ingestion error; private detail is omitted.",
            "local_jsonl",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print(
        "diagnose",
        now_ms(),
        availability,
        DiagnoseData {
            database_schema_version: 1,
            active_account_available,
            official_collector,
            local_ledger,
            notification_live_observation,
            privacy: PrivacyPosture {
                stores_prompts: false,
                stores_credentials: false,
                telemetry_enabled: false,
                exports_machine_paths: false,
            },
        },
        issues,
    )
}

fn print_export(home: &Path) -> Result<(), String> {
    let connection = open_database(home)?;
    let account_key: String = connection
        .query_row(
            "SELECT account_key FROM current_account_state
             WHERE id=1 AND account_key IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| format!("unable to read active account partition: {error}"))?
        .ok_or_else(|| "no active account partition is available".to_string())?;
    let quota_history = read_history(&connection, i64::MAX as usize)?;

    let quota_windows = {
        let mut statement = connection
            .prepare(
                "SELECT account_key, limit_id, reset_at_ms, duration_minutes, window_start_ms,
                        first_seen_at_ms, last_seen_at_ms, status
                 FROM quota_windows WHERE account_key=?1
                 ORDER BY reset_at_ms DESC, limit_id",
            )
            .map_err(|error| format!("unable to prepare export windows: {error}"))?;
        let values = statement
            .query_map([&account_key], |row| {
                Ok(QuotaWindow {
                    account_key: row.get(0)?,
                    limit_id: row.get(1)?,
                    reset_at_ms: row.get(2)?,
                    duration_minutes: row.get(3)?,
                    window_start_ms: row.get(4)?,
                    first_seen_at_ms: row.get(5)?,
                    last_seen_at_ms: row.get(6)?,
                    status: row.get(7)?,
                })
            })
            .map_err(|error| format!("unable to query export windows: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("unable to decode export windows: {error}"))?;
        values
    };

    let official_usage = {
        let mut statement = connection
            .prepare(
                "SELECT observed_at_ms, lifetime_tokens, peak_daily_tokens,
                        longest_running_turn_sec, current_streak_days, longest_streak_days,
                        daily_buckets_available, collector_version
                 FROM official_usage_observations
                 WHERE account_key=?1 ORDER BY observed_at_ms",
            )
            .map_err(|error| format!("unable to prepare official usage export: {error}"))?;
        let values = statement
            .query_map([&account_key], |row| {
                Ok(OfficialUsageExport {
                    observed_at_ms: row.get(0)?,
                    lifetime_tokens: row.get(1)?,
                    peak_daily_tokens: row.get(2)?,
                    longest_running_turn_sec: row.get(3)?,
                    current_streak_days: row.get(4)?,
                    longest_streak_days: row.get(5)?,
                    daily_buckets_available: row.get::<_, i64>(6)? != 0,
                    collector_version: row.get(7)?,
                })
            })
            .map_err(|error| format!("unable to query official usage export: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("unable to decode official usage export: {error}"))?;
        values
    };

    let official_daily_usage = {
        let mut statement = connection
            .prepare(
                "SELECT o.observed_at_ms, d.start_date, d.model, d.tokens
                 FROM official_daily_usage d
                 JOIN official_usage_observations o ON o.id=d.observation_id
                 WHERE d.account_key=?1
                 ORDER BY o.observed_at_ms, d.start_date, d.model",
            )
            .map_err(|error| format!("unable to prepare official daily export: {error}"))?;
        let values = statement
            .query_map([&account_key], |row| {
                Ok(OfficialDailyUsageExport {
                    observed_at_ms: row.get(0)?,
                    start_date: row.get(1)?,
                    model: row.get(2)?,
                    tokens: row.get(3)?,
                })
            })
            .map_err(|error| format!("unable to query official daily export: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("unable to decode official daily export: {error}"))?;
        values
    };

    let local_usage = usage::report_from_ledger(&connection, i64::MIN, i64::MAX)?;
    let mut issues = Vec::new();
    if official_usage.is_empty() {
        issues.push(query::Issue::warning(
            "official_usage_empty",
            "No official daily usage observations are available for the active account.",
            "official_daily_usage",
        ));
    }
    for warning in &local_usage.warnings {
        issues.push(query::Issue::warning(
            "local_usage_evidence_limit",
            warning.clone(),
            "local_jsonl",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print(
        "export",
        now_ms(),
        availability,
        ExportData {
            export_schema: "codex-quota-ledger.export",
            export_schema_version: 1,
            account_key,
            quota_history,
            quota_windows,
            official_usage,
            official_daily_usage,
            local_usage,
        },
        issues,
    )
}

fn record_network_read(connection: &Connection) -> Result<(), String> {
    connection
        .execute(
            "UPDATE quota_collector_state
             SET network_reads_total=network_reads_total+1 WHERE id=1",
            [],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to record quota read: {error}"))
}

fn record_collector_success(connection: &Connection) -> Result<(), String> {
    connection
        .execute(
            "UPDATE quota_collector_state
             SET state='healthy', last_success_at_ms=?1,
                 consecutive_failures=0, last_error_class=NULL,
                 last_error=NULL, next_retry_at_ms=NULL,
                 recoveries=recoveries + CASE
                     WHEN state IN ('recovering', 'degraded') THEN 1 ELSE 0 END
             WHERE id=1",
            [now_ms()],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to record quota collector success: {error}"))
}

fn record_collector_failure(
    connection: &Connection,
    failure: &SessionFailure,
    failures: u32,
    retry_delay_seconds: u64,
) -> Result<(), String> {
    let class = match failure.kind {
        SessionFailureKind::Authentication => "authentication",
        SessionFailureKind::RateLimited => "rate_limited",
        SessionFailureKind::Transient => "transient",
    };
    let state = if retry_delay_seconds >= 3_600 {
        "degraded"
    } else {
        "recovering"
    };
    let next_retry_at_ms = now_ms().saturating_add(
        retry_delay_seconds
            .saturating_mul(1_000)
            .min(i64::MAX as u64) as i64,
    );
    connection
        .execute(
            "UPDATE quota_collector_state
             SET state=?1, last_failure_at_ms=?2, consecutive_failures=?3,
                 last_error_class=?4, last_error=?5, next_retry_at_ms=?6
             WHERE id=1",
            params![
                state,
                now_ms(),
                failures,
                class,
                failure.message,
                next_retry_at_ms
            ],
        )
        .map(|_| ())
        .map_err(|error| format!("unable to record quota collector failure: {error}"))
}

fn print_history(home: &Path, limit: usize) -> Result<(), String> {
    let connection = open_database(home)?;
    let history = read_history(&connection, limit)?;
    let mut issues = Vec::new();
    if history.is_empty() {
        issues.push(query::Issue::warning(
            "quota_history_empty",
            "No official quota history is available for the active account.",
            "official_quota",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print(
        "history",
        now_ms(),
        availability,
        HistoryData { history },
        issues,
    )
}

fn print_windows(home: &Path) -> Result<(), String> {
    let connection = open_database(home)?;
    let mut statement = connection
        .prepare(
            "SELECT account_key, limit_id, reset_at_ms, duration_minutes, window_start_ms,
                    first_seen_at_ms, last_seen_at_ms, status
             FROM quota_windows
             WHERE account_key=COALESCE(
                 (SELECT account_key FROM current_account_state WHERE id=1),
                 'legacy-unknown'
             )
             ORDER BY reset_at_ms DESC, account_key, limit_id",
        )
        .map_err(|error| format!("unable to prepare quota windows query: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok(QuotaWindow {
                account_key: row.get(0)?,
                limit_id: row.get(1)?,
                reset_at_ms: row.get(2)?,
                duration_minutes: row.get(3)?,
                window_start_ms: row.get(4)?,
                first_seen_at_ms: row.get(5)?,
                last_seen_at_ms: row.get(6)?,
                status: row.get(7)?,
            })
        })
        .map_err(|error| format!("unable to query quota windows: {error}"))?;
    let windows = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("unable to decode quota windows: {error}"))?;
    let mut issues = Vec::new();
    if windows.is_empty() {
        issues.push(query::Issue::warning(
            "quota_windows_empty",
            "No reconstructed quota windows are available for the active account.",
            "derived_quota_windows",
        ));
    }
    let availability = if issues.is_empty() {
        query::Availability::Complete
    } else {
        query::Availability::Partial
    };
    query::print(
        "windows",
        now_ms(),
        availability,
        WindowsData { windows },
        issues,
    )
}

fn parse_window(value: &Value, inherited_limit_id: Option<String>) -> Option<Snapshot> {
    let used_percent = value
        .get("usedPercent")
        .or_else(|| value.get("used_percent"))
        .and_then(Value::as_f64)?;
    let duration_minutes = value
        .get("windowDurationMins")
        .or_else(|| value.get("window_duration_mins"))
        .or_else(|| value.get("durationMinutes"))
        .or_else(|| value.get("duration_minutes"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let reset_at_ms = value
        .get("resetsAt")
        .or_else(|| value.get("resets_at"))
        .or_else(|| value.get("resetAt"))
        .or_else(|| value.get("reset_at"))
        .and_then(Value::as_i64)
        .map(|value| {
            if value < 10_000_000_000 {
                value * 1000
            } else {
                value
            }
        });
    let limit_id = inherited_limit_id.or_else(|| {
        value
            .get("limitId")
            .or_else(|| value.get("limit_id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    });
    Some(Snapshot {
        account_key: String::new(),
        observed_at_ms: now_ms(),
        used_percent,
        remaining_percent: 100.0 - used_percent,
        reset_at_ms,
        duration_minutes,
        limit_id,
        plan: value
            .get("planType")
            .or_else(|| value.get("plan_type"))
            .or_else(|| value.get("plan"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        source: String::new(),
        raw_json: "{}".to_string(),
        observation_schema_version: QUOTA_OBSERVATION_SCHEMA_VERSION,
        collector_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

fn bucket_windows(value: &Value, inherited_limit_id: Option<String>) -> Vec<Snapshot> {
    let limit_id = value
        .get("limitId")
        .or_else(|| value.get("limit_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or(inherited_limit_id);
    let plan = value
        .get("planType")
        .or_else(|| value.get("plan_type"))
        .or_else(|| value.get("plan"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let mut observations = ["primary", "secondary"]
        .into_iter()
        .filter_map(|key| value.get(key))
        .filter_map(|window| parse_window(window, limit_id.clone()))
        .map(|mut observation| {
            if observation.plan.is_none() {
                observation.plan = plan.clone();
            }
            observation
        })
        .collect::<Vec<_>>();
    if observations.is_empty() {
        if let Some(mut observation) = parse_window(value, limit_id) {
            if observation.plan.is_none() {
                observation.plan = plan;
            }
            observations.push(observation);
        }
    }
    observations
}

fn select_weekly_window(mut candidates: Vec<Snapshot>) -> Result<Snapshot, String> {
    if candidates.is_empty() {
        return Err("unsupported rate-limit schema".to_string());
    }
    candidates.sort_by(|left, right| {
        (left.duration_minutes - 10_080.0)
            .abs()
            .total_cmp(&(right.duration_minutes - 10_080.0).abs())
    });
    let winner = candidates.remove(0);
    if (winner.duration_minutes - 10_080.0).abs() > 240.0
        || candidates.first().is_some_and(|next| {
            ((next.duration_minutes - 10_080.0).abs() - (winner.duration_minutes - 10_080.0).abs())
                .abs()
                < 0.000_001
        })
    {
        return Err("ambiguous weekly rate-limit schema".to_string());
    }
    Ok(winner)
}

fn select_codex_rate_limit(value: &Value, source: &str) -> Result<Snapshot, String> {
    let root = value.get("rateLimits").unwrap_or(value);
    let by_limit_id = root
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .or_else(|| value.get("rateLimitsByLimitId").and_then(Value::as_object));
    let mut selected = if let Some(by_limit_id) = by_limit_id {
        let codex = by_limit_id.get("codex").ok_or_else(|| {
            let mut ids = by_limit_id.keys().cloned().collect::<Vec<_>>();
            ids.sort();
            format!(
                "canonical codex limit id is missing; available limit ids: {}",
                ids.join(", ")
            )
        })?;
        select_weekly_window(bucket_windows(codex, Some("codex".to_string())))?
    } else {
        let mut candidates = Vec::new();
        if let Some(rate_limits) = root.get("rateLimits").or_else(|| value.get("rateLimits")) {
            if let Some(windows) = rate_limits.as_array() {
                for window in windows {
                    candidates.extend(bucket_windows(window, None));
                }
            } else {
                candidates.extend(bucket_windows(rate_limits, None));
            }
        } else {
            candidates.extend(bucket_windows(root, None));
        }
        select_weekly_window(candidates)?
    };
    selected.observed_at_ms = now_ms();
    selected.remaining_percent = (100.0 - selected.used_percent).clamp(0.0, 100.0);
    selected.source = source.to_string();
    selected.raw_json = serde_json::to_string(value)
        .map_err(|error| format!("unable to preserve raw rate-limit observation: {error}"))?;
    Ok(selected)
}

fn send_json(stdin: &mut ChildStdin, value: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *stdin, value)
        .map_err(|error| format!("unable to encode App Server request: {error}"))?;
    stdin
        .write_all(b"\n")
        .and_then(|_| stdin.flush())
        .map_err(|error| format!("unable to send App Server request: {error}"))
}

fn request(id: u64, method: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":{}})
}

fn account_read_request(id: u64) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id,
        "method":"account/read",
        "params":{"refreshToken":false}
    })
}

fn launch_server(binary: &Path) -> Result<RunningServer, String> {
    let mut command = Command::new(binary);
    command
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);

    let mut child = command
        .spawn()
        .map_err(|error| format!("unable to start {}: {error}", binary.display()))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "App Server stdin is unavailable".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "App Server stdout is unavailable".to_string())?;
    let mut reader = BufReader::new(stdout);

    send_json(
        &mut stdin,
        &json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{
                "clientInfo":{"name":"codex-quota-recorder","version":env!("CARGO_PKG_VERSION")},
                "capabilities":{}
            }
        }),
    )?;

    loop {
        let mut line = String::new();
        if reader
            .read_line(&mut line)
            .map_err(|error| format!("unable to initialize App Server: {error}"))?
            == 0
        {
            return Err("App Server closed during initialization".to_string());
        }
        let message: Value = serde_json::from_str(line.trim())
            .map_err(|error| format!("invalid App Server initialization response: {error}"))?;
        if message.get("id").and_then(Value::as_u64) == Some(1) {
            if message.get("error").is_some() {
                return Err(format!("App Server rejected initialization: {message}"));
            }
            break;
        }
    }

    send_json(
        &mut stdin,
        &json!({"jsonrpc":"2.0","method":"initialized","params":{}}),
    )?;

    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                let _ = sender.send(ServerEvent::Closed);
                break;
            }
            Ok(_) => match serde_json::from_str::<Value>(line.trim()) {
                Ok(value) => {
                    if sender.send(ServerEvent::Message(value)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(ServerEvent::Invalid(error.to_string()));
                }
            },
            Err(_) => {
                let _ = sender.send(ServerEvent::Closed);
                break;
            }
        }
    });

    Ok(RunningServer {
        child,
        stdin,
        receiver,
    })
}

fn send_rate_limit_read(
    server: &mut RunningServer,
    connection: &Connection,
    id: u64,
) -> Result<(), String> {
    record_network_read(connection)?;
    send_json(&mut server.stdin, &request(id, "account/rateLimits/read"))
}

fn send_usage_read(
    server: &mut RunningServer,
    connection: &Connection,
    id: u64,
) -> Result<(), String> {
    record_network_read(connection)?;
    send_json(&mut server.stdin, &request(id, "account/usage/read"))
}

fn classify_rate_limit_error(error: &Value) -> RateLimitErrorClass {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("token_invalidated")
        || text.contains("unauthorized")
        || text.contains("authentication")
        || text.contains("\"401\"")
        || text.contains("status 401")
    {
        RateLimitErrorClass::Authentication
    } else if text.contains("rate_limit")
        || text.contains("rate limit")
        || text.contains("too many requests")
        || text.contains("\"429\"")
        || text.contains("status 429")
    {
        RateLimitErrorClass::RateLimited
    } else if (500..=599).any(|status| {
        text.contains(&format!("\"{status}\"")) || text.contains(&format!("status {status}"))
    }) {
        RateLimitErrorClass::Server
    } else {
        RateLimitErrorClass::Other
    }
}

fn retry_after_seconds(error: &Value) -> Option<u64> {
    let roots = [Some(error), error.get("data")];
    for root in roots.into_iter().flatten() {
        if let Some(milliseconds) = root
            .get("retryAfterMs")
            .or_else(|| root.get("retry_after_ms"))
            .and_then(Value::as_u64)
        {
            return Some(milliseconds.saturating_add(999) / 1_000);
        }
        if let Some(seconds) = root
            .get("retryAfter")
            .or_else(|| root.get("retry_after"))
            .and_then(Value::as_u64)
        {
            return Some(seconds);
        }
    }
    None
}

fn jittered_fallback_seconds(base: u64, seed: u64) -> u64 {
    let spread = (base / 10).max(1);
    base.saturating_sub(spread)
        .saturating_add(seed % spread.saturating_mul(2).saturating_add(1))
}

fn authentication_retry_delay(failures: u32) -> u64 {
    if failures <= 1 {
        return 5 + (now_ms().unsigned_abs() % 11);
    }
    3_600_u64
        .saturating_mul(2_u64.saturating_pow(failures.saturating_sub(2).min(3)))
        .min(21_600)
}

fn transient_retry_delay(failures: u32) -> u64 {
    match failures {
        0 | 1 => 60,
        2 => 300,
        3 => 900,
        _ => 3_600,
    }
}

fn session_failure(
    kind: SessionFailureKind,
    message: impl Into<String>,
    had_success: bool,
) -> SessionFailure {
    SessionFailure {
        kind,
        message: message.into(),
        had_success,
        suggested_retry_seconds: None,
    }
}

fn run_server_session(
    home: &Path,
    connection: &Connection,
    binary: &Path,
) -> Result<(), SessionFailure> {
    let mut successful_reads = 0_u64;
    let mut server = launch_server(binary)
        .map_err(|error| session_failure(SessionFailureKind::Transient, error, false))?;
    append_log(home, &format!("App Server connected: {}", binary.display()));
    let fallback_seconds = env::var("CODEX_QUOTA_FALLBACK_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_FALLBACK_SECONDS)
        .clamp(300, 86_400);
    let timeout = Duration::from_secs(jittered_fallback_seconds(
        fallback_seconds,
        now_ms().unsigned_abs(),
    ));
    let mut next_id = 2_u64;
    let mut pending_account = Some(next_id);
    let mut pending_usage: Option<u64> = None;
    let mut pending_read: Option<(u64, &'static str)> = None;
    let mut current_account: Option<AccountIdentity> = None;
    send_json(&mut server.stdin, &account_read_request(next_id)).map_err(|error| {
        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
    })?;
    next_id += 1;

    loop {
        match server.receiver.recv_timeout(timeout) {
            Ok(ServerEvent::Message(message)) => {
                if message.get("method").and_then(Value::as_str) == Some("account/updated") {
                    return Err(session_failure(
                        SessionFailureKind::Authentication,
                        "account identity changed; rebuilding the App Server session",
                        successful_reads > 0,
                    ));
                }
                if message.get("method").and_then(Value::as_str)
                    == Some("account/rateLimits/updated")
                {
                    record_rate_limit_notification(connection).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    if current_account.is_some() && pending_read.is_none() {
                        pending_read = Some((next_id, "notification"));
                        send_rate_limit_read(&mut server, connection, next_id).map_err(
                            |error| {
                                session_failure(
                                    SessionFailureKind::Transient,
                                    error,
                                    successful_reads > 0,
                                )
                            },
                        )?;
                        next_id += 1;
                    }
                    continue;
                }

                let Some(id) = message.get("id").and_then(Value::as_u64) else {
                    continue;
                };
                if pending_account == Some(id) {
                    pending_account = None;
                    if let Some(error) = message.get("error") {
                        return Err(session_failure(
                            SessionFailureKind::Authentication,
                            error.to_string(),
                            successful_reads > 0,
                        ));
                    }
                    let result = message.get("result").ok_or_else(|| {
                        session_failure(
                            SessionFailureKind::Authentication,
                            "account/read returned no result",
                            successful_reads > 0,
                        )
                    })?;
                    let identity = account_identity(result).map_err(|error| {
                        session_failure(
                            SessionFailureKind::Authentication,
                            error,
                            successful_reads > 0,
                        )
                    })?;
                    persist_account_identity(connection, &identity, now_ms()).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    current_account = Some(identity);
                    pending_usage = Some(next_id);
                    send_usage_read(&mut server, connection, next_id).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    next_id += 1;
                    pending_read = Some((next_id, "initial"));
                    send_rate_limit_read(&mut server, connection, next_id).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    next_id += 1;
                    continue;
                }
                if pending_usage == Some(id) {
                    pending_usage = None;
                    if let Some(error) = message.get("error") {
                        if classify_rate_limit_error(error) == RateLimitErrorClass::Authentication {
                            return Err(session_failure(
                                SessionFailureKind::Authentication,
                                error.to_string(),
                                successful_reads > 0,
                            ));
                        }
                        append_log(home, &format!("account usage read unavailable: {error}"));
                        continue;
                    }
                    if let (Some(result), Some(identity)) =
                        (message.get("result"), current_account.as_ref())
                    {
                        persist_official_usage(connection, &identity.account_key, now_ms(), result)
                            .map_err(|error| {
                                session_failure(
                                    SessionFailureKind::Transient,
                                    error,
                                    successful_reads > 0,
                                )
                            })?;
                    }
                    continue;
                }
                if pending_read.map(|(pending_id, _)| pending_id) != Some(id) {
                    continue;
                }
                let source = pending_read
                    .take()
                    .map(|(_, source)| source)
                    .unwrap_or("read");
                if let Some(error) = message.get("error") {
                    append_log(home, &format!("rate-limit read rejected: {error}"));
                    let kind = match classify_rate_limit_error(error) {
                        RateLimitErrorClass::Authentication => SessionFailureKind::Authentication,
                        RateLimitErrorClass::RateLimited => SessionFailureKind::RateLimited,
                        RateLimitErrorClass::Server | RateLimitErrorClass::Other => {
                            SessionFailureKind::Transient
                        }
                    };
                    let mut failure =
                        session_failure(kind, error.to_string(), successful_reads > 0);
                    if kind == SessionFailureKind::RateLimited {
                        failure.suggested_retry_seconds = retry_after_seconds(error);
                    }
                    return Err(failure);
                }
                let result = message.get("result").ok_or_else(|| {
                    session_failure(
                        SessionFailureKind::Transient,
                        "rate-limit read returned no result",
                        successful_reads > 0,
                    )
                })?;
                match select_codex_rate_limit(result, source) {
                    Ok(mut snapshot) => {
                        let identity = current_account.as_ref().ok_or_else(|| {
                            session_failure(
                                SessionFailureKind::Authentication,
                                "quota read completed without an account partition",
                                successful_reads > 0,
                            )
                        })?;
                        snapshot.account_key = identity.account_key.clone();
                        if snapshot.plan.is_none() {
                            snapshot.plan = identity.plan.clone();
                        }
                        if persist_snapshot(connection, snapshot.clone()).map_err(|error| {
                            session_failure(
                                SessionFailureKind::Transient,
                                error,
                                successful_reads > 0,
                            )
                        })? {
                            append_log(
                                home,
                                &format!(
                                    "snapshot {}% reset={:?} source={}",
                                    snapshot.used_percent, snapshot.reset_at_ms, snapshot.source
                                ),
                            );
                        }
                        successful_reads = successful_reads.saturating_add(1);
                        record_collector_success(connection).map_err(|error| {
                            session_failure(SessionFailureKind::Transient, error, true)
                        })?;
                    }
                    Err(error) => {
                        append_log(home, &format!("unable to parse rate limits: {error}"))
                    }
                }
            }
            Ok(ServerEvent::Invalid(error)) => {
                append_log(home, &format!("invalid App Server message: {error}"));
            }
            Ok(ServerEvent::Closed) => {
                return Err(session_failure(
                    SessionFailureKind::Transient,
                    "App Server connection closed",
                    successful_reads > 0,
                ))
            }
            Err(RecvTimeoutError::Timeout) => {
                if current_account.is_some() && pending_read.is_none() {
                    pending_read = Some((next_id, "fallback"));
                    send_rate_limit_read(&mut server, connection, next_id).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    next_id += 1;
                }
                if current_account.is_some() && pending_usage.is_none() {
                    pending_usage = Some(next_id);
                    send_usage_read(&mut server, connection, next_id).map_err(|error| {
                        session_failure(SessionFailureKind::Transient, error, successful_reads > 0)
                    })?;
                    next_id += 1;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(session_failure(
                    SessionFailureKind::Transient,
                    "App Server reader stopped",
                    successful_reads > 0,
                ));
            }
        }
    }
}

fn run_recorder(home: &Path) -> Result<(), String> {
    prepare_home(home)?;
    let _pid_guard = acquire_pid(home)?;
    let connection = open_database(home)?;
    start_usage_recorder(home.to_path_buf());
    append_log(home, "recorder started");
    let mut authentication_failures = 0_u32;
    let mut transient_failures = 0_u32;

    loop {
        let binary = match discover_codex_binary() {
            Ok(binary) => binary,
            Err(error) => {
                append_log(home, &error);
                transient_failures = transient_failures.saturating_add(1);
                let delay = transient_retry_delay(transient_failures);
                let failure = session_failure(SessionFailureKind::Transient, error, false);
                let _ = record_collector_failure(&connection, &failure, transient_failures, delay);
                thread::sleep(Duration::from_secs(delay));
                continue;
            }
        };
        match run_server_session(home, &connection, &binary) {
            Ok(()) => {
                authentication_failures = 0;
                transient_failures = 0;
            }
            Err(failure) => {
                append_log(
                    home,
                    &format!(
                        "App Server session ended class={:?}: {}",
                        failure.kind, failure.message
                    ),
                );
                let (failures, delay) = match failure.kind {
                    SessionFailureKind::Authentication => {
                        if failure.had_success {
                            authentication_failures = 1;
                        } else {
                            authentication_failures = authentication_failures.saturating_add(1);
                        }
                        transient_failures = 0;
                        (
                            authentication_failures,
                            authentication_retry_delay(authentication_failures),
                        )
                    }
                    SessionFailureKind::RateLimited | SessionFailureKind::Transient => {
                        if failure.had_success {
                            transient_failures = 1;
                        } else {
                            transient_failures = transient_failures.saturating_add(1);
                        }
                        authentication_failures = 0;
                        let base_delay = transient_retry_delay(transient_failures);
                        let delay = failure
                            .suggested_retry_seconds
                            .map(|suggested| suggested.max(base_delay).min(21_600))
                            .unwrap_or(base_delay);
                        (transient_failures, delay)
                    }
                };
                let _ = record_collector_failure(&connection, &failure, failures, delay);
                append_log(home, &format!("next App Server recovery in {delay}s"));
                thread::sleep(Duration::from_secs(delay));
            }
        }
    }
}

fn start_usage_recorder(home: PathBuf) {
    thread::spawn(move || {
        let interval = env::var("CODEX_TOKEN_SCAN_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TOKEN_SCAN_SECONDS)
            .clamp(5, 3_600);
        loop {
            match open_database(&home) {
                Ok(mut connection) => match usage::ingest_once(&mut connection) {
                    Ok(summary) => {
                        if summary.events_inserted > 0 || summary.malformed_lines > 0 {
                            append_log(
                                &home,
                                &format!(
                                    "token sync inserted={} duplicates={} malformed={} files_changed={}",
                                    summary.events_inserted,
                                    summary.duplicate_events_skipped,
                                    summary.malformed_lines,
                                    summary.files_changed
                                ),
                            );
                        }
                    }
                    Err(error) => {
                        usage::record_ingest_error(&connection, &error);
                        append_log(&home, &format!("token sync failed: {error}"));
                    }
                },
                Err(error) => append_log(&home, &format!("token database failed: {error}")),
            }
            thread::sleep(Duration::from_secs(interval));
        }
    });
}

fn acquire_pid(home: &Path) -> Result<PidGuard, String> {
    let path = pid_path(home);
    if let Ok(existing) = fs::read_to_string(&path) {
        if let Ok(pid) = existing.trim().parse::<u32>() {
            if process_exists(pid) {
                return Err(format!("recorder is already running with PID {pid}"));
            }
        }
        let _ = fs::remove_file(&path);
    }
    let pid = std::process::id();
    fs::write(&path, pid.to_string())
        .map_err(|error| format!("unable to create PID file: {error}"))?;
    Ok(PidGuard { path, pid })
}

fn process_exists(pid: u32) -> bool {
    let mut command = Command::new("tasklist.exe");
    command.args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"]);
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    command
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\"")))
        .unwrap_or(false)
}

fn dashboard_process(home: &Path) -> Option<(u32, u16)> {
    let state = fs::read_to_string(dashboard_pid_path(home)).ok()?;
    let mut fields = state.split_whitespace();
    let pid = fields.next()?.parse().ok()?;
    let port = fields.next()?.parse().ok()?;
    Some((pid, port))
}

fn dashboard_ready(port: u16, timeout: Duration) -> bool {
    let Ok(mut stream) =
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], port)), timeout)
    else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    if write!(
        stream,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return false;
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }
    response.starts_with("HTTP/1.1 200") && response.contains("codex-quota-ledger")
}

fn launch_dashboard_browser(url: &str) -> Result<(), String> {
    let mut command = Command::new("explorer.exe");
    command.arg(url);
    command
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("unable to open the dashboard browser: {error}"))
}

#[cfg(windows)]
fn spawn_dashboard(executable: &Path, port: u16) -> Result<u32, String> {
    let script = "$process = Start-Process -FilePath $env:CODEX_QUOTA_DASHBOARD_EXE -ArgumentList @('serve', '--port', $env:CODEX_QUOTA_DASHBOARD_PORT) -WindowStyle Hidden -PassThru; $process.Id";
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("CODEX_QUOTA_DASHBOARD_EXE", executable)
        .env("CODEX_QUOTA_DASHBOARD_PORT", port.to_string())
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .map_err(|error| format!("unable to start the dashboard: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "unable to start the dashboard: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .map_err(|error| format!("unable to read the dashboard process id: {error}"))
}

#[cfg(not(windows))]
fn spawn_dashboard(executable: &Path, port: u16) -> Result<u32, String> {
    Command::new(executable)
        .args(["serve", "--port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|child| child.id())
        .map_err(|error| format!("unable to start the dashboard: {error}"))
}

fn stop_dashboard_process(pid: u32) {
    #[cfg(windows)]
    {
        let _ = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Stop-Process -Id $env:CODEX_QUOTA_DASHBOARD_PID -Force -ErrorAction SilentlyContinue",
            ])
            .env("CODEX_QUOTA_DASHBOARD_PID", pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .status();
    }
    #[cfg(not(windows))]
    {
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
}

fn close_dashboard(home: &Path) -> Result<(), String> {
    let state_path = dashboard_pid_path(home);
    if let Some((pid, _)) = dashboard_process(home) {
        stop_dashboard_process(pid);
        let _ = fs::remove_file(&state_path);
        println!("dashboard stopped");
    } else {
        println!("dashboard is not running");
    }
    Ok(())
}

fn open_dashboard(home: &Path, port: u16, launch_browser: bool) -> Result<(), String> {
    prepare_home(home)?;
    let state_path = dashboard_pid_path(home);
    if let Some((_pid, existing_port)) = dashboard_process(home) {
        if existing_port == port && dashboard_ready(port, Duration::from_secs(1)) {
            let url = format!("http://127.0.0.1:{port}");
            if launch_browser {
                launch_dashboard_browser(&url)?;
            }
            println!("{url}");
            return Ok(());
        }
        let _ = fs::remove_file(&state_path);
    }

    let executable = env::current_exe()
        .map_err(|error| format!("unable to locate dashboard executable: {error}"))?;
    let pid = spawn_dashboard(&executable, port)?;

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if dashboard_ready(port, Duration::from_millis(100)) {
            if let Err(error) = fs::write(&state_path, format!("{pid} {port}")) {
                stop_dashboard_process(pid);
                return Err(format!("unable to save dashboard process state: {error}"));
            }
            let url = format!("http://127.0.0.1:{port}");
            if launch_browser {
                launch_dashboard_browser(&url)?;
            }
            println!("{url}");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }

    stop_dashboard_process(pid);
    Err("dashboard did not become ready within 10 seconds".to_string())
}

fn discover_codex_binary() -> Result<PathBuf, String> {
    if let Some(path) = env::var_os("CODEX_CLI_PATH").map(PathBuf::from) {
        if path.is_file() {
            return Ok(path);
        }
    }

    let mut candidates = Vec::new();
    if let Some(local_app_data) = env::var_os("LOCALAPPDATA").map(PathBuf::from) {
        let bin = local_app_data.join("OpenAI").join("Codex").join("bin");
        let direct = bin.join("codex.exe");
        if direct.is_file() {
            candidates.push(direct);
        }
        if let Ok(entries) = fs::read_dir(&bin) {
            for entry in entries.flatten() {
                let versioned = entry.path().join("codex.exe");
                if versioned.is_file() {
                    candidates.push(versioned);
                }
            }
        }
    }

    if let Ok(output) = Command::new("where.exe").arg("codex.exe").output() {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let path = PathBuf::from(line.trim());
            if path.is_file() {
                candidates.push(path);
            }
        }
    }

    candidates.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(UNIX_EPOCH)
    });
    candidates
        .pop()
        .ok_or_else(|| "unable to find codex.exe; set CODEX_CLI_PATH".to_string())
}

fn run_schtasks(args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("schtasks.exe");
    command.args(args);
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    let output = command
        .output()
        .map_err(|error| format!("unable to run schtasks.exe: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn configure_task_settings() -> Result<(), String> {
    let script = r#"$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable -Hidden; Set-ScheduledTask -TaskName 'Codex Quota Recorder' -Settings $settings | Out-Null"#;
    let mut command = Command::new("powershell.exe");
    command.args([
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        script,
    ]);
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    let output = command
        .output()
        .map_err(|error| format!("unable to configure scheduled task: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "unable to configure scheduled task: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

fn install_task(home: &Path) -> Result<(), String> {
    prepare_home(home)?;
    let _ = open_database(home)?;
    let exe = env::current_exe()
        .map_err(|error| format!("unable to locate recorder executable: {error}"))?;
    let task_command = format!("\"{}\" run", exe.display());
    let result = run_schtasks(&[
        "/Create",
        "/F",
        "/SC",
        "ONLOGON",
        "/TN",
        TASK_NAME,
        "/TR",
        &task_command,
        "/RL",
        "LIMITED",
    ])?;
    configure_task_settings()?;
    println!("{result}");
    match run_schtasks(&["/Run", "/TN", TASK_NAME]) {
        Ok(result) => println!("{result}"),
        Err(error) => eprintln!("task installed but could not be started: {error}"),
    }
    Ok(())
}

fn stop_recorder(home: &Path) -> Result<(), String> {
    let path = pid_path(home);
    let pid = match fs::read_to_string(&path)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        Some(pid) => pid,
        None => {
            println!("recorder is not running");
            return Ok(());
        }
    };
    if process_exists(pid) {
        let mut command = Command::new("taskkill.exe");
        command.args(["/PID", &pid.to_string(), "/T", "/F"]);
        #[cfg(windows)]
        command.creation_flags(CREATE_NO_WINDOW);
        let output = command
            .output()
            .map_err(|error| format!("unable to stop recorder: {error}"))?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
        }
    }
    let _ = fs::remove_file(path);
    println!("recorder stopped");
    Ok(())
}

fn uninstall_task(home: &Path) -> Result<(), String> {
    let _ = stop_recorder(home);
    match run_schtasks(&["/Delete", "/F", "/TN", TASK_NAME]) {
        Ok(result) => println!("{result}"),
        Err(error) if error.contains("cannot find") || error.contains("找不到") => {
            println!("scheduled task is not installed")
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_weekly_secondary_window() {
        let value = json!({
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 31.0, "windowDurationMins": 300, "resetsAt": 1000},
                    "secondary": {"usedPercent": 7.0, "windowDurationMins": 10080, "resetsAt": 2000},
                    "plan": "prolite"
                }
            }
        });
        let result = select_codex_rate_limit(&value, "initial").expect("weekly window");
        assert_eq!(result.used_percent, 7.0);
        assert_eq!(result.remaining_percent, 93.0);
        assert_eq!(result.reset_at_ms, Some(2_000_000));
        assert_eq!(result.limit_id.as_deref(), Some("codex"));
        assert_eq!(result.plan.as_deref(), Some("prolite"));
    }

    #[test]
    fn parses_direct_weekly_window() {
        let value = json!({
            "rateLimits": [{
                "limitId": "codex",
                "usedPercent": 42.5,
                "durationMinutes": 10080,
                "resetAt": 1_700_000_000
            }]
        });
        let result = select_codex_rate_limit(&value, "read").expect("weekly window");
        assert_eq!(result.used_percent, 42.5);
        assert_eq!(result.remaining_percent, 57.5);
        assert_eq!(result.reset_at_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn authentication_rejection_requires_session_rebuild() {
        let error = json!({
            "code": -32001,
            "message": "Unauthorized: token_invalidated"
        });
        assert_eq!(
            classify_rate_limit_error(&error),
            RateLimitErrorClass::Authentication
        );
    }

    #[test]
    fn normal_fallback_is_low_frequency() {
        assert_eq!(DEFAULT_FALLBACK_SECONDS, 3_600);
        assert!((3_240..=3_960).contains(&jittered_fallback_seconds(3_600, 17)));
    }

    #[test]
    fn repeated_authentication_failures_open_circuit() {
        assert!(authentication_retry_delay(1) <= 15);
        assert_eq!(authentication_retry_delay(2), 3_600);
        assert_eq!(authentication_retry_delay(3), 7_200);
        assert_eq!(authentication_retry_delay(99), 21_600);
    }

    #[test]
    fn rate_limit_retry_after_is_honored() {
        let error = json!({
            "message": "Too many requests",
            "data": {"retryAfterMs": 90_001}
        });
        assert_eq!(
            classify_rate_limit_error(&error),
            RateLimitErrorClass::RateLimited
        );
        assert_eq!(retry_after_seconds(&error), Some(91));
    }

    #[test]
    fn collector_health_tracks_failure_and_recovery() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        let failure = session_failure(
            SessionFailureKind::Authentication,
            "token_invalidated",
            false,
        );
        record_collector_failure(&connection, &failure, 2, 3_600).expect("failure state");
        let degraded = collector_health(&connection).expect("degraded health");
        assert_eq!(degraded.state, "degraded");
        assert_eq!(degraded.consecutive_failures, 2);
        record_collector_success(&connection).expect("recovery state");
        let healthy = collector_health(&connection).expect("healthy health");
        assert_eq!(healthy.state, "healthy");
        assert_eq!(healthy.consecutive_failures, 0);
        assert_eq!(healthy.recoveries, 1);
    }

    #[test]
    fn rate_limit_notifications_are_auditable_without_storing_payloads() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        record_rate_limit_notification(&connection).expect("notification marker");
        let health = collector_health(&connection).expect("collector health");
        assert_eq!(health.notifications_total, 1);
        assert!(health.last_notification_at_ms.is_some());
    }

    #[test]
    fn database_records_changes_and_deduplicates_equal_values() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        connection
            .execute(
                "UPDATE current_account_state SET account_key='acct-test' WHERE id=1",
                [],
            )
            .expect("active account");
        let first = Snapshot {
            account_key: "acct-test".to_string(),
            observed_at_ms: 1_000,
            used_percent: 7.0,
            remaining_percent: 0.0,
            reset_at_ms: Some(9_000),
            duration_minutes: 10_080.0,
            limit_id: Some("codex".to_string()),
            plan: Some("prolite".to_string()),
            source: "initial".to_string(),
            raw_json: "{}".to_string(),
            observation_schema_version: QUOTA_OBSERVATION_SCHEMA_VERSION,
            collector_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        assert!(persist_snapshot(&connection, first.clone()).expect("insert"));
        assert!(!persist_snapshot(
            &connection,
            Snapshot {
                observed_at_ms: 2_000,
                ..first.clone()
            }
        )
        .expect("deduplicate"));
        assert!(persist_snapshot(
            &connection,
            Snapshot {
                observed_at_ms: 3_000,
                used_percent: 8.0,
                ..first
            }
        )
        .expect("changed insert"));
        let history = read_history(&connection, 10).expect("history");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].used_percent, 8.0);
        assert_eq!(history[1].remaining_percent, 93.0);
        let window: (i64, String) = connection
            .query_row(
                "SELECT last_seen_at_ms, status FROM quota_windows",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("quota window");
        assert_eq!(window, (3_000, "active".to_string()));
    }

    #[test]
    fn status_and_history_do_not_leak_the_previous_account_partition() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        for (account, observed_at_ms, used_percent) in
            [("acct-old", 2_000, 80.0), ("acct-current", 1_000, 10.0)]
        {
            persist_snapshot(
                &connection,
                Snapshot {
                    account_key: account.to_string(),
                    observed_at_ms,
                    used_percent,
                    remaining_percent: 100.0 - used_percent,
                    reset_at_ms: Some(9_000),
                    duration_minutes: 10_080.0,
                    limit_id: Some("codex".to_string()),
                    plan: Some("pro".to_string()),
                    source: "test".to_string(),
                    raw_json: "{}".to_string(),
                    observation_schema_version: QUOTA_OBSERVATION_SCHEMA_VERSION,
                    collector_version: env!("CARGO_PKG_VERSION").to_string(),
                },
            )
            .expect("snapshot");
        }
        connection
            .execute(
                "UPDATE current_account_state SET account_key='acct-current' WHERE id=1",
                [],
            )
            .expect("switch account");
        let latest = latest_snapshot(&connection)
            .expect("latest query")
            .expect("current snapshot");
        assert_eq!(latest.account_key, "acct-current");
        let history = read_history(&connection, 10).expect("current history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].account_key, "acct-current");
    }

    #[test]
    fn reset_timestamp_jitter_reuses_the_active_epoch() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        let base = Snapshot {
            account_key: "acct-test".to_string(),
            observed_at_ms: 1_000,
            used_percent: 7.0,
            remaining_percent: 93.0,
            reset_at_ms: Some(2_000_000),
            duration_minutes: 10_080.0,
            limit_id: Some("codex".to_string()),
            plan: Some("prolite".to_string()),
            source: "test".to_string(),
            raw_json: "{}".to_string(),
            observation_schema_version: QUOTA_OBSERVATION_SCHEMA_VERSION,
            collector_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        persist_snapshot(&connection, base.clone()).expect("first snapshot");
        persist_snapshot(
            &connection,
            Snapshot {
                observed_at_ms: 2_000,
                used_percent: 8.0,
                reset_at_ms: Some(2_000_000 + RESET_JITTER_TOLERANCE_MS),
                ..base
            },
        )
        .expect("jittered snapshot");
        let windows: i64 = connection
            .query_row("SELECT COUNT(*) FROM quota_windows", [], |row| row.get(0))
            .expect("window count");
        assert_eq!(windows, 1);
    }

    #[test]
    fn legacy_quota_windows_are_preserved_in_an_isolated_account_partition() {
        let connection = Connection::open_in_memory().expect("database");
        connection
            .execute_batch(
                "CREATE TABLE quota_windows (
                    limit_id TEXT NOT NULL,
                    reset_at_ms INTEGER NOT NULL,
                    duration_minutes REAL NOT NULL,
                    window_start_ms INTEGER NOT NULL,
                    first_seen_at_ms INTEGER NOT NULL,
                    last_seen_at_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (limit_id, reset_at_ms, duration_minutes)
                 );
                 INSERT INTO quota_windows VALUES
                    ('codex', 9000, 10080, 1000, 1000, 2000, 'active');",
            )
            .expect("legacy schema");
        initialize_database(&connection).expect("migration");
        let migrated: (String, String, i64) = connection
            .query_row(
                "SELECT account_key, limit_id, last_seen_at_ms FROM quota_windows",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("migrated window");
        assert_eq!(
            migrated,
            ("legacy-unknown".to_string(), "codex".to_string(), 2000)
        );
        connection
            .execute(
                "INSERT INTO quota_windows (
                    account_key, limit_id, reset_at_ms, duration_minutes,
                    window_start_ms, first_seen_at_ms, last_seen_at_ms, status
                 ) VALUES ('acct-new', 'codex', 9000, 10080, 1000, 1000, 2000, 'active')",
                [],
            )
            .expect("separate account window");
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM quota_windows", [], |row| row.get(0))
            .expect("window count");
        assert_eq!(count, 2);
    }

    #[test]
    fn bengalfox_is_not_silently_treated_as_codex() {
        let value = json!({
            "rateLimitsByLimitId": {
                "codex_bengalfox": {
                    "secondary": {
                        "usedPercent": 9.0,
                        "windowDurationMins": 10080,
                        "resetsAt": 2000
                    }
                }
            }
        });
        let error = select_codex_rate_limit(&value, "test").expect_err("must fail closed");
        assert!(error.contains("codex_bengalfox"));
    }

    #[test]
    fn account_identity_is_stable_and_does_not_retain_email() {
        let result = json!({
            "account": {
                "type": "chatgpt",
                "email": "Person@Example.com",
                "planType": "pro"
            },
            "requiresOpenaiAuth": true
        });
        let first = account_identity(&result).expect("identity");
        let second = account_identity(&json!({
            "account": {
                "type": "chatgpt",
                "email": "person@example.com",
                "planType": "pro"
            }
        }))
        .expect("normalized identity");
        assert_eq!(first.account_key, second.account_key);
        assert!(!first.account_key.contains("person"));
        assert_eq!(first.auth_type, "chatgpt");
    }

    #[test]
    fn official_usage_preserves_daily_data_and_null_availability() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_database(&connection).expect("schema");
        persist_official_usage(
            &connection,
            "acct-test",
            10,
            &json!({
                "summary": {"lifetimeTokens": 123, "peakDailyTokens": 45},
                "dailyUsageBuckets": [{"startDate": "2026-09-03", "tokens": 12}]
            }),
        )
        .expect("daily usage");
        persist_official_usage(
            &connection,
            "acct-test",
            20,
            &json!({"summary": {"lifetimeTokens": 124}, "dailyUsageBuckets": null}),
        )
        .expect("null usage");
        let observations: Vec<(i64, i64)> = connection
            .prepare(
                "SELECT observed_at_ms, daily_buckets_available
                 FROM official_usage_observations ORDER BY observed_at_ms",
            )
            .expect("statement")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        assert_eq!(observations, vec![(10, 1), (20, 0)]);
        let daily: (String, u64) = connection
            .query_row(
                "SELECT start_date, tokens FROM official_daily_usage",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("daily row");
        assert_eq!(daily, ("2026-09-03".to_string(), 12));
    }

    #[test]
    fn calendar_day_bounds_use_iana_timezone_rules() {
        let (start, end) =
            calendar_day_bounds_ms("2026-03-08", "America/New_York").expect("DST day");
        assert_eq!(end - start, 23 * 60 * 60 * 1_000);
        let (start, end) =
            calendar_day_bounds_ms("2026-09-03", "Asia/Shanghai").expect("normal day");
        assert_eq!(end - start, 24 * 60 * 60 * 1_000);
        assert!(calendar_day_bounds_ms("2026-09-03", "UTC+8").is_err());
    }

    #[test]
    fn capacity_estimate_uses_only_observed_positive_percent_change() {
        assert_eq!(implied_full_window_tokens(1_000_000, 20.0), Some(5_000_000));
        assert_eq!(implied_full_window_tokens(1_000_000, 0.0), None);
        assert_eq!(implied_full_window_tokens(1_000_000, -2.0), None);
    }
}
