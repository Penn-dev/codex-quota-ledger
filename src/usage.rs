use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const PRICE_SOURCE: &str = "https://developers.openai.com/api/docs/models";
const PRICE_VERIFIED_ON: &str = "2026-08-27";
const PRICE_VERSION: &str = "openai-2026-08-27";
const INGEST_BATCH_EVENT_LIMIT: u64 = 256;
const INGEST_BATCH_BYTE_LIMIT: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
struct TokenUsage {
    input: u64,
    cached_input: u64,
    cache_write_input: u64,
    output: u64,
    reasoning_output: u64,
}

impl TokenUsage {
    fn from_value(value: &Value) -> Option<Self> {
        Some(Self {
            input: read_u64(value, "input_tokens")?,
            cached_input: read_u64(value, "cached_input_tokens").unwrap_or(0),
            cache_write_input: read_u64(value, "cache_write_input_tokens").unwrap_or(0),
            output: read_u64(value, "output_tokens")?,
            reasoning_output: read_u64(value, "reasoning_output_tokens").unwrap_or(0),
        })
    }

    fn delta(self, previous: Self) -> Self {
        Self {
            input: self.input.saturating_sub(previous.input),
            cached_input: self.cached_input.saturating_sub(previous.cached_input),
            cache_write_input: self
                .cache_write_input
                .saturating_sub(previous.cache_write_input),
            output: self.output.saturating_sub(previous.output),
            reasoning_output: self
                .reasoning_output
                .saturating_sub(previous.reasoning_output),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTotals {
    pub input_tokens: u64,
    pub uncached_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub events: u64,
    pub api_equivalent_cost_usd: Option<f64>,
    pub priced_events: u64,
    pub unpriced_events: u64,
    pub unpriced_input_tokens: u64,
    pub unpriced_output_tokens: u64,
}

impl UsageTotals {
    fn add(&mut self, usage: TokenUsage, cost: Option<f64>) {
        let cached = usage.cached_input.min(usage.input);
        let cache_write = usage
            .cache_write_input
            .min(usage.input.saturating_sub(cached));
        self.input_tokens = self.input_tokens.saturating_add(usage.input);
        self.cached_input_tokens = self.cached_input_tokens.saturating_add(cached);
        self.cache_write_input_tokens = self.cache_write_input_tokens.saturating_add(cache_write);
        self.uncached_input_tokens = self
            .uncached_input_tokens
            .saturating_add(usage.input.saturating_sub(cached + cache_write));
        self.output_tokens = self.output_tokens.saturating_add(usage.output);
        self.reasoning_output_tokens = self
            .reasoning_output_tokens
            .saturating_add(usage.reasoning_output.min(usage.output));
        self.events = self.events.saturating_add(1);
        if let Some(value) = cost {
            self.priced_events = self.priced_events.saturating_add(1);
            self.api_equivalent_cost_usd =
                Some(self.api_equivalent_cost_usd.unwrap_or(0.0) + value);
        } else {
            self.unpriced_events = self.unpriced_events.saturating_add(1);
            self.unpriced_input_tokens = self.unpriced_input_tokens.saturating_add(usage.input);
            self.unpriced_output_tokens = self.unpriced_output_tokens.saturating_add(usage.output);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageGroup {
    pub model: String,
    pub limit_id: Option<String>,
    pub service_tier: Option<String>,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageReport {
    pub from_ms: i64,
    pub to_ms: i64,
    #[serde(skip_serializing)]
    pub _session_root: PathBuf,
    pub files_scanned: u64,
    pub malformed_lines: u64,
    pub duplicate_events_skipped: u64,
    pub groups: Vec<UsageGroup>,
    pub totals: UsageTotals,
    pub pricing_source: &'static str,
    pub pricing_verified_on: &'static str,
    pub pricing_version: &'static str,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestSummary {
    pub scanned_at_ms: i64,
    pub files_checked: u64,
    pub files_changed: u64,
    pub events_inserted: u64,
    pub duplicate_events_skipped: u64,
    pub malformed_lines: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerStatus {
    pub last_scan_at_ms: Option<i64>,
    pub last_event_at_ms: Option<i64>,
    pub files_checked: u64,
    pub last_events_inserted: u64,
    pub total_events: u64,
    pub checkpoint_files: u64,
    pub unpriced_events: u64,
    pub last_error_present: bool,
    #[serde(skip_serializing)]
    pub last_error: Option<String>,
}

#[derive(Clone, Copy)]
struct Price {
    input: f64,
    cached_input: f64,
    output: f64,
    long_context_threshold: Option<u64>,
    long_context_input_multiplier: f64,
    long_context_output_multiplier: f64,
}

#[derive(Default)]
struct Checkpoint {
    offset: u64,
    file_len: u64,
    session_id: String,
    owner_session_id: String,
    model: String,
    service_tier: Option<String>,
    previous_total: Option<TokenUsage>,
    pending_usage_at_ms: Option<i64>,
}

struct IngestBatch {
    changed: bool,
    events_inserted: u64,
    duplicates: u64,
    malformed: u64,
    last_event_at: Option<i64>,
    more_available: bool,
}

fn read_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn timestamp_ms(value: &Value) -> Option<i64> {
    let timestamp = value.get("timestamp")?.as_str()?;
    OffsetDateTime::parse(timestamp, &Rfc3339)
        .ok()
        .map(|value| value.unix_timestamp_nanos() / 1_000_000)
        .and_then(|value| i64::try_from(value).ok())
}

fn model_response_at_ms(value: &Value) -> Option<i64> {
    if value.get("type").and_then(Value::as_str) != Some("response_item") {
        return None;
    }
    let payload = value.get("payload")?;
    let item_type = payload.get("type").and_then(Value::as_str)?;
    let generated_by_model = match item_type {
        "reasoning" => true,
        "message" => payload.get("role").and_then(Value::as_str) == Some("assistant"),
        value => value.ends_with("_call"),
    };
    generated_by_model.then(|| timestamp_ms(value)).flatten()
}

fn discover_codex_home() -> Result<PathBuf, String> {
    if let Some(value) = env::var_os("CODEX_HOME") {
        return Ok(PathBuf::from(value));
    }
    env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .map(|home| home.join(".codex"))
        .ok_or_else(|| "unable to locate Codex sessions; set CODEX_HOME".to_string())
}

fn discover_session_root() -> Result<PathBuf, String> {
    Ok(discover_codex_home()?.join("sessions"))
}

fn discover_session_roots() -> Result<Vec<PathBuf>, String> {
    let home = discover_codex_home()?;
    let sessions = home.join("sessions");
    if !sessions.is_dir() {
        return Err(format!(
            "unable to read {}: directory not found",
            sessions.display()
        ));
    }
    let mut roots = vec![sessions];
    let archived = home.join("archived_sessions");
    if archived.is_dir() {
        roots.push(archived);
    }
    Ok(roots)
}

fn collect_jsonl_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(root)
        .map_err(|error| format!("unable to read {}: {error}", root.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("unable to read directory entry: {error}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(&path, files)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }
    Ok(())
}

fn token_usage(
    payload: &Value,
    previous_total: Option<TokenUsage>,
) -> Option<(TokenUsage, TokenUsage)> {
    let info = payload.get("info")?;
    let total = TokenUsage::from_value(info.get("total_token_usage")?)?;
    if previous_total == Some(total) {
        return None;
    }
    let usage = info
        .get("last_token_usage")
        .and_then(TokenUsage::from_value)
        .unwrap_or_else(|| total.delta(previous_total.unwrap_or_default()));
    Some((usage, total))
}

pub fn initialize_ledger(connection: &Connection) -> Result<(), String> {
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(|error| format!("unable to configure SQLite busy timeout: {error}"))?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS token_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 account_key TEXT NOT NULL DEFAULT 'legacy-unknown',
                 session_id TEXT NOT NULL,
                 observed_at_ms INTEGER NOT NULL,
                 usage_at_ms INTEGER NOT NULL,
                 reported_at_ms INTEGER NOT NULL,
                 model TEXT NOT NULL,
                 limit_id TEXT,
                 service_tier TEXT,
                 input_tokens INTEGER NOT NULL,
                 cached_input_tokens INTEGER NOT NULL,
                 cache_write_input_tokens INTEGER NOT NULL,
                 output_tokens INTEGER NOT NULL,
                 reasoning_output_tokens INTEGER NOT NULL,
                 total_input_tokens INTEGER NOT NULL,
                 total_cached_input_tokens INTEGER NOT NULL,
                 total_cache_write_input_tokens INTEGER NOT NULL,
                 total_output_tokens INTEGER NOT NULL,
                 total_reasoning_output_tokens INTEGER NOT NULL,
                 source_path TEXT NOT NULL,
                 provenance TEXT NOT NULL,
                 ingested_at_ms INTEGER NOT NULL,
                 UNIQUE (
                     account_key, session_id, total_input_tokens, total_cached_input_tokens,
                     total_cache_write_input_tokens, total_output_tokens,
                     total_reasoning_output_tokens
                 )
             );
             CREATE INDEX IF NOT EXISTS idx_token_events_time
                 ON token_events(observed_at_ms, id);
             CREATE TABLE IF NOT EXISTS file_checkpoints (
                 path TEXT PRIMARY KEY,
                 offset INTEGER NOT NULL,
                 file_len INTEGER NOT NULL,
                 session_id TEXT NOT NULL,
                 owner_session_id TEXT NOT NULL,
                 model TEXT NOT NULL,
                 service_tier TEXT,
                 previous_input INTEGER,
                 previous_cached_input INTEGER,
                 previous_cache_write_input INTEGER,
                 previous_output INTEGER,
                 previous_reasoning_output INTEGER,
                 pending_usage_at_ms INTEGER,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS ingestion_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 last_scan_at_ms INTEGER,
                 last_event_at_ms INTEGER,
                 files_checked INTEGER NOT NULL DEFAULT 0,
                 last_events_inserted INTEGER NOT NULL DEFAULT 0,
                 last_error TEXT,
                 timestamp_schema_version INTEGER NOT NULL DEFAULT 2
             );
             INSERT OR IGNORE INTO ingestion_state (id) VALUES (1);
             CREATE TABLE IF NOT EXISTS current_account_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 account_key TEXT,
                 updated_at_ms INTEGER
             );
             INSERT OR IGNORE INTO current_account_state (id) VALUES (1);
             CREATE TABLE IF NOT EXISTS pricing_versions (
                 version TEXT NOT NULL,
                 model_prefix TEXT NOT NULL,
                 input_per_million REAL NOT NULL,
                 cached_input_per_million REAL NOT NULL,
                 output_per_million REAL NOT NULL,
                 source TEXT NOT NULL,
                 verified_on TEXT NOT NULL,
                 active INTEGER NOT NULL,
                 PRIMARY KEY (version, model_prefix)
             );",
        )
        .map_err(|error| format!("unable to initialize token ledger: {error}"))?;
    ensure_column(connection, "token_events", "service_tier", "TEXT")?;
    ensure_column(connection, "file_checkpoints", "service_tier", "TEXT")?;
    ensure_column(
        connection,
        "token_events",
        "account_key",
        "TEXT NOT NULL DEFAULT 'legacy-unknown'",
    )?;
    ensure_column(
        connection,
        "token_events",
        "usage_at_ms",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        connection,
        "token_events",
        "reported_at_ms",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        connection,
        "file_checkpoints",
        "pending_usage_at_ms",
        "INTEGER",
    )?;
    ensure_column(
        connection,
        "ingestion_state",
        "timestamp_schema_version",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    connection
        .execute(
            "UPDATE token_events SET usage_at_ms=observed_at_ms WHERE usage_at_ms=0",
            [],
        )
        .map_err(|error| format!("unable to initialize usage timestamps: {error}"))?;
    connection
        .execute(
            "UPDATE token_events SET reported_at_ms=observed_at_ms WHERE reported_at_ms=0",
            [],
        )
        .map_err(|error| format!("unable to initialize report timestamps: {error}"))?;
    connection
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_token_events_usage_time
             ON token_events(usage_at_ms, id)",
            [],
        )
        .map_err(|error| format!("unable to index usage timestamps: {error}"))?;
    ensure_column(
        connection,
        "pricing_versions",
        "long_context_threshold",
        "INTEGER",
    )?;
    ensure_column(
        connection,
        "pricing_versions",
        "long_context_input_multiplier",
        "REAL NOT NULL DEFAULT 1.0",
    )?;
    migrate_token_events_account_identity(connection)?;
    connection
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_token_events_time
                 ON token_events(observed_at_ms, id);
             CREATE INDEX IF NOT EXISTS idx_token_events_usage_time
                 ON token_events(usage_at_ms, id);",
        )
        .map_err(|error| format!("unable to recreate token event indexes: {error}"))?;
    ensure_column(
        connection,
        "pricing_versions",
        "long_context_output_multiplier",
        "REAL NOT NULL DEFAULT 1.0",
    )?;
    for (prefix, price) in [
        (
            "gpt-5.6-sol",
            Price {
                input: 4.0,
                cached_input: 0.4,
                output: 20.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
        ),
        (
            "gpt-5.6-terra",
            Price {
                input: 2.0,
                cached_input: 0.2,
                output: 12.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
        ),
        (
            "gpt-5.6-luna",
            Price {
                input: 0.2,
                cached_input: 0.02,
                output: 1.2,
                long_context_threshold: None,
                long_context_input_multiplier: 1.0,
                long_context_output_multiplier: 1.0,
            },
        ),
        (
            "gpt-5.6",
            Price {
                input: 4.0,
                cached_input: 0.4,
                output: 20.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
        ),
        (
            "gpt-5.5",
            Price {
                input: 5.0,
                cached_input: 0.5,
                output: 30.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
        ),
        (
            "gpt-5.4-mini",
            Price {
                input: 0.75,
                cached_input: 0.075,
                output: 4.5,
                long_context_threshold: None,
                long_context_input_multiplier: 1.0,
                long_context_output_multiplier: 1.0,
            },
        ),
        (
            "gpt-5.4",
            Price {
                input: 2.5,
                cached_input: 0.25,
                output: 15.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
        ),
    ] {
        connection
            .execute(
                "INSERT OR IGNORE INTO pricing_versions (
                     version, model_prefix, input_per_million,
                     cached_input_per_million, output_per_million,
                     source, verified_on, active, long_context_threshold,
                     long_context_input_multiplier, long_context_output_multiplier
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10)
                 ON CONFLICT(version, model_prefix) DO UPDATE SET
                     input_per_million=excluded.input_per_million,
                     cached_input_per_million=excluded.cached_input_per_million,
                     output_per_million=excluded.output_per_million,
                     source=excluded.source,
                     verified_on=excluded.verified_on,
                     active=excluded.active,
                     long_context_threshold=excluded.long_context_threshold,
                     long_context_input_multiplier=excluded.long_context_input_multiplier,
                     long_context_output_multiplier=excluded.long_context_output_multiplier",
                params![
                    PRICE_VERSION,
                    prefix,
                    price.input,
                    price.cached_input,
                    price.output,
                    PRICE_SOURCE,
                    PRICE_VERIFIED_ON,
                    price.long_context_threshold,
                    price.long_context_input_multiplier,
                    price.long_context_output_multiplier
                ],
            )
            .map_err(|error| format!("unable to seed pricing version: {error}"))?;
    }
    Ok(())
}

fn migrate_token_events_account_identity(connection: &Connection) -> Result<(), String> {
    let mut has_account_unique = false;
    let mut indexes = connection
        .prepare("PRAGMA index_list(token_events)")
        .map_err(|error| format!("unable to inspect token event indexes: {error}"))?;
    let rows = indexes
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
        })
        .map_err(|error| format!("unable to query token event indexes: {error}"))?;
    for row in rows {
        let (name, unique) = row.map_err(|error| format!("unable to read token index: {error}"))?;
        if unique == 0 {
            continue;
        }
        let escaped = name.replace('\'', "''");
        let mut info = connection
            .prepare(&format!("PRAGMA index_info('{escaped}')"))
            .map_err(|error| format!("unable to inspect token index {name}: {error}"))?;
        let columns = info
            .query_map([], |row| row.get::<_, String>(2))
            .map_err(|error| format!("unable to query token index {name}: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("unable to read token index {name}: {error}"))?;
        if columns.first().map(String::as_str) == Some("account_key") {
            has_account_unique = true;
            break;
        }
    }
    drop(indexes);
    if has_account_unique {
        return Ok(());
    }
    connection
        .execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE token_events RENAME TO token_events_legacy;
             CREATE TABLE token_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 account_key TEXT NOT NULL DEFAULT 'legacy-unknown',
                 session_id TEXT NOT NULL,
                 observed_at_ms INTEGER NOT NULL,
                 usage_at_ms INTEGER NOT NULL,
                 reported_at_ms INTEGER NOT NULL,
                 model TEXT NOT NULL,
                 limit_id TEXT,
                 service_tier TEXT,
                 input_tokens INTEGER NOT NULL,
                 cached_input_tokens INTEGER NOT NULL,
                 cache_write_input_tokens INTEGER NOT NULL,
                 output_tokens INTEGER NOT NULL,
                 reasoning_output_tokens INTEGER NOT NULL,
                 total_input_tokens INTEGER NOT NULL,
                 total_cached_input_tokens INTEGER NOT NULL,
                 total_cache_write_input_tokens INTEGER NOT NULL,
                 total_output_tokens INTEGER NOT NULL,
                 total_reasoning_output_tokens INTEGER NOT NULL,
                 source_path TEXT NOT NULL,
                 provenance TEXT NOT NULL,
                 ingested_at_ms INTEGER NOT NULL,
                 UNIQUE (
                     account_key, session_id, total_input_tokens,
                     total_cached_input_tokens, total_cache_write_input_tokens,
                     total_output_tokens, total_reasoning_output_tokens
                 )
             );
             INSERT INTO token_events (
                 id, account_key, session_id, observed_at_ms, usage_at_ms,
                 reported_at_ms, model, limit_id, service_tier, input_tokens,
                 cached_input_tokens, cache_write_input_tokens, output_tokens,
                 reasoning_output_tokens, total_input_tokens,
                 total_cached_input_tokens, total_cache_write_input_tokens,
                 total_output_tokens, total_reasoning_output_tokens, source_path,
                 provenance, ingested_at_ms
              ) SELECT id, account_key, session_id, observed_at_ms, usage_at_ms,
                       reported_at_ms, model, limit_id, service_tier, input_tokens,
                      cached_input_tokens, cache_write_input_tokens, output_tokens,
                      reasoning_output_tokens, total_input_tokens,
                      total_cached_input_tokens, total_cache_write_input_tokens,
                      total_output_tokens, total_reasoning_output_tokens, source_path,
                      provenance, ingested_at_ms
               FROM token_events_legacy;
             DROP TABLE token_events_legacy;
             COMMIT;",
        )
        .map_err(|error| format!("unable to migrate token events by account: {error}"))
}

fn ensure_column(
    connection: &Connection,
    table_name: &str,
    column_name: &str,
    declaration: &str,
) -> Result<(), String> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table_name})"))
        .map_err(|error| format!("unable to inspect {table_name} schema: {error}"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| format!("unable to inspect pricing columns: {error}"))?;
    for column in columns {
        if column.map_err(|error| format!("unable to read pricing column: {error}"))? == column_name
        {
            return Ok(());
        }
    }
    connection
        .execute(
            &format!("ALTER TABLE {table_name} ADD COLUMN {column_name} {declaration}"),
            [],
        )
        .map_err(|error| format!("unable to migrate {table_name} schema: {error}"))?;
    Ok(())
}

fn ledger_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn checkpoint(connection: &Connection, path: &Path) -> Result<Checkpoint, String> {
    connection
        .query_row(
            "SELECT offset, file_len, session_id, owner_session_id, model, service_tier,
                     previous_input, previous_cached_input,
                     previous_cache_write_input, previous_output,
                     previous_reasoning_output, pending_usage_at_ms
             FROM file_checkpoints WHERE path = ?1",
            [path.to_string_lossy().as_ref()],
            |row| {
                let previous_input: Option<u64> = row.get(6)?;
                let previous_cached_input: Option<u64> = row.get(7)?;
                let previous_cache_write_input: Option<u64> = row.get(8)?;
                let previous_output: Option<u64> = row.get(9)?;
                let previous_reasoning_output: Option<u64> = row.get(10)?;
                Ok(Checkpoint {
                    offset: row.get(0)?,
                    file_len: row.get(1)?,
                    session_id: row.get(2)?,
                    owner_session_id: row.get(3)?,
                    model: row.get(4)?,
                    service_tier: row.get(5)?,
                    previous_total: previous_input.map(|input| TokenUsage {
                        input,
                        cached_input: previous_cached_input.unwrap_or(0),
                        cache_write_input: previous_cache_write_input.unwrap_or(0),
                        output: previous_output.unwrap_or(0),
                        reasoning_output: previous_reasoning_output.unwrap_or(0),
                    }),
                    pending_usage_at_ms: row.get(11)?,
                })
            },
        )
        .optional()
        .map(|value| value.unwrap_or_default())
        .map_err(|error| format!("unable to read file checkpoint: {error}"))
}

fn write_checkpoint(
    connection: &Connection,
    path: &Path,
    checkpoint: &Checkpoint,
    updated_at_ms: i64,
) -> Result<(), String> {
    let previous = checkpoint.previous_total.unwrap_or_default();
    connection
        .execute(
            "INSERT INTO file_checkpoints (
                 path, offset, file_len, session_id, owner_session_id, model, service_tier,
                 previous_input, previous_cached_input,
                 previous_cache_write_input, previous_output,
                 previous_reasoning_output, pending_usage_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(path) DO UPDATE SET
                 offset=excluded.offset, file_len=excluded.file_len,
                 session_id=excluded.session_id,
                  owner_session_id=excluded.owner_session_id,
                  model=excluded.model, service_tier=excluded.service_tier,
                  previous_input=excluded.previous_input,
                 previous_cached_input=excluded.previous_cached_input,
                 previous_cache_write_input=excluded.previous_cache_write_input,
                 previous_output=excluded.previous_output,
                 previous_reasoning_output=excluded.previous_reasoning_output,
                 pending_usage_at_ms=excluded.pending_usage_at_ms,
                 updated_at_ms=excluded.updated_at_ms",
            params![
                path.to_string_lossy().as_ref(),
                checkpoint.offset,
                checkpoint.file_len,
                checkpoint.session_id,
                checkpoint.owner_session_id,
                checkpoint.model,
                checkpoint.service_tier,
                checkpoint.previous_total.map(|_| previous.input),
                checkpoint.previous_total.map(|_| previous.cached_input),
                checkpoint
                    .previous_total
                    .map(|_| previous.cache_write_input),
                checkpoint.previous_total.map(|_| previous.output),
                checkpoint.previous_total.map(|_| previous.reasoning_output),
                checkpoint.pending_usage_at_ms,
                updated_at_ms
            ],
        )
        .map_err(|error| format!("unable to write file checkpoint: {error}"))?;
    Ok(())
}

#[cfg(test)]
fn ingest_file(
    connection: &mut Connection,
    path: &Path,
    scanned_at_ms: i64,
) -> Result<(bool, u64, u64, u64, Option<i64>), String> {
    let account_key = active_account_key(connection)?;
    ingest_file_for_account(connection, path, scanned_at_ms, &account_key)
}

fn ingest_file_for_account(
    connection: &mut Connection,
    path: &Path,
    scanned_at_ms: i64,
    account_key: &str,
) -> Result<(bool, u64, u64, u64, Option<i64>), String> {
    let mut changed = false;
    let mut inserted = 0;
    let mut duplicates = 0;
    let mut malformed = 0;
    let mut last_event_at = None;
    loop {
        let batch = ingest_file_batch_for_account(
            connection,
            path,
            scanned_at_ms,
            account_key,
            INGEST_BATCH_EVENT_LIMIT,
            INGEST_BATCH_BYTE_LIMIT,
        )?;
        changed |= batch.changed;
        inserted += batch.events_inserted;
        duplicates += batch.duplicates;
        malformed += batch.malformed;
        if let Some(value) = batch.last_event_at {
            last_event_at = Some(last_event_at.map_or(value, |last: i64| last.max(value)));
        }
        if !batch.more_available {
            break;
        }
        std::thread::yield_now();
    }
    Ok((changed, inserted, duplicates, malformed, last_event_at))
}

fn ingest_file_batch_for_account(
    connection: &mut Connection,
    path: &Path,
    scanned_at_ms: i64,
    account_key: &str,
    max_events: u64,
    max_bytes: u64,
) -> Result<IngestBatch, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("unable to stat {}: {error}", path.display()))?;
    let mut state = checkpoint(connection, path)?;
    if metadata.len() == state.file_len && state.offset == state.file_len {
        return Ok(IngestBatch {
            changed: false,
            events_inserted: 0,
            duplicates: 0,
            malformed: 0,
            last_event_at: None,
            more_available: false,
        });
    }
    if metadata.len() < state.offset {
        state = Checkpoint::default();
    }
    let mut file =
        File::open(path).map_err(|error| format!("unable to open {}: {error}", path.display()))?;
    file.seek(SeekFrom::Start(state.offset))
        .map_err(|error| format!("unable to seek {}: {error}", path.display()))?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("unable to start token transaction: {error}"))?;
    let mut reader = BufReader::new(file);
    let mut inserted = 0_u64;
    let mut duplicates = 0_u64;
    let mut malformed = 0_u64;
    let mut last_event_at = None;
    let mut processed_bytes = 0_u64;
    let mut processed_events = 0_u64;
    let mut partial_tail = false;
    macro_rules! finish_or_continue {
        () => {
            if processed_bytes >= max_bytes.max(1) || processed_events >= max_events.max(1) {
                break;
            } else {
                continue;
            }
        };
    }
    loop {
        let line_start = state.offset;
        let mut bytes = Vec::new();
        let read = reader
            .read_until(b'\n', &mut bytes)
            .map_err(|error| format!("unable to read {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        if !bytes.ends_with(b"\n") {
            state.offset = line_start;
            partial_tail = true;
            break;
        }
        state.offset = state.offset.saturating_add(read as u64);
        processed_bytes = processed_bytes.saturating_add(read as u64);
        let line = match std::str::from_utf8(&bytes) {
            Ok(line) => line.trim(),
            Err(_) => {
                malformed += 1;
                finish_or_continue!();
            }
        };
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => {
                malformed += 1;
                finish_or_continue!();
            }
        };
        let record_type = value.get("type").and_then(Value::as_str);
        if record_type == Some("session_meta") {
            let payload = value.get("payload");
            if let Some(session_id) = payload.and_then(|payload| {
                ["id", "session_id"]
                    .into_iter()
                    .find_map(|key| payload.get(key).and_then(Value::as_str))
            }) {
                if state.owner_session_id.is_empty() {
                    state.owner_session_id = session_id.to_string();
                }
                if state.session_id != session_id {
                    state.session_id = session_id.to_string();
                    state.service_tier = payload
                        .and_then(|payload| payload.get("service_tier"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    state.previous_total = None;
                    state.pending_usage_at_ms = None;
                }
            }
            finish_or_continue!();
        }
        if record_type == Some("turn_context") {
            let payload = value.get("payload");
            if let Some(model) = payload
                .and_then(|payload| payload.get("model"))
                .and_then(Value::as_str)
            {
                state.model = model.to_string();
            }
            if let Some(service_tier) = payload.and_then(|payload| payload.get("service_tier")) {
                state.service_tier = service_tier.as_str().map(str::to_string);
            }
            finish_or_continue!();
        }
        if let Some(usage_at_ms) = model_response_at_ms(&value) {
            state.pending_usage_at_ms = Some(usage_at_ms);
            finish_or_continue!();
        }
        let Some(payload) = value.get("payload") else {
            finish_or_continue!();
        };
        if record_type == Some("event_msg")
            && payload.get("type").and_then(Value::as_str) == Some("thread_settings_applied")
        {
            let settings = payload.get("thread_settings");
            if let Some(model) = payload
                .get("thread_settings")
                .and_then(|settings| settings.get("model"))
                .and_then(Value::as_str)
            {
                state.model = model.to_string();
            }
            if let Some(service_tier) = settings.and_then(|settings| settings.get("service_tier")) {
                state.service_tier = service_tier.as_str().map(str::to_string);
            }
            finish_or_continue!();
        }
        if record_type != Some("event_msg")
            || payload.get("type").and_then(Value::as_str) != Some("token_count")
        {
            finish_or_continue!();
        }
        processed_events = processed_events.saturating_add(1);
        let Some((usage, total)) = token_usage(payload, state.previous_total) else {
            duplicates += 1;
            finish_or_continue!();
        };
        state.previous_total = Some(total);
        let Some(reported_at_ms) = timestamp_ms(&value) else {
            malformed += 1;
            finish_or_continue!();
        };
        let usage_at_ms = state.pending_usage_at_ms.take().unwrap_or(reported_at_ms);
        let model =
            payload
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or(if state.model.is_empty() {
                    "unknown"
                } else {
                    &state.model
                });
        let limit_id = payload
            .get("rate_limits")
            .and_then(|limits| limits.get("limit_id"))
            .and_then(Value::as_str);
        let session_id = if state.session_id.is_empty() {
            path.to_string_lossy().into_owned()
        } else {
            state.session_id.clone()
        };
        let provenance =
            if !state.owner_session_id.is_empty() && state.owner_session_id != session_id {
                "legacy_replay"
            } else {
                "native"
            };
        let changed = transaction
            .execute(
                "INSERT INTO token_events (
                     account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                     model, limit_id, service_tier,
                     input_tokens, cached_input_tokens,
                     cache_write_input_tokens, output_tokens,
                     reasoning_output_tokens, total_input_tokens,
                     total_cached_input_tokens, total_cache_write_input_tokens,
                     total_output_tokens, total_reasoning_output_tokens,
                     source_path, provenance, ingested_at_ms
                 ) VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                     ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21
                  )
                  ON CONFLICT (
                      account_key, session_id, total_input_tokens, total_cached_input_tokens,
                      total_cache_write_input_tokens, total_output_tokens,
                      total_reasoning_output_tokens
                  ) DO UPDATE SET
                      observed_at_ms=excluded.observed_at_ms,
                      usage_at_ms=excluded.usage_at_ms,
                      reported_at_ms=excluded.reported_at_ms,
                      ingested_at_ms=excluded.ingested_at_ms
                  WHERE token_events.provenance='native'
                    AND excluded.provenance='native'
                    AND (
                        token_events.usage_at_ms<>excluded.usage_at_ms OR
                        token_events.reported_at_ms<>excluded.reported_at_ms
                    )",
                params![
                    account_key,
                    session_id,
                    reported_at_ms,
                    usage_at_ms,
                    reported_at_ms,
                    model,
                    limit_id,
                    state.service_tier,
                    usage.input,
                    usage.cached_input,
                    usage.cache_write_input,
                    usage.output,
                    usage.reasoning_output,
                    total.input,
                    total.cached_input,
                    total.cache_write_input,
                    total.output,
                    total.reasoning_output,
                    path.to_string_lossy().as_ref(),
                    provenance,
                    scanned_at_ms
                ],
            )
            .map_err(|error| format!("unable to insert token event: {error}"))?;
        if changed == 1 {
            inserted += 1;
            last_event_at =
                Some(last_event_at.map_or(reported_at_ms, |last: i64| last.max(reported_at_ms)));
        } else {
            duplicates += 1;
        }
        if processed_bytes >= max_bytes.max(1) || processed_events >= max_events.max(1) {
            break;
        }
    }
    state.file_len = metadata.len();
    write_checkpoint(&transaction, path, &state, scanned_at_ms)?;
    transaction
        .commit()
        .map_err(|error| format!("unable to commit token transaction: {error}"))?;
    Ok(IngestBatch {
        changed: true,
        events_inserted: inserted,
        duplicates,
        malformed,
        last_event_at,
        more_available: !partial_tail && state.offset < metadata.len(),
    })
}

pub fn ingest_once(connection: &mut Connection) -> Result<IngestSummary, String> {
    initialize_ledger(connection)?;
    let _ = required_active_account_key(connection)?;
    let timestamp_schema_version: i64 = connection
        .query_row(
            "SELECT timestamp_schema_version FROM ingestion_state WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("unable to read timestamp schema version: {error}"))?;
    if timestamp_schema_version < 2 {
        connection
            .execute("DELETE FROM file_checkpoints", [])
            .map_err(|error| {
                format!("unable to reset checkpoints for timestamp backfill: {error}")
            })?;
    }
    ingest_roots(connection, &discover_session_roots()?)
}

fn ingest_roots(connection: &mut Connection, roots: &[PathBuf]) -> Result<IngestSummary, String> {
    let account_key = required_active_account_key(connection)?;
    let scanned_at_ms = ledger_now_ms();
    let mut files = Vec::new();
    for root in roots {
        collect_jsonl_files(root, &mut files)?;
    }
    files.sort();
    let mut summary = IngestSummary {
        scanned_at_ms,
        files_checked: files.len() as u64,
        files_changed: 0,
        events_inserted: 0,
        duplicate_events_skipped: 0,
        malformed_lines: 0,
    };
    let mut last_event_at = None;
    for path in files {
        let (changed, inserted, duplicates, malformed, file_last_event) =
            ingest_file_for_account(connection, &path, scanned_at_ms, &account_key)?;
        summary.files_changed += u64::from(changed);
        summary.events_inserted += inserted;
        summary.duplicate_events_skipped += duplicates;
        summary.malformed_lines += malformed;
        if let Some(value) = file_last_event {
            last_event_at = Some(last_event_at.map_or(value, |last: i64| last.max(value)));
        }
    }
    connection
        .execute(
            "UPDATE ingestion_state SET
                 last_scan_at_ms=?1,
                 last_event_at_ms=COALESCE(?2, last_event_at_ms),
                 files_checked=?3,
                 last_events_inserted=?4,
                 last_error=NULL,
                 timestamp_schema_version=2
             WHERE id=1",
            params![
                summary.scanned_at_ms,
                last_event_at,
                summary.files_checked,
                summary.events_inserted
            ],
        )
        .map_err(|error| format!("unable to update ingestion state: {error}"))?;
    Ok(summary)
}

pub fn record_ingest_error(connection: &Connection, error: &str) {
    let _ = connection.execute(
        "UPDATE ingestion_state SET last_scan_at_ms=?1, last_error=?2 WHERE id=1",
        params![ledger_now_ms(), error],
    );
}

pub fn ledger_status(connection: &Connection) -> Result<LedgerStatus, String> {
    initialize_ledger(connection)?;
    connection
        .query_row(
            "SELECT
                 s.last_scan_at_ms, s.last_event_at_ms, s.files_checked,
                 s.last_events_inserted,
                 (SELECT COUNT(*) FROM token_events),
                 (SELECT COUNT(*) FROM file_checkpoints),
                 (SELECT COUNT(*) FROM token_events e
                    WHERE NOT EXISTS (
                      SELECT 1 FROM pricing_versions p
                      WHERE p.active=1 AND e.model LIKE p.model_prefix || '%'
                    )),
                 s.last_error
             FROM ingestion_state s WHERE s.id=1",
            [],
            |row| {
                let last_error: Option<String> = row.get(7)?;
                Ok(LedgerStatus {
                    last_scan_at_ms: row.get(0)?,
                    last_event_at_ms: row.get(1)?,
                    files_checked: row.get(2)?,
                    last_events_inserted: row.get(3)?,
                    total_events: row.get(4)?,
                    checkpoint_files: row.get(5)?,
                    unpriced_events: row.get(6)?,
                    last_error_present: last_error.is_some(),
                    last_error,
                })
            },
        )
        .map_err(|error| format!("unable to read ledger status: {error}"))
}

fn database_price(connection: &Connection, model: &str) -> Result<Option<Price>, String> {
    connection
        .query_row(
            "SELECT input_per_million, cached_input_per_million, output_per_million,
                    long_context_threshold, long_context_input_multiplier,
                    long_context_output_multiplier
             FROM pricing_versions
             WHERE active=1 AND ?1 LIKE model_prefix || '%'
             ORDER BY length(model_prefix) DESC
             LIMIT 1",
            [model],
            |row| {
                Ok(Price {
                    input: row.get(0)?,
                    cached_input: row.get(1)?,
                    output: row.get(2)?,
                    long_context_threshold: row.get(3)?,
                    long_context_input_multiplier: row.get(4)?,
                    long_context_output_multiplier: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(|error| format!("unable to read model price: {error}"))
}

fn event_cost_with_price(price: Price, usage: TokenUsage) -> f64 {
    let cached = usage.cached_input.min(usage.input);
    let cache_write = usage
        .cache_write_input
        .min(usage.input.saturating_sub(cached));
    let uncached = usage.input.saturating_sub(cached + cache_write);
    let long_context = price
        .long_context_threshold
        .is_some_and(|threshold| usage.input > threshold);
    let input_multiplier = if long_context {
        price.long_context_input_multiplier
    } else {
        1.0
    };
    let output_multiplier = if long_context {
        price.long_context_output_multiplier
    } else {
        1.0
    };
    (uncached as f64 * price.input * input_multiplier
        + cached as f64 * price.cached_input * input_multiplier
        + cache_write as f64 * price.input * 1.25 * input_multiplier
        + usage.output as f64 * price.output * output_multiplier)
        / 1_000_000.0
}

pub fn report_from_ledger(
    connection: &Connection,
    from_ms: i64,
    to_ms: i64,
) -> Result<UsageReport, String> {
    report_from_ledger_with_root(connection, from_ms, to_ms, discover_session_root()?)
}

fn report_from_ledger_with_root(
    connection: &Connection,
    from_ms: i64,
    to_ms: i64,
    root: PathBuf,
) -> Result<UsageReport, String> {
    initialize_ledger(connection)?;
    let mut statement = connection
        .prepare(
            "SELECT model, limit_id, service_tier, input_tokens, cached_input_tokens,
                    cache_write_input_tokens, output_tokens,
                    reasoning_output_tokens
             FROM token_events
             WHERE usage_at_ms >= ?1 AND usage_at_ms < ?2
                AND provenance = 'native'
                AND account_key = COALESCE(
                    (SELECT account_key FROM current_account_state WHERE id=1),
                    'legacy-unknown'
                )
             ORDER BY usage_at_ms, id",
        )
        .map_err(|error| format!("unable to prepare ledger report: {error}"))?;
    let rows = statement
        .query_map(params![from_ms, to_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                TokenUsage {
                    input: row.get(3)?,
                    cached_input: row.get(4)?,
                    cache_write_input: row.get(5)?,
                    output: row.get(6)?,
                    reasoning_output: row.get(7)?,
                },
            ))
        })
        .map_err(|error| format!("unable to query ledger report: {error}"))?;
    let mut grouped: BTreeMap<(String, Option<String>, Option<String>), UsageTotals> =
        BTreeMap::new();
    let mut prices: BTreeMap<String, Option<Price>> = BTreeMap::new();
    for row in rows {
        let (model, limit_id, service_tier, usage) =
            row.map_err(|error| format!("unable to decode token event: {error}"))?;
        let price = match prices.get(&model) {
            Some(price) => *price,
            None => {
                let price = database_price(connection, &model)?;
                prices.insert(model.clone(), price);
                price
            }
        };
        let is_default_tier = service_tier.as_deref().is_none_or(|tier| tier == "default");
        grouped
            .entry((model, limit_id, service_tier))
            .or_default()
            .add(
                usage,
                price
                    .filter(|_| is_default_tier)
                    .map(|price| event_cost_with_price(price, usage)),
            );
    }
    let mut totals = UsageTotals::default();
    let mut warnings = Vec::new();
    let excluded_legacy_events: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM token_events
             WHERE account_key='legacy-unknown'
               AND usage_at_ms >= ?1 AND usage_at_ms < ?2
               AND provenance='native'",
            params![from_ms, to_ms],
            |row| row.get(0),
        )
        .map_err(|error| format!("unable to count unassigned legacy events: {error}"))?;
    if excluded_legacy_events > 0 {
        warnings.push(format!(
            "{excluded_legacy_events} legacy token events have no verified account identity and are excluded from this account report."
        ));
    }
    let groups = grouped
        .into_iter()
        .map(|((model, limit_id, service_tier), group)| {
            if group.api_equivalent_cost_usd.is_none() {
                if let Some(tier) = service_tier.as_deref().filter(|tier| *tier != "default") {
                    warnings.push(format!(
                        "Model {model} service tier {tier} is not priced; its tokens remain visible and are excluded from cost."
                    ));
                } else {
                    warnings.push(format!(
                        "No active price for model {model}; its tokens are excluded from cost."
                    ));
                }
            }
            merge_totals(&mut totals, &group);
            UsageGroup {
                model,
                limit_id,
                service_tier,
                totals: group,
            }
        })
        .collect();
    Ok(UsageReport {
        from_ms,
        to_ms,
        _session_root: root,
        files_scanned: 0,
        malformed_lines: 0,
        duplicate_events_skipped: 0,
        groups,
        totals,
        pricing_source: PRICE_SOURCE,
        pricing_verified_on: PRICE_VERIFIED_ON,
        pricing_version: PRICE_VERSION,
        warnings,
    })
}

#[cfg(test)]
fn active_account_key(connection: &Connection) -> Result<String, String> {
    connection
        .query_row(
            "SELECT COALESCE(account_key, 'legacy-unknown')
             FROM current_account_state WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("unable to read active account partition: {error}"))
}

fn required_active_account_key(connection: &Connection) -> Result<String, String> {
    connection
        .query_row(
            "SELECT account_key FROM current_account_state
             WHERE id=1 AND account_key IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| format!("unable to read active account partition: {error}"))?
        .ok_or_else(|| {
            "account identity is not confirmed; local token ingestion is deferred".to_string()
        })
}

fn merge_totals(totals: &mut UsageTotals, group: &UsageTotals) {
    totals.input_tokens = totals.input_tokens.saturating_add(group.input_tokens);
    totals.uncached_input_tokens = totals
        .uncached_input_tokens
        .saturating_add(group.uncached_input_tokens);
    totals.cached_input_tokens = totals
        .cached_input_tokens
        .saturating_add(group.cached_input_tokens);
    totals.cache_write_input_tokens = totals
        .cache_write_input_tokens
        .saturating_add(group.cache_write_input_tokens);
    totals.output_tokens = totals.output_tokens.saturating_add(group.output_tokens);
    totals.reasoning_output_tokens = totals
        .reasoning_output_tokens
        .saturating_add(group.reasoning_output_tokens);
    totals.events = totals.events.saturating_add(group.events);
    totals.priced_events = totals.priced_events.saturating_add(group.priced_events);
    totals.unpriced_events = totals.unpriced_events.saturating_add(group.unpriced_events);
    totals.unpriced_input_tokens = totals
        .unpriced_input_tokens
        .saturating_add(group.unpriced_input_tokens);
    totals.unpriced_output_tokens = totals
        .unpriced_output_tokens
        .saturating_add(group.unpriced_output_tokens);
    totals.api_equivalent_cost_usd = match (
        totals.api_equivalent_cost_usd,
        group.api_equivalent_cost_usd,
    ) {
        (Some(total), Some(cost)) => Some(total + cost),
        (Some(total), None) => Some(total),
        (None, Some(cost)) => Some(cost),
        (None, None) => None,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn test_dir(label: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "codex-quota-ledger-{label}-{}-{}",
            std::process::id(),
            ledger_now_ms()
        ));
        fs::create_dir_all(&path).expect("test directory");
        path
    }

    fn event(total_input: u64, last_input: u64) -> String {
        format!(
            r#"{{"timestamp":"2026-08-27T00:00:00Z","type":"event_msg","payload":{{"type":"token_count","info":{{"total_token_usage":{{"input_tokens":{total_input},"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":10,"reasoning_output_tokens":2}},"last_token_usage":{{"input_tokens":{last_input},"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":10,"reasoning_output_tokens":2}}}},"rate_limits":{{"limit_id":"codex"}}}}}}"#
        )
    }

    fn native_prefix(session: &str) -> String {
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{session}\"}}}}\n\
             {{\"type\":\"turn_context\",\"payload\":{{\"model\":\"gpt-5.6-sol\"}}}}\n"
        )
    }

    #[test]
    fn partial_line_is_committed_only_after_newline() {
        let dir = test_dir("partial");
        let path = dir.join("rollout.jsonl");
        fs::write(&path, format!("{}{}", native_prefix("s1"), event(100, 100)))
            .expect("write partial");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        let first = ingest_file(&mut connection, &path, 1).expect("first ingest");
        assert_eq!(first.1, 0);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(b"\n")
            .expect("finish line");
        let second = ingest_file(&mut connection, &path, 2).expect("second ingest");
        assert_eq!(second.1, 1);
        let count: u64 = connection
            .query_row("SELECT COUNT(*) FROM token_events", [], |row| row.get(0))
            .expect("event count");
        assert_eq!(count, 1);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn restart_truncation_and_legacy_replay_do_not_duplicate_events() {
        let dir = test_dir("replay");
        let original = dir.join("original.jsonl");
        let replay = dir.join("replay.jsonl");
        let record = event(100, 100);
        fs::write(&original, format!("{}{}\n", native_prefix("s1"), record))
            .expect("write original");
        fs::write(
            &replay,
            format!(
                "{}{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"s1\"}}}}\n{}\n",
                native_prefix("outer"),
                record
            ),
        )
        .expect("write replay");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        assert_eq!(
            ingest_file(&mut connection, &original, 1)
                .expect("original")
                .1,
            1
        );
        assert_eq!(
            ingest_file(&mut connection, &replay, 2).expect("replay").1,
            0
        );
        assert_eq!(
            ingest_file(&mut connection, &original, 3)
                .expect("unchanged restart")
                .1,
            0
        );
        fs::write(
            &original,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"s1\"}}}}\n{}\n",
                record
            ),
        )
        .expect("truncate and rewrite");
        assert_eq!(
            ingest_file(&mut connection, &original, 4)
                .expect("truncated")
                .1,
            0
        );
        let count: u64 = connection
            .query_row("SELECT COUNT(*) FROM token_events", [], |row| row.get(0))
            .expect("event count");
        assert_eq!(count, 1);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn token_report_after_tool_output_keeps_model_response_time() {
        let dir = test_dir("usage-time");
        let path = dir.join("rollout.jsonl");
        let records = format!(
            "{}{}\n{}\n{}\n",
            native_prefix("s1"),
            r#"{"timestamp":"2026-08-28T00:00:40Z","type":"response_item","payload":{"type":"custom_tool_call"}}"#,
            r#"{"timestamp":"2026-08-28T00:00:49Z","type":"response_item","payload":{"type":"custom_tool_call_output"}}"#,
            event(100, 100).replace("2026-08-27T00:00:00Z", "2026-08-28T00:00:49Z")
        );
        fs::write(&path, records).expect("write crossing fixture");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        assert_eq!(
            ingest_file(&mut connection, &path, 1)
                .expect("crossing ingest")
                .1,
            1
        );
        let times: (i64, i64) = connection
            .query_row(
                "SELECT usage_at_ms, reported_at_ms FROM token_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("event times");
        assert_eq!(times, (1_787_875_240_000, 1_787_875_249_000));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn pending_usage_time_survives_incremental_checkpoint() {
        let dir = test_dir("pending-checkpoint");
        let path = dir.join("rollout.jsonl");
        fs::write(
            &path,
            format!(
                "{}{}\n",
                native_prefix("s1"),
                r#"{"timestamp":"2026-08-28T00:00:40Z","type":"response_item","payload":{"type":"custom_tool_call"}}"#
            ),
        )
        .expect("write model response");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        assert_eq!(
            ingest_file(&mut connection, &path, 1)
                .expect("response ingest")
                .1,
            0
        );
        let suffix = format!(
            "{}\n{}\n",
            r#"{"timestamp":"2026-08-28T00:00:49Z","type":"response_item","payload":{"type":"custom_tool_call_output"}}"#,
            event(100, 100).replace("2026-08-27T00:00:00Z", "2026-08-28T00:00:49Z")
        );
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(suffix.as_bytes())
            .expect("append token report");
        assert_eq!(
            ingest_file(&mut connection, &path, 2)
                .expect("token ingest")
                .1,
            1
        );
        let usage_at_ms: i64 = connection
            .query_row("SELECT usage_at_ms FROM token_events", [], |row| row.get(0))
            .expect("usage time");
        assert_eq!(usage_at_ms, 1_787_875_240_000);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn replay_backfills_existing_usage_time() {
        let dir = test_dir("timestamp-backfill");
        let path = dir.join("rollout.jsonl");
        let report = event(100, 100).replace("2026-08-27T00:00:00Z", "2026-08-28T00:00:49Z");
        fs::write(&path, format!("{}{}\n", native_prefix("s1"), report))
            .expect("write legacy event");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        ingest_file(&mut connection, &path, 1).expect("legacy ingest");
        connection
            .execute("DELETE FROM file_checkpoints", [])
            .expect("reset checkpoint");
        fs::write(
            &path,
            format!(
                "{}{}\n{}\n{}\n",
                native_prefix("s1"),
                r#"{"timestamp":"2026-08-28T00:00:40Z","type":"response_item","payload":{"type":"custom_tool_call"}}"#,
                r#"{"timestamp":"2026-08-28T00:00:49Z","type":"response_item","payload":{"type":"custom_tool_call_output"}}"#,
                report
            ),
        )
        .expect("write enriched history");
        assert_eq!(
            ingest_file(&mut connection, &path, 2)
                .expect("backfill ingest")
                .1,
            1
        );
        let times: (i64, i64) = connection
            .query_row(
                "SELECT usage_at_ms, reported_at_ms FROM token_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("backfilled times");
        assert_eq!(times, (1_787_875_240_000, 1_787_875_249_000));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn half_open_windows_assign_boundary_event_once() {
        let dir = test_dir("half-open-window");
        let path = dir.join("rollout.jsonl");
        fs::write(
            &path,
            format!(
                "{}{}\n",
                native_prefix("s1"),
                event(100, 100).replace("2026-08-27T00:00:00Z", "2026-08-28T00:00:44Z")
            ),
        )
        .expect("write boundary event");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        ingest_file(&mut connection, &path, 1).expect("boundary ingest");
        let boundary = 1_787_875_244_000;
        let before = report_from_ledger_with_root(&connection, 0, boundary, dir.clone())
            .expect("before report");
        let after =
            report_from_ledger_with_root(&connection, boundary, 1_787_875_250_000, dir.clone())
                .expect("after report");
        assert_eq!(before.totals.events, 0);
        assert_eq!(after.totals.events, 1);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn token_usage_prefers_last_and_skips_duplicate_total() {
        let payload = serde_json::json!({
            "info": {
                "total_token_usage": {
                    "input_tokens": 1200,
                    "cached_input_tokens": 1000,
                    "cache_write_input_tokens": 0,
                    "output_tokens": 100,
                    "reasoning_output_tokens": 40
                },
                "last_token_usage": {
                    "input_tokens": 200,
                    "cached_input_tokens": 100,
                    "cache_write_input_tokens": 0,
                    "output_tokens": 20,
                    "reasoning_output_tokens": 5
                }
            }
        });
        let previous = TokenUsage {
            input: 1000,
            cached_input: 900,
            output: 80,
            reasoning_output: 35,
            ..Default::default()
        };
        let (usage, total) = token_usage(&payload, Some(previous)).expect("new usage");
        assert_eq!(usage.input, 200);
        assert_eq!(usage.cached_input, 100);
        assert!(token_usage(&payload, Some(total)).is_none());
    }

    #[test]
    fn official_price_separates_cached_input() {
        let usage = TokenUsage {
            input: 100_000,
            cached_input: 90_000,
            output: 1_000,
            ..Default::default()
        };
        let cost = event_cost_with_price(
            Price {
                input: 4.0,
                cached_input: 0.4,
                output: 20.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
            usage,
        );
        assert!((cost - 0.096).abs() < 0.000_001);
    }

    #[test]
    fn long_context_multiplier_is_applied_per_event() {
        let usage = TokenUsage {
            input: 300_000,
            output: 10_000,
            ..Default::default()
        };
        let cost = event_cost_with_price(
            Price {
                input: 2.0,
                cached_input: 0.2,
                output: 12.0,
                long_context_threshold: Some(272_000),
                long_context_input_multiplier: 2.0,
                long_context_output_multiplier: 1.5,
            },
            usage,
        );
        assert!((cost - 1.38).abs() < 0.000_001);
    }

    #[test]
    fn model_without_long_context_surcharge_is_not_multiplied() {
        let usage = TokenUsage {
            input: 300_000,
            output: 10_000,
            ..Default::default()
        };
        let cost = event_cost_with_price(
            Price {
                input: 0.75,
                cached_input: 0.075,
                output: 4.5,
                long_context_threshold: None,
                long_context_input_multiplier: 1.0,
                long_context_output_multiplier: 1.0,
            },
            usage,
        );
        assert!((cost - 0.27).abs() < 0.000_001);
    }

    #[test]
    fn mixed_pricing_keeps_known_subtotal_and_unpriced_remainder() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        connection
            .execute(
                "INSERT INTO token_events (
                    session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                    model, limit_id, input_tokens, cached_input_tokens,
                    cache_write_input_tokens, output_tokens, reasoning_output_tokens,
                    total_input_tokens, total_cached_input_tokens,
                    total_cache_write_input_tokens, total_output_tokens,
                    total_reasoning_output_tokens, source_path, provenance, ingested_at_ms
                 ) VALUES
                    ('known', 10, 10, 10, 'gpt-5.6-sol', 'codex', 1000, 0, 0, 10, 0,
                     1000, 0, 0, 10, 0, 'known.jsonl', 'native', 10),
                    ('unknown', 20, 20, 20, 'future-model', 'codex', 2000, 500, 0, 20, 0,
                     2000, 500, 0, 20, 0, 'unknown.jsonl', 'native', 20)",
                [],
            )
            .expect("events");
        let report = report_from_ledger_with_root(&connection, 0, 100, PathBuf::from("test"))
            .expect("report");
        assert!(report.totals.api_equivalent_cost_usd.is_some());
        assert_eq!(report.totals.priced_events, 1);
        assert_eq!(report.totals.unpriced_events, 1);
        assert_eq!(report.totals.unpriced_input_tokens, 2_000);
        assert_eq!(report.totals.unpriced_output_tokens, 20);
    }

    #[test]
    fn explicit_fast_service_tier_keeps_tokens_but_is_not_default_priced() {
        let dir = test_dir("fast-tier");
        let path = dir.join("rollout.jsonl");
        fs::write(
            &path,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"fast\",\"service_tier\":\"priority\"}}}}\n\
                 {{\"type\":\"turn_context\",\"payload\":{{\"model\":\"gpt-5.6-sol\"}}}}\n{}\n",
                event(100, 100)
            ),
        )
        .expect("fast fixture");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        assert_eq!(
            ingest_file(&mut connection, &path, 1)
                .expect("fast ingest")
                .1,
            1
        );
        let report = report_from_ledger_with_root(&connection, 0, i64::MAX, dir.clone())
            .expect("fast report");
        assert_eq!(report.totals.input_tokens, 100);
        assert_eq!(report.totals.unpriced_events, 1);
        assert_eq!(report.totals.api_equivalent_cost_usd, None);
        assert_eq!(report.groups[0].service_tier.as_deref(), Some("priority"));
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("service tier priority") && warning.contains("not priced")
        }));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn active_and_archived_roots_deduplicate_the_same_session_event() {
        let dir = test_dir("active-archive");
        let active = dir.join("sessions");
        let archived = dir.join("archived_sessions");
        fs::create_dir_all(&active).expect("active root");
        fs::create_dir_all(&archived).expect("archive root");
        let record = format!("{}{}\n", native_prefix("same"), event(100, 100));
        fs::write(active.join("active.jsonl"), &record).expect("active fixture");
        fs::write(archived.join("archived.jsonl"), &record).expect("archive fixture");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        connection
            .execute(
                "UPDATE current_account_state SET account_key='acct-test' WHERE id=1",
                [],
            )
            .expect("active account");
        let summary = ingest_roots(&mut connection, &[active, archived]).expect("ingest roots");
        assert_eq!(summary.events_inserted, 1);
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM token_events", [], |row| row.get(0))
            .expect("event count");
        assert_eq!(count, 1);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn bulk_backfill_commits_resumable_bounded_batches() {
        let dir = test_dir("bounded-batches");
        let path = dir.join("rollout.jsonl");
        fs::write(
            &path,
            format!(
                "{}{}\n{}\n",
                native_prefix("batched"),
                event(100, 100),
                event(200, 100)
            ),
        )
        .expect("batch fixture");
        let mut connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");

        let first =
            ingest_file_batch_for_account(&mut connection, &path, 1, "acct-test", 1, u64::MAX)
                .expect("first batch");
        assert_eq!(first.events_inserted, 1);
        assert!(first.more_available);
        let first_offset: u64 = connection
            .query_row("SELECT offset FROM file_checkpoints", [], |row| row.get(0))
            .expect("first checkpoint");
        assert!(first_offset > 0 && first_offset < fs::metadata(&path).unwrap().len());

        let second =
            ingest_file_batch_for_account(&mut connection, &path, 2, "acct-test", 1, u64::MAX)
                .expect("second batch");
        assert_eq!(second.events_inserted, 1);
        assert!(!second.more_available);
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM token_events", [], |row| row.get(0))
            .expect("event count");
        assert_eq!(count, 2);
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn reports_only_the_active_account_partition() {
        let connection = Connection::open_in_memory().expect("database");
        initialize_ledger(&connection).expect("schema");
        connection
            .execute(
                "UPDATE current_account_state SET account_key='acct-a' WHERE id=1",
                [],
            )
            .expect("active account");
        for (session, account, input) in [("a", "acct-a", 100_u64), ("b", "acct-b", 200)] {
            connection
                .execute(
                    "INSERT INTO token_events (
                        account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                        model, limit_id, input_tokens, cached_input_tokens,
                        cache_write_input_tokens, output_tokens, reasoning_output_tokens,
                        total_input_tokens, total_cached_input_tokens,
                        total_cache_write_input_tokens, total_output_tokens,
                        total_reasoning_output_tokens, source_path, provenance, ingested_at_ms
                     ) VALUES (?1, ?2, 10, 10, 10, 'gpt-5.6-sol', 'codex', ?3, 0, 0,
                               10, 0, ?3, 0, 0, 10, 0, ?2, 'native', 10)",
                    params![account, session, input],
                )
                .expect("event");
        }
        let report = report_from_ledger_with_root(&connection, 0, 100, PathBuf::from("test"))
            .expect("report");
        assert_eq!(report.totals.input_tokens, 100);
        assert_eq!(report.totals.events, 1);
    }

    #[test]
    fn migration_allows_identical_event_identity_in_two_accounts() {
        let connection = Connection::open_in_memory().expect("database");
        connection
            .execute_batch(
                "CREATE TABLE token_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    observed_at_ms INTEGER NOT NULL,
                    usage_at_ms INTEGER NOT NULL,
                    reported_at_ms INTEGER NOT NULL,
                    model TEXT NOT NULL,
                    limit_id TEXT,
                    input_tokens INTEGER NOT NULL,
                    cached_input_tokens INTEGER NOT NULL,
                    cache_write_input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    reasoning_output_tokens INTEGER NOT NULL,
                    total_input_tokens INTEGER NOT NULL,
                    total_cached_input_tokens INTEGER NOT NULL,
                    total_cache_write_input_tokens INTEGER NOT NULL,
                    total_output_tokens INTEGER NOT NULL,
                    total_reasoning_output_tokens INTEGER NOT NULL,
                    source_path TEXT NOT NULL,
                    provenance TEXT NOT NULL,
                    ingested_at_ms INTEGER NOT NULL,
                    UNIQUE (session_id, total_input_tokens, total_cached_input_tokens,
                            total_cache_write_input_tokens, total_output_tokens,
                            total_reasoning_output_tokens)
                );",
            )
            .expect("legacy schema");
        initialize_ledger(&connection).expect("migration");
        for account in ["acct-a", "acct-b"] {
            connection
                .execute(
                    "INSERT INTO token_events (
                        account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                        model, input_tokens, cached_input_tokens, cache_write_input_tokens,
                        output_tokens, reasoning_output_tokens, total_input_tokens,
                        total_cached_input_tokens, total_cache_write_input_tokens,
                        total_output_tokens, total_reasoning_output_tokens,
                        source_path, provenance, ingested_at_ms
                     ) VALUES (?1, 'same', 1, 1, 1, 'gpt-5.6-sol', 1, 0, 0, 1, 0,
                               1, 0, 0, 1, 0, 'fixture', 'native', 1)",
                    [account],
                )
                .expect("partitioned duplicate");
        }
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM token_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 2);
    }
}
