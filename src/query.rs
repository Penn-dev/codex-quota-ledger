use serde::Serialize;

pub const SCHEMA: &str = "codex-quota-ledger.query";
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Availability {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    pub code: &'static str,
    pub severity: &'static str,
    pub message: String,
    pub source: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EvidenceSource {
    source: &'static str,
    confidence: &'static str,
    relationship: &'static str,
}

impl Issue {
    pub fn warning(code: &'static str, message: impl Into<String>, source: &'static str) -> Self {
        Self {
            code,
            severity: "warning",
            message: message.into(),
            source: Some(source),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<T> {
    schema: &'static str,
    schema_version: u32,
    command: String,
    generated_at_ms: i64,
    availability: Availability,
    evidence_sources: Vec<EvidenceSource>,
    data: T,
    issues: Vec<Issue>,
}

pub fn print<T: Serialize>(
    command: &str,
    generated_at_ms: i64,
    availability: Availability,
    data: T,
    issues: Vec<Issue>,
) -> Result<(), String> {
    let value = Envelope {
        schema: SCHEMA,
        schema_version: SCHEMA_VERSION,
        command: command.to_string(),
        generated_at_ms,
        availability,
        evidence_sources: evidence_sources(command),
        data,
        issues,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn evidence_sources(command: &str) -> Vec<EvidenceSource> {
    let direct = |source| EvidenceSource {
        source,
        confidence: "direct",
        relationship: "independent-evidence-source",
    };
    let derived = |source, confidence| EvidenceSource {
        source,
        confidence,
        relationship: "derived-not-official-accounting",
    };
    match command {
        "status" => vec![direct("official_quota"), direct("local_jsonl")],
        "history" => vec![direct("official_quota")],
        "windows" => vec![derived("reconstructed_quota_windows", "bounded")],
        "sync" => vec![direct("local_jsonl")],
        "estimate" => vec![
            direct("official_quota"),
            direct("local_jsonl"),
            derived("api_equivalent_estimate", "limited"),
        ],
        "reconcile" => vec![
            direct("official_quota"),
            direct("local_jsonl"),
            derived("optional_web_observation", "user-supplied"),
        ],
        "day" => vec![direct("official_daily_usage"), direct("local_jsonl")],
        "capacity" => vec![
            direct("official_quota"),
            direct("local_jsonl"),
            derived("historical_capacity", "limited"),
        ],
        "diagnose" => vec![direct("local_health_state")],
        "export" => vec![
            direct("official_quota"),
            direct("official_daily_usage"),
            direct("local_jsonl"),
            derived("reconstructed_quota_windows", "bounded"),
        ],
        _ => Vec::new(),
    }
}

pub fn is_query_command(command: &str) -> bool {
    matches!(
        command,
        "status"
            | "history"
            | "windows"
            | "sync"
            | "estimate"
            | "reconcile"
            | "day"
            | "capacity"
            | "diagnose"
            | "export"
    )
}

pub fn print_unavailable(command: &str, generated_at_ms: i64, message: &str) -> Result<(), String> {
    print(
        command,
        generated_at_ms,
        Availability::Unavailable,
        Option::<serde_json::Value>::None,
        vec![Issue {
            code: "query_unavailable",
            severity: "error",
            message: message.to_string(),
            source: None,
        }],
    )
}
