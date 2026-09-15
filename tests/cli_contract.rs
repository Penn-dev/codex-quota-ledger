use rusqlite::Connection;
use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn test_home(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let home = std::env::temp_dir().join(format!(
        "codex-quota-ledger-cli-{label}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(home.join("codex").join("sessions")).unwrap();
    home
}

fn run_query(home: &PathBuf, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_codex-quota-ledger"))
        .args(args)
        .env("CODEX_QUOTA_HOME", home.join("ledger"))
        .env("CODEX_HOME", home.join("codex"))
        .output()
        .expect("run query command")
}

#[test]
fn version_reports_the_package_version() {
    let home = test_home("version");
    let output = run_query(&home, &["--version"]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("codex-quota-ledger {}", env!("CARGO_PKG_VERSION"))
    );
    fs::remove_dir_all(home).unwrap();
}

fn initialize_and_seed(home: &PathBuf) {
    let output = run_query(home, &["status"]);
    assert!(output.status.success());
    let connection = Connection::open(home.join("ledger").join("data").join("quota.sqlite"))
        .expect("open fixture database");
    connection
        .execute_batch(
            "UPDATE current_account_state SET account_key='acct-current' WHERE id=1;
             INSERT INTO quota_snapshots (
                 account_key, observed_at_ms, used_percent, remaining_percent,
                 reset_at_ms, duration_minutes, limit_id, plan, source
             ) VALUES
                 ('acct-old', 2000, 80, 20, 9000, 10080, 'codex', 'test', 'fixture'),
                 ('acct-current', 1000, 10, 90, 9000, 10080, 'codex', 'test', 'fixture');
             INSERT INTO quota_windows (
                 account_key, limit_id, reset_at_ms, duration_minutes,
                 window_start_ms, first_seen_at_ms, last_seen_at_ms, status
             ) VALUES
                 ('acct-old', 'codex', 9000, 10080, 100, 100, 2000, 'active'),
                 ('acct-current', 'codex', 9000, 10080, 100, 100, 1000, 'active');",
        )
        .expect("seed fixture database");
}

#[test]
fn status_uses_the_versioned_query_envelope() {
    let home = test_home("status-envelope");
    let output = run_query(&home, &["status"]);
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("status JSON");
    assert_eq!(value["schema"], "codex-quota-ledger.query");
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["command"], "status");
    assert_eq!(value["availability"], "partial");
    assert_eq!(value["evidenceSources"][0]["source"], "official_quota");
    assert_eq!(value["evidenceSources"][0]["confidence"], "direct");
    assert!(value["generatedAtMs"].as_i64().is_some());
    assert!(value["data"].is_object());
    assert!(value["issues"]
        .as_array()
        .is_some_and(|issues| !issues.is_empty()));
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn history_and_windows_are_versioned_and_account_scoped() {
    let home = test_home("history-windows");
    initialize_and_seed(&home);
    for command in ["history", "windows"] {
        let output = run_query(&home, &[command]);
        assert!(
            output.status.success(),
            "{command} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).expect("query JSON");
        assert_eq!(value["schema"], "codex-quota-ledger.query");
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["command"], command);
        assert_eq!(value["availability"], "complete");
        let rows = value["data"][command].as_array().expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["accountKey"], "acct-current");
    }
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn estimate_and_reconcile_expose_partial_evidence_in_the_common_envelope() {
    let home = test_home("estimate-reconcile");
    initialize_and_seed(&home);
    for command in ["estimate", "reconcile"] {
        let output = run_query(&home, &[command]);
        assert!(
            output.status.success(),
            "{command} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).expect("query JSON");
        assert_eq!(value["schema"], "codex-quota-ledger.query");
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["command"], command);
        assert_eq!(value["availability"], "partial");
        assert!(value["data"].is_object());
        assert!(value["issues"]
            .as_array()
            .is_some_and(|issues| !issues.is_empty()));
    }
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn unavailable_query_returns_a_machine_readable_error_without_local_paths() {
    let home = test_home("unavailable");
    let output = run_query(&home, &["estimate"]);
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).expect("error JSON");
    assert_eq!(value["schema"], "codex-quota-ledger.query");
    assert_eq!(value["command"], "estimate");
    assert_eq!(value["availability"], "unavailable");
    assert!(value["data"].is_null());
    assert_eq!(value["issues"][0]["code"], "query_unavailable");
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains(&home.to_string_lossy().to_string()));
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn diagnose_is_read_only_structured_and_redacts_internal_error_details() {
    let home = test_home("diagnose");
    let output = run_query(&home, &["diagnose"]);
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("diagnose JSON");
    assert_eq!(value["command"], "diagnose");
    assert_eq!(value["availability"], "partial");
    assert_eq!(value["data"]["privacy"]["storesPrompts"], false);
    assert_eq!(value["data"]["privacy"]["storesCredentials"], false);
    assert_eq!(value["data"]["notificationLiveObservation"], "pending");
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains("lastError\""));
    assert!(!rendered.contains(&home.to_string_lossy().to_string()));
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn export_is_versioned_account_scoped_and_omits_private_source_material() {
    let home = test_home("export");
    initialize_and_seed(&home);
    let connection = Connection::open(home.join("ledger").join("data").join("quota.sqlite"))
        .expect("open fixture database");
    connection
        .execute_batch(
            "UPDATE quota_snapshots
                SET raw_json='{\"prompt\":\"DO_NOT_EXPORT_RAW\"}'
              WHERE account_key='acct-current';
             INSERT INTO token_events (
                 account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                 model, input_tokens, cached_input_tokens, cache_write_input_tokens,
                 output_tokens, reasoning_output_tokens, total_input_tokens,
                 total_cached_input_tokens, total_cache_write_input_tokens,
                 total_output_tokens, total_reasoning_output_tokens,
                 source_path, provenance, ingested_at_ms
             ) VALUES (
                 'acct-current', 'DO_NOT_EXPORT_SESSION', 500, 500, 500,
                 'gpt-5.6-sol', 100, 20, 0, 10, 2, 100, 20, 0, 10, 2,
                 'DO_NOT_EXPORT_SOURCE_PATH', 'native', 500
             );
             INSERT INTO token_events (
                 account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
                 model, input_tokens, cached_input_tokens, cache_write_input_tokens,
                 output_tokens, reasoning_output_tokens, total_input_tokens,
                 total_cached_input_tokens, total_cache_write_input_tokens,
                 total_output_tokens, total_reasoning_output_tokens,
                 source_path, provenance, ingested_at_ms
             ) VALUES (
                 'acct-old', 'DO_NOT_EXPORT_OTHER_ACCOUNT', 600, 600, 600,
                 'gpt-5.6-sol', 900, 0, 0, 0, 0, 900, 0, 0, 0, 0,
                 'OTHER_ACCOUNT_SOURCE_PATH', 'native', 600
             );",
        )
        .expect("seed private source material");
    drop(connection);

    let output = run_query(&home, &["export"]);
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("export JSON");
    assert_eq!(value["command"], "export");
    assert_eq!(value["data"]["exportSchema"], "codex-quota-ledger.export");
    assert_eq!(value["data"]["exportSchemaVersion"], 1);
    assert_eq!(value["data"]["accountKey"], "acct-current");
    assert_eq!(value["data"]["localUsage"]["totals"]["inputTokens"], 100);
    let rendered = String::from_utf8(output.stdout).unwrap();
    for forbidden in [
        "DO_NOT_EXPORT_RAW",
        "DO_NOT_EXPORT_SESSION",
        "DO_NOT_EXPORT_SOURCE_PATH",
        "DO_NOT_EXPORT_OTHER_ACCOUNT",
        "sourcePath",
        "sessionId",
        "rawJson",
    ] {
        assert!(!rendered.contains(forbidden), "export leaked {forbidden}");
    }
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn sync_day_and_capacity_use_the_common_query_envelope() {
    let home = test_home("remaining-queries");
    initialize_and_seed(&home);
    for args in [
        vec!["sync"],
        vec!["day", "--date", "2026-09-04", "--timezone", "Asia/Shanghai"],
        vec!["capacity"],
    ] {
        let output = run_query(&home, &args);
        assert!(
            output.status.success(),
            "{} stderr={}",
            args[0],
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).expect("query JSON");
        assert_eq!(value["schema"], "codex-quota-ledger.query", "{}", args[0]);
        assert_eq!(value["schemaVersion"], 1, "{}", args[0]);
        assert_eq!(value["command"], args[0], "{}", args[0]);
        assert!(value["data"].is_object(), "{}", args[0]);
    }
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn open_starts_a_reusable_dashboard_outside_the_source_directory() {
    let home = test_home("open-dashboard");
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve dashboard port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let output = run_query(
        &home,
        &["open", "--port", &port.to_string(), "--no-browser"],
    );
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("http://127.0.0.1:{port}")
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect dashboard");
    write!(
        stream,
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    let second = run_query(
        &home,
        &["open", "--port", &port.to_string(), "--no-browser"],
    );
    assert!(second.status.success());

    let close = run_query(&home, &["close"]);
    assert!(close.status.success());
    for _ in 0..20 {
        if fs::remove_dir_all(&home).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("unable to remove dashboard test home");
}
