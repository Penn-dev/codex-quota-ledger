use rusqlite::Connection;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn test_home(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let home = std::env::temp_dir().join(format!(
        "codex-quota-ledger-web-{label}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(home.join("codex").join("sessions")).unwrap();
    home
}

struct RunningServer {
    child: Child,
    home: PathBuf,
    authority: String,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn start_server(label: &str) -> RunningServer {
    let home = test_home(label);
    start_server_with_home(home)
}

fn start_server_with_home(home: PathBuf) -> RunningServer {
    start_server_with_args(home, &[])
}

fn start_server_with_args(home: PathBuf, extra_args: &[&str]) -> RunningServer {
    let mut args = vec!["serve", "--port", "0"];
    args.extend_from_slice(extra_args);
    let mut child = Command::new(env!("CARGO_BIN_EXE_codex-quota-ledger"))
        .args(args)
        .env("CODEX_QUOTA_HOME", home.join("ledger"))
        .env("CODEX_HOME", home.join("codex"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start local web server");
    let stdout = child.stdout.take().expect("server stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read startup URL");
    let url = line
        .trim()
        .strip_prefix("Codex Quota Ledger: http://")
        .expect("server must report its loopback URL");
    assert!(url.starts_with("127.0.0.1:"), "unexpected listener: {url}");
    RunningServer {
        child,
        home,
        authority: url.to_string(),
    }
}

fn representative_home(label: &str) -> PathBuf {
    let home = test_home(label);
    let initialized = Command::new(env!("CARGO_BIN_EXE_codex-quota-ledger"))
        .arg("status")
        .env("CODEX_QUOTA_HOME", home.join("ledger"))
        .env("CODEX_HOME", home.join("codex"))
        .output()
        .expect("initialize representative ledger");
    assert!(initialized.status.success());
    let connection = Connection::open(home.join("ledger/data/quota.sqlite"))
        .expect("open representative ledger");
    connection
        .execute_batch(include_str!("fixtures/representative_dashboard.sql"))
        .expect("seed representative evidence");
    home
}

fn request(authority: &str, method: &str, path: &str, headers: &[(&str, &str)]) -> String {
    request_with_host(authority, authority, method, path, headers)
}

fn request_with_host(
    authority: &str,
    host: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> String {
    let mut stream = TcpStream::connect(authority).expect("connect to local web server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n"
    )
    .unwrap();
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").unwrap();
    }
    write!(stream, "\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

#[test]
fn web_routes_reject_non_loopback_hosts_cross_origin_requests_and_writes() {
    let server = start_server("request-guard");

    let hostile_host = request_with_host(&server.authority, "attacker.example", "GET", "/", &[]);
    assert!(hostile_host.starts_with("HTTP/1.1 403"), "{hostile_host}");

    let hostile_origin = request(
        &server.authority,
        "GET",
        "/",
        &[("Origin", "https://attacker.example")],
    );
    assert!(
        hostile_origin.starts_with("HTTP/1.1 403"),
        "{hostile_origin}"
    );

    let cross_site = request(
        &server.authority,
        "GET",
        "/api/dashboard",
        &[("Sec-Fetch-Site", "cross-site")],
    );
    assert!(cross_site.starts_with("HTTP/1.1 403"), "{cross_site}");

    let write = request(&server.authority, "POST", "/api/dashboard", &[]);
    assert!(write.starts_with("HTTP/1.1 405"), "{write}");
}

#[test]
fn serve_is_loopback_only_and_sets_browser_security_headers() {
    let server = start_server("security-headers");
    let response = request(&server.authority, "GET", "/", &[]);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let lower = response.to_ascii_lowercase();
    assert!(lower.contains("content-security-policy: default-src 'none'"));
    assert!(lower.contains("x-content-type-options: nosniff"));
    assert!(lower.contains("cache-control: no-store"));
    assert!(lower.contains("referrer-policy: no-referrer"));
}

fn response_body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("HTTP response body")
}

#[test]
fn dashboard_api_adapts_versioned_queries_and_preserves_unavailable_states() {
    let server = start_server("query-adapter");
    let response = request(&server.authority, "GET", "/api/dashboard", &[]);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let value: Value = serde_json::from_str(response_body(&response)).expect("dashboard JSON");
    assert_eq!(value["schema"], "codex-quota-ledger.dashboard");
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["queries"]["status"]["schemaVersion"], 1);
    assert_eq!(value["queries"]["status"]["availability"], "partial");
    assert_eq!(value["queries"]["estimate"]["availability"], "unavailable");
    assert_eq!(value["queries"]["capacity"]["availability"], "unavailable");
    assert_eq!(value["queries"]["reconcile"]["availability"], "unavailable");
    let rendered = response_body(&response);
    for forbidden in [
        "sessionRoot",
        "sourcePath",
        "databasePath",
        "lastError\"",
        "CODEX_HOME",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "dashboard leaked {forbidden}"
        );
    }
}

#[test]
fn representative_evidence_reaches_the_dashboard_without_losing_uncertainty() {
    let server = start_server_with_home(representative_home("representative"));
    let response = request(&server.authority, "GET", "/api/dashboard", &[]);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let value: Value = serde_json::from_str(response_body(&response)).expect("dashboard JSON");
    assert_eq!(
        value["queries"]["status"]["data"]["quota"]["usedPercent"],
        25.5
    );
    assert_eq!(
        value["queries"]["estimate"]["data"]["localUsage"]["totals"]["inputTokens"],
        26000
    );
    assert_eq!(
        value["queries"]["estimate"]["data"]["localUsage"]["totals"]["unpricedEvents"],
        1
    );
    assert!(
        value["queries"]["estimate"]["data"]["localUsage"]["totals"]["apiEquivalentCostUsd"]
            .as_f64()
            .is_some_and(|cost| cost > 0.0)
    );
    let epochs = value["queries"]["capacity"]["data"]["epochs"]
        .as_array()
        .expect("capacity epochs");
    assert_eq!(epochs.len(), 2);
    assert!(epochs[1]["relativeToPreviousEpoch"].as_f64().is_some());
    assert_eq!(
        value["queries"]["reconcile"]["data"]["webObservation"],
        Value::Null
    );
    assert_eq!(value["queries"]["reconcile"]["availability"], "partial");
}

#[test]
fn dashboard_times_out_safely_and_recovers_after_database_contention() {
    let home = representative_home("timeout-recovery");
    let connection = Connection::open(home.join("ledger/data/quota.sqlite"))
        .expect("open locked representative ledger");
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold writer lock");
    let server = start_server_with_args(home, &["--query-timeout-seconds", "1"]);

    let unavailable = request(&server.authority, "GET", "/api/dashboard", &[]);
    assert!(unavailable.starts_with("HTTP/1.1 503"), "{unavailable}");
    let value: Value = serde_json::from_str(response_body(&unavailable)).expect("safe 503 JSON");
    assert_eq!(value["availability"], "unavailable");
    assert_eq!(value["issues"][0]["code"], "query_adapter_unavailable");

    connection
        .execute_batch("ROLLBACK")
        .expect("release writer lock");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let recovered = loop {
        let response = request(&server.authority, "GET", "/api/dashboard", &[]);
        if response.starts_with("HTTP/1.1 200") || std::time::Instant::now() >= deadline {
            break response;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(recovered.starts_with("HTTP/1.1 200"), "{recovered}");
}

#[test]
fn concurrent_dashboard_refreshes_return_the_same_evidence() {
    let server = start_server_with_home(representative_home("concurrent-refresh"));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let authority = server.authority.clone();
        workers.push(std::thread::spawn(move || {
            request(&authority, "GET", "/api/dashboard", &[])
        }));
    }

    let responses: Vec<String> = workers
        .into_iter()
        .map(|worker| worker.join().expect("refresh worker"))
        .collect();
    for response in &responses {
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }
    let payloads: Vec<Value> = responses
        .iter()
        .map(|response| serde_json::from_str(response_body(response)).expect("dashboard JSON"))
        .collect();
    for payload in &payloads {
        assert_eq!(payload["schema"], "codex-quota-ledger.dashboard");
        assert_eq!(
            payload["queries"]["status"]["data"]["quota"]["usedPercent"],
            25.5
        );
        assert_eq!(
            payload["queries"]["estimate"]["data"]["localUsage"]["totals"]["inputTokens"],
            26000
        );
        assert_eq!(
            payload["queries"]["capacity"]["data"]["epochs"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            payload["queries"]["reconcile"]["data"]["webObservation"],
            Value::Null
        );
    }
}

#[test]
fn dashboard_drains_large_history_output_without_a_false_timeout() {
    let home = representative_home("large-query-output");
    let connection = Connection::open(home.join("ledger/data/quota.sqlite"))
        .expect("open representative ledger");
    connection
        .execute_batch(
            "WITH RECURSIVE sequence(n) AS (
                 SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 300
             )
             INSERT INTO quota_windows (
                 account_key, limit_id, reset_at_ms, duration_minutes,
                 window_start_ms, first_seen_at_ms, last_seen_at_ms, status
             )
             SELECT 'acct-demo', 'codex', -n * 604800000, 10080,
                    -(n + 1) * 604800000, -(n + 1) * 604800000,
                    -n * 604800000, 'closed'
             FROM sequence;",
        )
        .expect("seed long capacity history");
    let server = start_server_with_args(home, &["--query-timeout-seconds", "5"]);

    let response = request(&server.authority, "GET", "/api/dashboard", &[]);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let payload: Value = serde_json::from_str(response_body(&response)).expect("dashboard JSON");
    assert_eq!(
        payload["queries"]["capacity"]["data"]["epochs"]
            .as_array()
            .map(Vec::len),
        Some(302)
    );
}

#[test]
fn serve_fails_closed_when_the_requested_loopback_port_is_occupied() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let port = listener.local_addr().expect("reserved address").port();
    let home = test_home("occupied-port");
    let output = Command::new(env!("CARGO_BIN_EXE_codex-quota-ledger"))
        .args(["serve", "--port", &port.to_string()])
        .env("CODEX_QUOTA_HOME", home.join("ledger"))
        .env("CODEX_HOME", home.join("codex"))
        .output()
        .expect("attempt occupied port");
    std::fs::remove_dir_all(home).expect("remove temporary home");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert!(
        stderr.contains("unable to bind loopback web server"),
        "{stderr}"
    );
}

#[test]
fn core_page_exposes_the_v1_evidence_sections_without_external_assets() {
    let server = start_server("core-page");
    let page = request(&server.authority, "GET", "/", &[]);
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    let html = response_body(&page);
    for required in [
        "当前额度",
        "本周期 Token 与等价成本",
        "历史容量变化",
        "官方数据与本地日志",
        "采集健康与数据新鲜度",
        "data-state=\"loading\"",
        "/app.css",
        "/app.js",
    ] {
        assert!(html.contains(required), "page omitted {required}");
    }
    assert!(!html.contains("https://"));
    assert!(!html.contains("http://"));

    let script = request(&server.authority, "GET", "/app.js", &[]);
    assert!(script.starts_with("HTTP/1.1 200"), "{script}");
    let javascript = response_body(&script);
    assert!(javascript.contains("fetch('/api/dashboard')"));
    assert!(javascript.contains("证据不足，无法判断"));
    assert!(javascript.contains("数据暂不可用"));
    for translated_issue in [
        "estimate_evidence_limit",
        "capacity_evidence_limit",
        "reconciliation_evidence_limit",
    ] {
        assert!(
            javascript.contains(translated_issue),
            "page omitted translation for {translated_issue}"
        );
    }
    assert!(!javascript.contains("innerHTML"));
    assert!(!javascript.contains("https://"));

    let style = request(&server.authority, "GET", "/app.css", &[]);
    assert!(style.starts_with("HTTP/1.1 200"), "{style}");
    assert!(!response_body(&style).contains("url("));
}

#[test]
fn core_page_exposes_keyboard_focus_and_assistive_loading_state() {
    let server = start_server("accessibility");
    let page = request(&server.authority, "GET", "/", &[]);
    let html = response_body(&page);
    assert!(html.contains("class=\"skip-link\" href=\"#app\""));
    assert!(html.contains("id=\"app\" data-state=\"loading\" aria-busy=\"true\""));
    assert!(html.contains("id=\"overall-state\" role=\"status\""));
    assert!(html.contains("id=\"refresh\" type=\"button\" aria-controls=\"app\""));
    assert!(html.contains("<noscript>"));

    let script = request(&server.authority, "GET", "/app.js", &[]);
    let javascript = response_body(&script);
    assert!(javascript.contains("setAttribute('aria-busy','true')"));
    assert!(javascript.contains("setAttribute('aria-busy','false')"));

    let style = request(&server.authority, "GET", "/app.css", &[]);
    let css = response_body(&style);
    assert!(css.contains(":focus-visible"));
    assert!(css.contains(".skip-link:focus"));
}
