use axum::extract::Request;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::{json, Map, Value};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Codex Quota Ledger</title>
  <link rel="stylesheet" href="/app.css">
  <script src="/app.js" defer></script>
</head>
<body>
  <a class="skip-link" href="#app">跳到主要内容</a>
  <header class="topbar">
    <div class="brand"><span class="brand-mark">CQ</span><span>Codex Quota Ledger</span></div>
    <div class="local-pill"><span class="pulse"></span>仅在本机运行</div>
  </header>
  <main id="app" data-state="loading" aria-busy="true">
    <noscript><p class="noscript">此页面需要本地 JavaScript 来读取查询结果；不会连接外部网络。</p></noscript>
    <section class="intro">
      <div>
        <p class="eyebrow">额度证据面板</p>
        <h1>这周用了多少，证据是否完整。</h1>
        <p class="lede">官方额度、本地 Token 与历史周期分别呈现；缺失数据不会被当作零。</p>
      </div>
      <div class="toolbar">
        <span id="overall-state" role="status" class="state-badge loading">正在读取本地证据</span>
        <button id="refresh" type="button" aria-controls="app">刷新</button>
      </div>
    </section>

    <section class="grid hero-grid" aria-label="当前概览">
      <article class="panel quota-panel">
        <div class="panel-heading"><div><p class="kicker">官方周额度</p><h2>当前额度</h2></div><span id="quota-confidence" class="source-tag">官方来源</span></div>
        <div class="quota-content">
          <div id="quota-ring" class="quota-ring"><div><strong id="quota-remaining">—</strong><span>剩余</span></div></div>
          <div class="quota-copy">
            <p id="quota-used" class="metric-line">数据暂不可用</p>
            <p class="label">下次重置</p><p id="quota-reset" class="value">—</p>
            <p id="quota-freshness" class="freshness">等待官方观测</p>
          </div>
        </div>
      </article>

      <article class="panel token-panel">
        <div class="panel-heading"><div><p class="kicker">本地日志观测</p><h2>本周期 Token 与等价成本</h2></div><span class="source-tag local">本机来源</span></div>
        <div class="metric-grid">
          <div><span>输入 Token</span><strong id="input-tokens">—</strong></div>
          <div><span>输出 Token</span><strong id="output-tokens">—</strong></div>
          <div><span>已知 API 等价成本</span><strong id="known-cost">—</strong></div>
          <div><span>未定价部分</span><strong id="unpriced-tokens">—</strong></div>
        </div>
        <p class="fine-print">等价成本不是 OpenAI 账单，也不是订阅额度内部公式。</p>
      </article>
    </section>

    <section class="grid evidence-grid">
      <article class="panel">
        <div class="panel-heading"><div><p class="kicker">周期取证</p><h2>历史容量变化</h2></div><span id="capacity-confidence" class="state-badge neutral">等待证据</span></div>
        <p id="capacity-result" class="result-callout">证据不足，无法判断</p>
        <dl class="detail-list"><div><dt>当前周期观测</dt><dd id="capacity-current">—</dd></div><div><dt>与上一周期</dt><dd id="capacity-relative">—</dd></div></dl>
        <p id="capacity-note" class="fine-print">需要至少两个可比较周期。</p>
      </article>

      <article class="panel">
        <div class="panel-heading"><div><p class="kicker">来源对账</p><h2>官方数据与本地日志</h2></div><span id="reconcile-state" class="state-badge neutral">待检查</span></div>
        <div class="source-row"><span class="source-dot official"></span><div><strong>官方额度</strong><p id="official-coverage">等待观测</p></div></div>
        <div class="source-row"><span class="source-dot local"></span><div><strong>本地 JSONL</strong><p id="local-coverage">仅覆盖本机可见日志</p></div></div>
        <div class="source-row"><span class="source-dot web"></span><div><strong>独立网页观测</strong><p id="web-coverage">未提供独立网页观测</p></div></div>
        <p id="reconcile-note" class="fine-print">来源彼此独立，不会合并成伪精确总量。</p>
      </article>

      <article class="panel health-panel">
        <div class="panel-heading"><div><p class="kicker">运行状态</p><h2>采集健康与数据新鲜度</h2></div><span id="health-state" class="state-badge neutral">读取中</span></div>
        <dl class="detail-list"><div><dt>官方采集器</dt><dd id="collector-health">—</dd></div><div><dt>最近官方成功</dt><dd id="collector-freshness">—</dd></div><div><dt>最近本地事件</dt><dd id="ledger-freshness">—</dd></div><div><dt>实时通知取证</dt><dd id="notification-state">—</dd></div></dl>
      </article>
    </section>

    <section class="panel issues-panel" aria-live="polite">
      <div class="panel-heading"><div><p class="kicker">证据说明</p><h2>缺失、限制与待确认项</h2></div></div>
      <ul id="issues"><li>正在读取本地查询结果。</li></ul>
    </section>
  </main>
  <footer>本地只读界面 · 不上传 Prompt、凭据或日志 · <span id="updated-at">尚未刷新</span></footer>
</body>
</html>
"##;

const APP_CSS: &str = r#"
:root{color-scheme:light;--ink:#18221e;--muted:#66716b;--line:#dce3de;--paper:#f4f6f3;--card:#fff;--green:#287a57;--green-soft:#e3f2e9;--amber:#9a6417;--amber-soft:#fbefd5;--red:#a33c32;--red-soft:#fbe7e4;--navy:#263e52;--shadow:0 18px 50px rgba(28,45,36,.08);font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 12% 0,#eaf2ec 0,transparent 30rem),var(--paper);color:var(--ink)}
.skip-link{position:fixed;z-index:20;left:16px;top:10px;transform:translateY(-160%);padding:10px 14px;border-radius:10px;background:var(--ink);color:#fff;font-weight:700}.skip-link:focus{transform:translateY(0)}
.noscript{padding:14px;border:1px solid #e6c982;border-radius:12px;background:var(--amber-soft);color:#71490f}a:focus-visible,button:focus-visible{outline:3px solid #2f7fc1;outline-offset:3px}
.topbar{height:68px;padding:0 max(24px,calc((100vw - 1180px)/2));display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(24,34,30,.08);background:rgba(250,252,249,.88)}
.brand{display:flex;align-items:center;gap:11px;font-weight:730;letter-spacing:-.02em}.brand-mark{display:grid;place-items:center;width:34px;height:34px;border-radius:11px;background:var(--ink);color:#fff;font-size:12px;letter-spacing:.08em}
.local-pill,.state-badge,.source-tag{display:inline-flex;align-items:center;gap:7px;border-radius:999px;padding:7px 10px;font-size:12px;font-weight:650;background:var(--green-soft);color:var(--green)}.pulse{width:7px;height:7px;border-radius:50%;background:#39a36f;box-shadow:0 0 0 4px rgba(57,163,111,.12)}
main{max-width:1180px;margin:0 auto;padding:54px 24px 40px}.intro{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:26px}.eyebrow,.kicker{margin:0 0 9px;color:var(--green);font-size:11px;font-weight:800;letter-spacing:.13em;text-transform:uppercase}.intro h1{max-width:760px;margin:0;font-size:clamp(32px,5vw,54px);line-height:1.04;letter-spacing:-.055em}.lede{max-width:680px;margin:17px 0 0;color:var(--muted);font-size:16px;line-height:1.65}.toolbar{display:flex;align-items:center;gap:10px;flex-shrink:0}button{border:1px solid var(--line);border-radius:12px;background:#fff;color:var(--ink);font:inherit;font-weight:700;padding:10px 14px;cursor:pointer}button:hover{border-color:#aab8b0}button:disabled{opacity:.55;cursor:wait}
.state-badge.loading,.state-badge.neutral{background:#edf0ee;color:#65706a}.state-badge.partial{background:var(--amber-soft);color:var(--amber)}.state-badge.complete{background:var(--green-soft);color:var(--green)}.state-badge.unavailable{background:var(--red-soft);color:var(--red)}
.grid{display:grid;gap:18px}.hero-grid{grid-template-columns:1.02fr 1.48fr}.evidence-grid{grid-template-columns:repeat(3,1fr);margin-top:18px}.panel{background:var(--card);border:1px solid rgba(24,34,30,.08);border-radius:20px;padding:24px;box-shadow:var(--shadow)}.panel-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}.panel h2{margin:0;font-size:18px;letter-spacing:-.025em}.source-tag{background:#edf1f5;color:var(--navy)}.source-tag.local{background:#eeeafb;color:#6652a2}
.quota-content{display:flex;align-items:center;gap:24px;margin-top:22px}.quota-ring{--used:0%;width:148px;height:148px;flex:0 0 auto;border-radius:50%;display:grid;place-items:center;background:conic-gradient(var(--green) var(--used),#edf0ed 0);position:relative}.quota-ring:before{content:"";position:absolute;inset:13px;border-radius:50%;background:#fff}.quota-ring div{position:relative;display:grid;text-align:center}.quota-ring strong{font-size:30px;letter-spacing:-.05em}.quota-ring span{font-size:12px;color:var(--muted)}.metric-line{margin:0 0 18px;font-size:17px;font-weight:750}.label{margin:0;color:var(--muted);font-size:12px}.value{margin:4px 0 8px;font-weight:700}.freshness,.fine-print{margin:0;color:var(--muted);font-size:12px;line-height:1.55}
.metric-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:1px;margin:22px 0;background:var(--line);border:1px solid var(--line);border-radius:14px;overflow:hidden}.metric-grid div{display:grid;gap:7px;padding:17px;background:#fff}.metric-grid span{color:var(--muted);font-size:12px}.metric-grid strong{font-size:22px;letter-spacing:-.035em}
.result-callout{min-height:72px;margin:22px 0 18px;padding:17px;border-radius:14px;background:#f2f6f3;font-size:18px;font-weight:750;line-height:1.35}.detail-list{display:grid;gap:0;margin:0 0 16px}.detail-list div{display:flex;justify-content:space-between;gap:12px;padding:11px 0;border-bottom:1px solid #edf0ed}.detail-list dt{color:var(--muted);font-size:12px}.detail-list dd{margin:0;text-align:right;font-size:12px;font-weight:700}.source-row{display:flex;gap:11px;padding:14px 0;border-bottom:1px solid #edf0ed}.source-row p{margin:4px 0 0;color:var(--muted);font-size:12px;line-height:1.45}.source-dot{width:9px;height:9px;margin-top:4px;border-radius:50%;background:var(--navy)}.source-dot.local{background:#765ab1}.source-dot.web{background:#ba8740}.issues-panel{margin-top:18px}.issues-panel ul{margin:18px 0 0;padding-left:20px;color:var(--muted);font-size:13px;line-height:1.7}.issues-panel li+li{margin-top:6px}footer{max-width:1180px;margin:0 auto;padding:0 24px 34px;color:#77817c;font-size:11px;text-align:center}
@media(max-width:900px){.hero-grid,.evidence-grid{grid-template-columns:1fr}.evidence-grid{grid-template-columns:repeat(2,1fr)}.health-panel{grid-column:1/-1}}
@media(max-width:640px){.topbar{height:60px;padding:0 16px}.local-pill{font-size:0}.local-pill .pulse{margin:2px}.brand{font-size:14px}main{padding:36px 16px 28px}.intro{display:grid;align-items:start}.toolbar{justify-content:space-between}.hero-grid,.evidence-grid{grid-template-columns:1fr}.health-panel{grid-column:auto}.panel{padding:20px;border-radius:17px}.quota-content{align-items:flex-start}.quota-ring{width:122px;height:122px}.metric-grid{grid-template-columns:1fr}.intro h1{font-size:36px}}
"#;

const APP_JS: &str = r#"
'use strict';
const byId = (id) => document.getElementById(id);
const present = (value) => value !== null && value !== undefined;
const number = (value) => present(value) ? new Intl.NumberFormat('zh-CN').format(value) : '— 数据缺失';
const percent = (value) => present(value) ? `${Number(value).toFixed(1).replace('.0','')}%` : '— 数据缺失';
const money = (value) => present(value) ? `$${Number(value).toFixed(2)}` : '— 尚无已知金额';
const moment = (value) => present(value) ? new Intl.DateTimeFormat('zh-CN',{dateStyle:'medium',timeStyle:'short'}).format(new Date(value)) : '— 数据缺失';
const set = (id, value) => { byId(id).textContent = value; };
const availabilityText = {complete:'证据可用',partial:'部分证据',unavailable:'数据暂不可用'};
const collectorText = {starting:'等待首次采集',healthy:'运行正常',recovering:'正在恢复',degraded:'采集受阻'};
const confidenceText = {higher:'较高置信','partial-window':'仅部分周期','insufficient-observations':'观测不足'};

function setBadge(id, state, text) {
  const node = byId(id);
  node.className = `state-badge ${state || 'neutral'}`;
  node.textContent = text || availabilityText[state] || '状态未知';
}

function renderStatus(envelope) {
  const quota = envelope.data && envelope.data.quota;
  if (quota) {
    set('quota-remaining', percent(quota.remainingPercent));
    set('quota-used', `已使用 ${percent(quota.usedPercent)}`);
    set('quota-reset', moment(quota.resetAtMs));
    set('quota-freshness', `观测于 ${moment(quota.observedAtMs)}`);
    byId('quota-ring').style.setProperty('--used', `${Math.max(0,Math.min(100,Number(quota.usedPercent)))}%`);
  } else {
    set('quota-remaining', '—');
    set('quota-used', '数据暂不可用');
    set('quota-reset', '— 缺少官方重置时间');
    set('quota-freshness', '等待官方额度观测');
  }
  const data = envelope.data || {};
  const collector = data.quotaCollector || {};
  const ledger = data.tokenLedger || {};
  setBadge('health-state', envelope.availability, availabilityText[envelope.availability]);
  set('collector-health', collectorText[collector.state] || '状态未知');
  set('collector-freshness', moment(collector.lastSuccessAtMs));
  set('ledger-freshness', moment(ledger.lastEventAtMs));
  set('notification-state', present(collector.lastNotificationAtMs) ? moment(collector.lastNotificationAtMs) : '尚未观察到真实通知');
}

function renderEstimate(envelope) {
  const report = envelope.data;
  const totals = report && report.localUsage && report.localUsage.totals;
  if (!totals) {
    set('input-tokens','— 数据缺失'); set('output-tokens','— 数据缺失');
    set('known-cost','— 尚无已知金额'); set('unpriced-tokens','— 数据缺失'); return;
  }
  set('input-tokens', number(totals.inputTokens));
  set('output-tokens', number(totals.outputTokens));
  set('known-cost', money(totals.apiEquivalentCostUsd));
  if (!present(totals.unpricedEvents)) { set('unpriced-tokens','— 数据缺失'); }
  else if (totals.unpricedEvents === 0) { set('unpriced-tokens','0 · 无未定价事件'); }
  else if (present(totals.unpricedInputTokens) && present(totals.unpricedOutputTokens)) {
    const unpriced = Number(totals.unpricedInputTokens) + Number(totals.unpricedOutputTokens);
    set('unpriced-tokens',`${number(unpriced)} · ${number(totals.unpricedEvents)} 个事件`);
  } else { set('unpriced-tokens',`Token 数量缺失 · ${number(totals.unpricedEvents)} 个事件`); }
}

function renderCapacity(envelope) {
  const epochs = envelope.data && envelope.data.epochs;
  const latest = epochs && epochs.length ? epochs[epochs.length-1] : null;
  if (!latest) {
    setBadge('capacity-confidence','unavailable','证据不足');
    set('capacity-result','证据不足，无法判断');
    set('capacity-current','—'); set('capacity-relative','—');
    set('capacity-note','需要至少两个可比较周期，缺失不会显示为 0。'); return;
  }
  setBadge('capacity-confidence', envelope.availability, confidenceText[latest.confidence] || '置信度未知');
  set('capacity-current', present(latest.impliedFullWindowLocalTokens) ? `${number(latest.impliedFullWindowLocalTokens)} 推算 Token` : '观测不足');
  if (present(latest.relativeToPreviousEpoch)) {
    const ratio = Number(latest.relativeToPreviousEpoch);
    set('capacity-result', `当前可比容量约为上一周期的 ${ratio.toFixed(2)} 倍`);
    set('capacity-relative', `${ratio.toFixed(2)}×`);
    set('capacity-note','这是本地 Token 与官方百分比变化的推算，不是官方额度公式。');
  } else {
    set('capacity-result','证据不足，无法判断'); set('capacity-relative','— 无可比周期');
  }
}

function renderReconcile(envelope) {
  const report = envelope.data;
  setBadge('reconcile-state', envelope.availability, availabilityText[envelope.availability]);
  if (!report) {
    set('official-coverage','数据暂不可用'); set('local-coverage','本地覆盖范围未知');
    set('web-coverage','未提供独立网页观测'); return;
  }
  set('official-coverage', `额度观测于 ${moment(report.appServerQuota && report.appServerQuota.observedAtMs)}`);
  const events = report.localUsage && report.localUsage.totals && report.localUsage.totals.events;
  set('local-coverage', present(events) ? `当前窗口观测到 ${number(events)} 个本地事件；不含其他设备与缺失日志` : '仅覆盖本机可见日志');
  set('web-coverage', report.webObservation ? `与官方相差 ${percent(report.webObservation.appServerDeltaPercentagePoints)}` : '未提供独立网页观测');
}

function renderIssues(queries) {
  const list = byId('issues'); list.replaceChildren();
  const commandText={status:'当前状态',estimate:'Token 与成本',capacity:'历史容量',reconcile:'来源对账'};
  const issueText={
    active_account_unavailable:'当前账户尚未确认，账户相关证据暂不可用。',
    official_quota_unavailable:'尚无官方额度观测。',
    collector_not_healthy:'官方采集器尚未进入正常状态。',
    query_unavailable:'该项所需证据暂不可用。',
    notification_live_observation_pending:'尚未观察到真实额度更新通知。',
    estimate_evidence_limit:'成本仅包含已有定价且本机日志可见的部分；未定价和缺失日志已单独保留。',
    capacity_evidence_limit:'容量比较是基于本地 Token 与官方百分比变化的推算；其他设备和缺失日志可能使周期不可比。',
    reconciliation_evidence_limit:'对账保留各证据来源的独立性；本地日志不能覆盖网页、云端、其他设备或缺失记录。'
  };
  const messages=new Set();
  for (const query of Object.values(queries)) {
    for (const issue of query.issues || []) {
      const message=issueText[issue.code] || issue.message || issue.code || '未说明的问题';
      messages.add(`${commandText[query.command] || query.command || '查询'}：${message}`);
    }
  }
  if (!messages.size) { const item=document.createElement('li'); item.textContent='当前查询没有报告额外限制。'; list.append(item); return; }
  for (const message of messages) { const item=document.createElement('li'); item.textContent=message; list.append(item); }
}

async function loadDashboard() {
  const button=byId('refresh'); const app=byId('app'); button.disabled=true; app.setAttribute('aria-busy','true'); setBadge('overall-state','loading','正在读取本地证据');
  try {
    const response=await fetch('/api/dashboard'); if(!response.ok) throw new Error('dashboard unavailable');
    const payload=await response.json(); const queries=payload.queries || {};
    renderStatus(queries.status || {}); renderEstimate(queries.estimate || {});
    renderCapacity(queries.capacity || {}); renderReconcile(queries.reconcile || {}); renderIssues(queries);
    const states=Object.values(queries).map((query)=>query.availability);
    const state=states.includes('unavailable')?'partial':states.includes('partial')?'partial':'complete';
    setBadge('overall-state',state,state==='complete'?'当前证据可用':'存在缺失或低置信证据');
    byId('app').dataset.state=state; set('updated-at',`页面刷新于 ${moment(Date.now())}`);
  } catch (_) {
    setBadge('overall-state','unavailable','数据暂不可用'); byId('app').dataset.state='unavailable';
    const list=byId('issues'); list.replaceChildren(); const item=document.createElement('li'); item.textContent='本地查询适配器暂不可用，请稍后刷新或运行 diagnose。'; list.append(item);
  } finally { button.disabled=false; app.setAttribute('aria-busy','false'); }
}
byId('refresh').addEventListener('click',loadDashboard);
loadDashboard();
"#;

#[derive(Clone)]
struct RequestPolicy {
    loopback_authority: String,
    localhost_authority: String,
}

#[derive(Clone)]
struct WebState {
    executable: PathBuf,
    query_timeout: Duration,
}

pub fn serve(port: u16, query_timeout: Duration) -> Result<(), String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("unable to start local web runtime: {error}"))?
        .block_on(serve_async(port, query_timeout))
}

async fn serve_async(port: u16, query_timeout: Duration) -> Result<(), String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("unable to resolve query executable: {error}"))?;
    let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
        .map_err(|error| format!("unable to bind loopback web server: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("unable to read loopback address: {error}"))?;
    println!("Codex Quota Ledger: http://{address}");
    std::io::stdout()
        .flush()
        .map_err(|error| format!("unable to report local web address: {error}"))?;
    axum::serve(listener, router(address, executable, query_timeout))
        .await
        .map_err(|error| format!("local web server stopped: {error}"))
}

fn router(address: SocketAddr, executable: PathBuf, query_timeout: Duration) -> Router {
    let policy = RequestPolicy {
        loopback_authority: address.to_string(),
        localhost_authority: format!("localhost:{}", address.port()),
    };
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/healthz", get(|| async { "codex-quota-ledger\n" }))
        .route("/app.css", get(asset_css))
        .route("/app.js", get(asset_js))
        .route("/api/dashboard", get(dashboard))
        .with_state(WebState {
            executable,
            query_timeout,
        })
        .layer(middleware::from_fn_with_state(policy, request_guard))
        .layer(middleware::from_fn(security_headers))
}

async fn asset_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn asset_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn dashboard(State(state): State<WebState>) -> Response {
    match tokio::task::spawn_blocking(move || {
        dashboard_queries(&state.executable, state.query_timeout)
    })
    .await
    {
        Ok(Ok(value)) => axum::Json(value).into_response(),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({
                "schema": "codex-quota-ledger.dashboard",
                "schemaVersion": 1,
                "availability": "unavailable",
                "issues": [{
                    "code": "query_adapter_unavailable",
                    "severity": "error",
                    "message": "The local query adapter could not produce dashboard data."
                }]
            })),
        )
            .into_response(),
    }
}

fn dashboard_queries(executable: &PathBuf, timeout: Duration) -> Result<Value, ()> {
    let mut queries = Map::new();
    for command in ["status", "estimate", "capacity", "reconcile"] {
        let output = query_output(executable, command, timeout)?;
        let value: Value = serde_json::from_slice(&output.stdout).map_err(|_| ())?;
        if value.get("schema").and_then(Value::as_str) != Some("codex-quota-ledger.query")
            || value.get("schemaVersion").and_then(Value::as_u64) != Some(1)
            || value.get("command").and_then(Value::as_str) != Some(command)
        {
            return Err(());
        }
        queries.insert(command.to_string(), value);
    }
    Ok(json!({
        "schema": "codex-quota-ledger.dashboard",
        "schemaVersion": 1,
        "queries": queries
    }))
}

fn query_output(executable: &PathBuf, command: &str, timeout: Duration) -> Result<Output, ()> {
    let mut child = Command::new(executable)
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ())?;
    let mut stdout = child.stdout.take().ok_or(())?;
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map_err(|_| ())?;
        Ok::<_, ()>(bytes)
    });
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|_| ())? {
            let stdout = stdout_reader.join().map_err(|_| ())??;
            return Ok(Output {
                status,
                stdout,
                stderr: Vec::new(),
            });
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            return Err(());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

async fn request_guard(
    State(policy): State<RequestPolicy>,
    request: Request,
    next: Next,
) -> Response {
    let host_allowed = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| {
            host.eq_ignore_ascii_case(&policy.loopback_authority)
                || host.eq_ignore_ascii_case(&policy.localhost_authority)
        });
    let origin_allowed = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|origin| {
            origin.eq_ignore_ascii_case(&format!("http://{}", policy.loopback_authority))
                || origin.eq_ignore_ascii_case(&format!("http://{}", policy.localhost_authority))
        });
    let fetch_site_allowed = request
        .headers()
        .get(header::HeaderName::from_static("sec-fetch-site"))
        .and_then(|value| value.to_str().ok())
        .is_none_or(|site| !site.eq_ignore_ascii_case("cross-site"));
    if !host_allowed || !origin_allowed || !fetch_site_allowed {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    next.run(request).await
}

async fn security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}
