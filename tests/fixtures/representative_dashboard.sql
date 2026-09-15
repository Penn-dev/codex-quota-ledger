CREATE TEMP TABLE fixture_clock(now_ms INTEGER NOT NULL);
INSERT INTO fixture_clock VALUES (unixepoch('now') * 1000);

UPDATE current_account_state
SET account_key='acct-demo', updated_at_ms=(SELECT now_ms FROM fixture_clock)
WHERE id=1;

INSERT INTO account_partitions (
    account_key, auth_type, plan, first_seen_at_ms, last_seen_at_ms
) VALUES (
    'acct-demo', 'chatgpt', 'pro',
    (SELECT now_ms FROM fixture_clock) - 1036800000,
    (SELECT now_ms FROM fixture_clock)
);

UPDATE quota_collector_state
SET state='healthy',
    last_success_at_ms=(SELECT now_ms FROM fixture_clock) - 60000,
    notifications_total=1,
    last_notification_at_ms=(SELECT now_ms FROM fixture_clock) - 3600000
WHERE id=1;

INSERT INTO quota_windows (
    account_key, limit_id, reset_at_ms, duration_minutes, window_start_ms,
    first_seen_at_ms, last_seen_at_ms, status
) VALUES
    ('acct-demo', 'codex',
     (SELECT now_ms FROM fixture_clock) - 432000000, 10080,
     (SELECT now_ms FROM fixture_clock) - 1036800000,
     (SELECT now_ms FROM fixture_clock) - 1033200000,
     (SELECT now_ms FROM fixture_clock) - 435600000, 'closed'),
    ('acct-demo', 'codex',
     (SELECT now_ms FROM fixture_clock) + 172800000, 10080,
     (SELECT now_ms FROM fixture_clock) - 432000000,
     (SELECT now_ms FROM fixture_clock) - 428400000,
     (SELECT now_ms FROM fixture_clock) - 60000, 'active');

INSERT INTO quota_snapshots (
    account_key, observed_at_ms, used_percent, remaining_percent, reset_at_ms,
    duration_minutes, limit_id, plan, source
) VALUES
    ('acct-demo', (SELECT now_ms FROM fixture_clock) - 1033200000,
     0, 100, (SELECT now_ms FROM fixture_clock) - 432000000,
     10080, 'codex', 'pro', 'fixture'),
    ('acct-demo', (SELECT now_ms FROM fixture_clock) - 435600000,
     20, 80, (SELECT now_ms FROM fixture_clock) - 432000000,
     10080, 'codex', 'pro', 'fixture'),
    ('acct-demo', (SELECT now_ms FROM fixture_clock) - 428400000,
     0.5, 99.5, (SELECT now_ms FROM fixture_clock) + 172800000,
     10080, 'codex', 'pro', 'fixture'),
    ('acct-demo', (SELECT now_ms FROM fixture_clock) - 60000,
     25.5, 74.5, (SELECT now_ms FROM fixture_clock) + 172800000,
     10080, 'codex', 'pro', 'fixture');

INSERT INTO token_events (
    account_key, session_id, observed_at_ms, usage_at_ms, reported_at_ms,
    model, input_tokens, cached_input_tokens, cache_write_input_tokens,
    output_tokens, reasoning_output_tokens, total_input_tokens,
    total_cached_input_tokens, total_cache_write_input_tokens,
    total_output_tokens, total_reasoning_output_tokens,
    source_path, provenance, ingested_at_ms
) VALUES
    ('acct-demo', 'fixture-previous',
     (SELECT now_ms FROM fixture_clock) - 864000000,
     (SELECT now_ms FROM fixture_clock) - 864000000,
     (SELECT now_ms FROM fixture_clock) - 864000000,
     'gpt-5.6-sol', 16000, 4000, 0, 4000, 1000,
     16000, 4000, 0, 4000, 1000,
     'fixture/previous.jsonl', 'native', (SELECT now_ms FROM fixture_clock)),
    ('acct-demo', 'fixture-current-priced',
     (SELECT now_ms FROM fixture_clock) - 172800000,
     (SELECT now_ms FROM fixture_clock) - 172800000,
     (SELECT now_ms FROM fixture_clock) - 172800000,
     'gpt-5.6-sol', 25000, 5000, 0, 5000, 1200,
     25000, 5000, 0, 5000, 1200,
     'fixture/current-priced.jsonl', 'native', (SELECT now_ms FROM fixture_clock)),
    ('acct-demo', 'fixture-current-unpriced',
     (SELECT now_ms FROM fixture_clock) - 86400000,
     (SELECT now_ms FROM fixture_clock) - 86400000,
     (SELECT now_ms FROM fixture_clock) - 86400000,
     'unknown-fixture-model', 1000, 0, 0, 500, 0,
     1000, 0, 0, 500, 0,
     'fixture/current-unpriced.jsonl', 'native', (SELECT now_ms FROM fixture_clock));

INSERT INTO official_usage_observations (
    account_key, observed_at_ms, lifetime_tokens, peak_daily_tokens,
    longest_running_turn_sec, current_streak_days, longest_streak_days,
    daily_buckets_available, raw_json, collector_version
) VALUES (
    'acct-demo', (SELECT now_ms FROM fixture_clock) - 60000,
    740000, 68000, 480, 6, 21, 1, '{}', 'fixture'
);

DROP TABLE fixture_clock;
