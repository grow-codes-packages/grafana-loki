// Created by sdk-kit 0.1.0; owned by this repository.
//! Helpers for the integration tests: bootstrap credentials and the conformance directory.

use std::path::PathBuf;

use grow_grafana_loki as sdk;

/// Loads `KEY=value` lines from `$GROW_BOOTSTRAP_ENV` (default `/bootstrap/env`) into the
/// process environment; variables that are already set win. A missing file is not an error.
pub fn load_bootstrap_env() {
    let path = std::env::var("GROW_BOOTSTRAP_ENV").unwrap_or_else(|_| "/bootstrap/env".to_owned());
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().trim_start_matches("export ").trim();
            let value = value.trim().trim_matches('"');
            if !key.is_empty() && std::env::var_os(key).is_none() {
                std::env::set_var(key, value);
            }
        }
    }
}

/// `$GROW_CONFORMANCE_DIR`, else `<repo>/conformance/scenarios`.
pub fn conformance_dir() -> PathBuf {
    sdk::testing::scenarios_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// A short unique id for one test run (`GROW_TEST_RUN_ID` + time + process id).
pub fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{:x}{:x}", nanos % 0xffff_ffff, std::process::id() % 0xfff)
}

/// `true` when the Docker fixture is configured (`GROW_GRAFANA_LOKI_URL` after the bootstrap).
pub fn configured() -> bool {
    load_bootstrap_env();
    let set = std::env::var_os("GROW_GRAFANA_LOKI_URL").is_some();
    if !set {
        eprintln!("GROW_GRAFANA_LOKI_URL is not set; run through docker/compose.yml");
    }
    set
}

/// A bootstrap variable (empty when unset).
pub fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}
