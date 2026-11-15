//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042): it starts, speaks the protocol's version, opens sessions, returns results in the
//! order the contract says, fails without hanging, obeys a cancel while a statement runs and
//! ends when its input closes. What the engine's types look like is `tests/oracle.rs`.
//!
//! It needs `SPELAION_TEST_ORACLE=host:port` (`ci/services.sh` starts the server); without it
//! the tests are skipped, saying so, which does not count as tested.

use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-oracle");

fn target() -> Option<Target> {
    let Ok(addr) = std::env::var("SPELAION_TEST_ORACLE") else {
        eprintln!("SPELAION_TEST_ORACLE not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some(Target {
        program: PROGRAM.into(),
        engine: "oracle".into(),
        params: ConnectParams {
            host: host.into(),
            port: port.parse().expect("port"),
            user: "spelaion".into(),
            password: Some(Secret::new("spelaion")),
            database: "FREEPDB1".into(),
            ..Default::default()
        },
        one_row: "select 1 from dual".into(),
        refused: "select * from a_table_that_is_not_there".into(),
        // Busy on the server until it is cancelled.
        slow: Some("begin dbms_session.sleep(600); end;".into()),
    })
}

#[tokio::test]
async fn the_plugin_conforms() {
    if let Some(target) = target() {
        assert_conforms(&target).await;
    }
}

/// `RSO_DEBUG_PACKETS` once made the driver print every packet to standard output, which is
/// this program's protocol, and packets carry the logon. The patched driver has no such code
/// (`vendor/oracledb/PATCHED.md`, 3). SpelaionDB starts a plugin with its own environment, so a
/// user's variable reaches the program: with it set, the whole conformance set must still pass.
/// The variable is set for a child run of this test binary, since changing this process's own
/// environment would need `unsafe`.
#[tokio::test]
async fn the_protocol_stays_clean_whatever_the_environment_says() {
    if std::env::var_os("SPELAION_ORACLE_CHILD").is_some() || target().is_none() {
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().expect("this test"))
        .args(["the_plugin_conforms", "--exact", "--nocapture"])
        .env("RSO_DEBUG_PACKETS", "1")
        .env("SPELAION_ORACLE_CHILD", "1")
        .status()
        .expect("the child test starts");
    assert!(
        status.success(),
        "the conformance set failed with RSO_DEBUG_PACKETS set"
    );
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
