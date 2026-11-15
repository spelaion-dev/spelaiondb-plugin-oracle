//! The plugin's program: Oracle's driver served to SpelaionDB over its plugin protocol. It
//! hands the driver to `spelaion_plugin_sdk::run` and nothing else: standard output is the
//! protocol.

use std::sync::Arc;

use spelaion_driver_oracle::OracleDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    run(Plugin::new(
        "spelaiondb-plugin-oracle",
        env!("CARGO_PKG_VERSION"),
        vec![Arc::new(OracleDriver)],
    ))
    .await
}
