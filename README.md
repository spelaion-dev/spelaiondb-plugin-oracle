# spelaiondb-plugin-oracle

Oracle Database for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client: the engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for Oracle. It is built here, in its own repository, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engine:** `oracle`, over Oracle's own pure-Rust thin driver,
  [`oracledb`](https://github.com/oracle/rust-oracledb) (its ADR-059). Nothing to install: no
  Oracle client library. The driver is a pre-release, pinned exactly and patched here
  (`vendor/oracledb/PATCHED.md`): cancel from another thread, TLS checked against the host name
  when the database is reached through a tunnel, nothing ever printed, and no aws-lc.
- **What thin mode cannot do,** and SpelaionDB says so when a connection fails: servers older
  than Oracle 12.1, Native Network Encryption (use TLS instead) and operating-system
  authentication.
- **License:** Apache-2.0. The vendored driver is Oracle's (UPL-1.0 or Apache-2.0).

## Working on it

```bash
ci/services.sh
```

starts the test server (Oracle Database Free in Docker, with a TLS proxy in front of it and
its certificates). It takes a minute or two the first time. Then

```bash
SPELAION_TEST_ORACLE=127.0.0.1:11521 SPELAION_TEST_ORACLE_TCPS=127.0.0.1:12484 cargo test
```

runs the type zoo, cancel, TLS and the protocol's conformance set against this plugin's own
program. Without the variables the tests that need the server are skipped, which does not
count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_ORACLE=/path/to/target/debug/spelaiondb-plugin-oracle`, in SpelaionDB's
repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
