# SpelaionDB's Oracle plugin — rules for agents

Oracle Database for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's
driver, and a small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Oracle's thin driver `oracledb` (SpelaionDB's ADR-059):
  - `lib.rs`: each connection lives in a thread of its own (an actor), since `oracledb` is
    synchronous. It connects to `connect_addr` when it is given (the local end of SpelaionDB's
    tunnel) and checks TLS against `host`; the connect descriptor is written from the
    parameters, and a value that would change it is refused. A cancel goes through the patched
    driver's `CancelHandle` and the contract's `thread_cancel`. The service name is
    `database`; the `sid` option (`true`) makes it a SID. The transaction state comes from the
    driver after every statement.
  - `exec.rs`: one statement, sent as it arrived: a query streams its rows in batches, anything
    else reports the rows it changed. Errors keep Oracle's code (`ORA-00942`) and position.
  - `types.rs`: Oracle's types as SpelaionDB's logical types, decided from the columns'
    metadata before any row is read. A type that cannot be shown yet (objects, collections,
    `XMLTYPE`, `BFILE`) is refused naming the column, never shown wrongly.
  - `introspect.rs`: the explorer, from the `ALL_*` views, every name bound as a parameter.
- `vendor/oracledb`: Oracle's driver (UPL-1.0 or Apache-2.0), the crates.io release as
  published in its first commit here, then patched. **`vendor/oracledb/PATCHED.md` lists each
  patch: read it before touching or upgrading the driver.** Every change there is marked in the
  code, and comes with a test here against Oracle Free.
- `src/bin/spelaiondb-plugin-oracle.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/oracle.rs`: the type zoo, errors, transactions, bound values, batches, cancel (each
  bounded with `tokio::time::timeout`), the explorer and TLS checked against the host name,
  against a real server. **A new type is added there first.** `tests/conformance.rs`:
  SpelaionDB's conformance set against this program, the same with `RSO_DEBUG_PACKETS` set
  (standard output must stay the protocol), and `plugin.toml` checked against what the program
  says of itself.
- `testing/` and `ci/services.sh`: the test server, Oracle Database Free (the lite image) on
  11521 with the tests' user `spelaion` in `FREEPDB1`, and the same database behind a TLS proxy
  on 12484 with a certificate for `oracle.spelaion.test` (`testing/gen-keys.sh`).
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent, cancels and
introspects. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`, here or in the
  vendored driver (its packet dump is removed, PATCHED.md 3).
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file or the environment.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a connection is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, nor Oracle's own
  (`tnsnames.ora`, wallets): the descriptor is always written from the parameters.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are git
dependencies on a tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`), all on the same tag.
A protocol change is made there first, then here: move the tag, adapt, release. Arrow must stay
the version SpelaionDB uses. `plugin.toml`'s `protocol` is what the SDK of that tag speaks.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit). **Nothing is signed and no secret lives here**:
a release reaches users only when SpelaionDB's repository pins its hashes in a pull request of
its own (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` (the
vendored driver included) waits for an owner. The work is tracked in SpelaionDB's tracker
(Plane, project `SDB`): write the item's identifier in the pull request's description, never
in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against a real server (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_ORACLE=127.0.0.1:11521 SPELAION_TEST_ORACLE_TCPS=127.0.0.1:12484 cargo test`.
- `cargo deny check` if dependencies change.
- If something could not be tested, say so.
