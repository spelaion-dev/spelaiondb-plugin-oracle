# oracledb 26.0.0-beta.6, patched for SpelaionDB

This is the crates.io release of Oracle's thin driver (UPL-1.0 or Apache-2.0,
`oracledb-26.0.0-beta.6.crate`, SHA-256
`03813952b1aeba41b54330ea0fb993c0807c785720ff0b9c3e2c8385c1eda9fe`). It is used
through `[patch.crates-io]`, and only the library is kept: no examples, tests or
binaries. The first commit that adds this folder holds the release as it was. Every
change since then is marked `Patched for SpelaionDB (PATCHED.md, n)` in the code.
SpelaionDB's ADR-059 says why the fork exists. The fork is ours to keep: by the
maintainers' decision, no issues are opened upstream.

1. **Cancel.** `Connection::cancel_handle()` gives a `CancelHandle` that stops, from
   another thread, the call the connection is waiting on.
   - Once a handle exists, a read waiting for the database wakes up every 100 ms
     (`CANCEL_TICK`). The bytes read so far stay in the transport's buffer, so
     reading goes on where it stopped.
   - The flag is checked before every read, so a query whose rows keep coming
     stops too. If it is set, the read fails with `ErrorKind::Cancelled`. The
     client then breaks the call exactly as on a call timeout: it sends the
     interrupt marker, resets, and discards the server's answer. The connection
     stays usable.
   - The reset itself ignores the flag, so a cancel asked for again while it
     runs cannot lose the connection.
   - `CancelHandle::reset` forgets a cancel that reached no call. The plugin calls
     it before each statement, so a cancel meant for an earlier statement never
     stops a later one. Fetching more rows of a query is the same statement.
   - The interrupt is in-band. The server sees it at once while it sends rows, but
     a call that is busy on the server (a long PL/SQL sleep, measured on Oracle
     Free 23.26) can take a few seconds to notice it.
   - The call timeout still applies. It is kept apart, since the socket's read
     timeout is now the tick, and it still bounds writes. It is measured per packet
     waited for, not per call.
2. **TLS server name and roots.**
   - `Config::set_tls_server_name` gives the name the server's certificate is
     checked against. SpelaionDB connects to a tunnel's local end (`127.0.0.1`)
     and verifies the real host.
   - `Config::set_tls_roots_pem` trusts only the given certificates instead of the
     built-in roots (`webpki-roots`).
   - A server name rustls cannot use is an error instead of a panic.
3. **Nothing is ever printed.** The packet dump that `RSO_DEBUG_PACKETS` turned on is
   removed with all its code. A plugin's standard output is its protocol, and
   packets carry the logon. No environment variable can make the driver write to
   standard output or standard error.
4. **rustls without its default features.** `ring` is the provider (`aws-lc-rs`
   needs CMake and NASM on Windows and is never built), both in `Cargo.toml` and
   where the driver picks a provider when none is installed.

5. **Whether a transaction is open.** `Connection::transaction_in_progress()` reads
   the flag the driver already keeps from each call's end status, with no round trip.
   SpelaionDB reads the transaction state from the driver after every statement
   (ADR-031).

6. **A LOB fetched whole keeps its type's name.** By default the driver reads a CLOB,
   NCLOB or BLOB column as LONG data, the whole value in the row, and its metadata
   then said `LONG` or `LONG RAW`. `Metadata::data_type()` now names the declared
   type, while the value is still read whole.

When upgrading, start from the new release as published, and reapply each change or
drop it where the upstream API now does the same. Then run the plugin's tests
against Oracle Database Free: cancel (bounded with `tokio::time::timeout`), TLS
through a tunnel, and the protocol staying clean with `RSO_DEBUG_PACKETS` set.
