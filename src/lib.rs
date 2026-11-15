//! Oracle Database driver for SpelaionDB (ADR-059 there), over Oracle's own thin driver
//! `oracledb`: pure Rust, no Oracle client library on the user's machine.
//!
//! `oracledb` is synchronous, so each connection lives in a thread of its own (an actor) and
//! the rest of the program talks to it through channels, as SpelaionDB's DuckDB plugin does.
//! A cancel reaches the statement running there through the patched driver's `CancelHandle`
//! (`vendor/oracledb/PATCHED.md`), with the contract's `thread_cancel`: it stops what was sent
//! before it, queued or running, and never what is sent after it.
//!
//! What the thin driver cannot do is said, never worked around (ADR-059, point 3): servers
//! before Oracle 12.1, Native Network Encryption (TLS instead) and operating-system
//! authentication.

mod exec;
mod introspect;
mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use spelaion_driver_api::thread_cancel::{Canceller, Closer};
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, ObjectInfo, ReferencingKey, Result, SchemaColumn, SchemaInfo,
    TableInfo, TlsMode, TransactionState,
};
use tokio::sync::{mpsc, oneshot};

/// Settings of an Oracle connection besides host, port and database (SpelaionDB checks the
/// keys before a connection opens, ADR-056).
pub const OPTIONS: [&str; 1] = ["sid"];

/// The Oracle driver. `ConnectParams::database` is the service name, or the SID when the
/// `sid` option says so.
#[derive(Debug, Default, Clone)]
pub struct OracleDriver;

#[async_trait]
impl Driver for OracleDriver {
    fn engine(&self) -> &str {
        "oracle"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let config = config(params)?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let (commands, inbox) = mpsc::unbounded_channel();
        let closed = Arc::new(AtomicBool::new(false));
        let thread_closed = closed.clone();
        std::thread::Builder::new()
            .name("oracle connection".into())
            .spawn(move || {
                let conn = match oracledb::connect(config).map_err(connect_error) {
                    Ok(conn) => conn,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let handle = match conn.cancel_handle() {
                    Ok(handle) => handle,
                    Err(e) => {
                        let _ = ready_tx.send(Err(exec::map_error(e)));
                        return;
                    }
                };
                let interrupt = handle.clone();
                let canceller = Canceller::new(Box::new(move || interrupt.cancel()));
                let closer = Closer(Arc::clone(&canceller));
                let _ = ready_tx.send(Ok(Arc::clone(&canceller)));
                actor(&conn, &handle, &canceller, &thread_closed, inbox);
                drop(closer);
                let mut conn = conn;
                let _ = conn.close();
            })
            .map_err(|e| DriverError::Other(format!("could not start the Oracle thread: {e}")))?;
        let canceller = ready_rx.await.map_err(|_| {
            DriverError::Connection("the Oracle thread ended while connecting".into())
        })??;
        Ok(Box::new(OracleConnection {
            commands,
            closed,
            cancel: Arc::new(OracleCancel(canceller)),
            capabilities: Capabilities {
                transactions: true,
                cancel: true,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: true,
            },
        }))
    }
}

/// Characters that would change an Oracle connect descriptor if a value held them.
fn descriptor_safe(what: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.chars().any(|c| {
            matches!(c, '(' | ')' | '=' | '"' | '\'' | '\\') || c.is_whitespace() || c.is_control()
        })
    {
        return Err(DriverError::Connection(format!(
            "the {what} {value:?} cannot be used in an Oracle connection: it is empty or has spaces, quotes, parentheses or `=`"
        )));
    }
    Ok(())
}

/// The connect descriptor and settings of a connection, from SpelaionDB's parameters: to
/// `connect_addr` when it is given (a tunnel's local end), with TLS checked against `host`.
pub(crate) fn descriptor(params: &ConnectParams) -> Result<String> {
    let (host, port) = match params.connect_addr {
        Some(addr) => (addr.ip().to_string(), addr.port()),
        None => (params.host.clone(), params.port),
    };
    descriptor_safe("host", &host)?;
    let protocol = match params.tls {
        TlsMode::Disable | TlsMode::Prefer => "TCP",
        // The thin driver always verifies the certificate and the host name: `require` and
        // `verify-ca` are as strict as `verify-full` here.
        TlsMode::Require | TlsMode::VerifyCa | TlsMode::VerifyFull => "TCPS",
    };
    let target = match params.options.get("sid") {
        Some(flag) if flag == "true" => {
            descriptor_safe("SID", &params.database)?;
            format!("(SID={})", params.database)
        }
        Some(flag) if flag != "false" => {
            return Err(DriverError::Connection(format!(
                "the sid option is true or false, not {flag:?}"
            )));
        }
        _ => {
            descriptor_safe("service name", &params.database)?;
            format!("(SERVICE_NAME={})", params.database)
        }
    };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    Ok(format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL={protocol})(HOST={host})(PORT={port}))(CONNECT_DATA={target}))"
    ))
}

fn config(params: &ConnectParams) -> Result<oracledb::Config> {
    if let Some(key) = params
        .options
        .keys()
        .find(|k| !OPTIONS.contains(&k.as_str()))
    {
        return Err(DriverError::Connection(format!(
            "{key} is not a setting of an Oracle connection"
        )));
    }
    if params.client_certificate.is_some() || params.client_key.is_some() {
        return Err(DriverError::Connection(
            "Oracle connections do not log in with a client certificate in SpelaionDB yet: use a password".into(),
        ));
    }
    if params.user.is_empty() {
        return Err(DriverError::Connection(
            "an Oracle connection needs a user: the thin driver has no operating-system authentication".into(),
        ));
    }
    let password = params
        .password
        .as_ref()
        .map(|p| p.expose().to_string())
        .unwrap_or_default();
    let mut config = oracledb::Config::default()
        .set_credentials(&params.user, &password)
        .set_connect_string(&descriptor(params)?)
        .map_err(exec::map_error)?
        .set_program(params.application_name.as_deref().unwrap_or("SpelaionDB"))
        .map_err(exec::map_error)?
        .set_driver_name("SpelaionDB")
        .set_transport_connect_timeout(Some(params.connect_timeout()));
    if params.tls != TlsMode::Disable && params.tls != TlsMode::Prefer {
        config = config.set_tls_server_name(params.host.clone());
        if let Some(ca) = &params.ca_certificate {
            let pem = std::fs::read(ca).map_err(|e| {
                DriverError::Connection(format!(
                    "could not read the CA certificate {}: {e}",
                    ca.display()
                ))
            })?;
            config = config.set_tls_roots_pem(pem);
        }
    }
    Ok(config)
}

/// A failed login with what the thin driver cannot do said in words (ADR-032).
fn connect_error(e: oracledb::Error) -> DriverError {
    let message = e.to_string();
    let lower = message.to_lowercase();
    let reason = if lower.contains("native network encryption") || lower.contains("ora-12660") {
        Some(
            "the server requires Native Network Encryption, which the thin driver does not support: ask for TLS (TCPS) instead",
        )
    } else if lower.contains("not supported") && lower.contains("version") {
        Some("the server is older than Oracle 12.1, which the thin driver does not support")
    } else {
        None
    };
    match reason {
        Some(reason) => DriverError::Connection(format!("{reason} ({message})")),
        None => match exec::map_error(e) {
            DriverError::Other(m) => DriverError::Connection(m),
            other => other,
        },
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Execute {
        id: u64,
        sql: String,
        params: Vec<String>,
        batch_size: usize,
        events: mpsc::Sender<Result<ExecEvent>>,
    },
    Version(Reply<String>),
    Schemas(Reply<Vec<SchemaInfo>>),
    Objects(String, Reply<Vec<ObjectInfo>>),
    Describe(String, String, Reply<TableInfo>),
    Referencing(String, String, Reply<Vec<ReferencingKey>>),
    Columns(String, Reply<Vec<SchemaColumn>>),
    TransactionState(Reply<TransactionState>),
}

/// The connection thread's loop. Ends when the `OracleConnection` is dropped.
fn actor(
    conn: &oracledb::Connection,
    handle: &oracledb::CancelHandle,
    canceller: &Arc<Canceller>,
    closed: &AtomicBool,
    mut inbox: mpsc::UnboundedReceiver<Command>,
) {
    while let Some(command) = inbox.blocking_recv() {
        match command {
            Command::Execute {
                id,
                sql,
                params,
                batch_size,
                events,
            } => {
                let work = canceller.begin(id);
                // A cancel meant for an earlier statement never stops this one; one meant for
                // this one is asked for again until it ends (`thread_cancel`).
                handle.reset();
                let result = work
                    .check()
                    .and_then(|()| exec::execute(conn, &sql, &params, batch_size, &events, &work));
                drop(work);
                note_lost(closed, &result);
                if let Err(e) = result {
                    let _ = events.blocking_send(Err(e));
                }
            }
            Command::Version(reply) => {
                let result = conn
                    .version()
                    .map(|v| format!("Oracle Database {v}"))
                    .map_err(exec::map_error);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::Schemas(reply) => {
                let result = introspect::list_schemas(conn);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::Objects(schema, reply) => {
                let result = introspect::list_objects(conn, &schema);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::Describe(schema, name, reply) => {
                let result = introspect::describe_table(conn, &schema, &name);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::Referencing(schema, name, reply) => {
                let result = introspect::list_referencing_keys(conn, &schema, &name);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::Columns(schema, reply) => {
                let result = introspect::list_columns(conn, &schema);
                note_lost(closed, &result);
                let _ = reply.send(result);
            }
            Command::TransactionState(reply) => {
                let _ = reply.send(
                    conn.transaction_in_progress()
                        .map(|open| {
                            if open {
                                TransactionState::Open
                            } else {
                                TransactionState::Idle
                            }
                        })
                        .map_err(exec::map_error),
                );
            }
        }
    }
}

/// A session the driver says is gone is marked closed: SpelaionDB then marks it lost (ADR-028).
fn note_lost<T>(closed: &AtomicBool, result: &Result<T>) {
    if let Err(e) = result
        && exec::is_lost(e)
    {
        closed.store(true, Ordering::SeqCst);
    }
}

struct OracleConnection {
    commands: mpsc::UnboundedSender<Command>,
    closed: Arc<AtomicBool>,
    cancel: Arc<OracleCancel>,
    capabilities: Capabilities,
}

fn gone() -> DriverError {
    DriverError::Connection("the Oracle connection is closed".into())
}

impl OracleConnection {
    async fn ask<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(make(tx)).map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?
    }

    fn run(&self, sql: &str, params: Vec<String>, opts: &ExecOptions) -> Result<EventStream> {
        // Bounded: when nobody reads, the Oracle thread waits instead of filling memory.
        let (events, rx) = mpsc::channel(4);
        self.cancel.0.submit(|id| {
            self.commands
                .send(Command::Execute {
                    id,
                    sql: sql.to_string(),
                    params,
                    batch_size: opts.batch_size.max(1),
                    events,
                })
                .map_err(|_| gone())
        })?;
        Ok(Box::pin(futures_util::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|event| (event, rx)) },
        )))
    }
}

#[async_trait]
impl Connection for OracleConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        self.run(sql, Vec::new(), opts)
    }

    async fn execute_bound(
        &self,
        sql: &str,
        params: Vec<String>,
        opts: &ExecOptions,
    ) -> Result<EventStream> {
        self.run(sql, params, opts)
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        self.cancel.clone()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn server_version(&self) -> Result<String> {
        self.ask(Command::Version).await
    }

    async fn transaction_state(&self) -> Result<TransactionState> {
        self.ask(Command::TransactionState).await
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.ask(Command::Schemas).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::Objects(schema, reply)).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let (schema, name) = (schema.to_string(), name.to_string());
        self.ask(|reply| Command::Describe(schema, name, reply))
            .await
    }

    async fn list_referencing_keys(&self, schema: &str, name: &str) -> Result<Vec<ReferencingKey>> {
        let (schema, name) = (schema.to_string(), name.to_string());
        self.ask(|reply| Command::Referencing(schema, name, reply))
            .await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::Columns(schema, reply)).await
    }
}

/// Stops the statements submitted before it, queued or running (`thread_cancel`).
struct OracleCancel(Arc<Canceller>);

#[async_trait]
impl CancelHandle for OracleCancel {
    async fn cancel(&self) -> Result<()> {
        self.0.cancel()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> ConnectParams {
        ConnectParams {
            host: "db.example.com".into(),
            port: 1521,
            user: "app".into(),
            database: "FREEPDB1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_descriptor_connects_to_the_tunnel_and_names_the_service_or_sid() {
        assert_eq!(
            descriptor(&params()).unwrap(),
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
        );
        let mut tunnel = params();
        tunnel.connect_addr = Some("127.0.0.1:40123".parse().unwrap());
        tunnel.tls = TlsMode::VerifyFull;
        tunnel.options.insert("sid".into(), "true".into());
        tunnel.database = "FREE".into();
        assert_eq!(
            descriptor(&tunnel).unwrap(),
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=127.0.0.1)(PORT=40123))(CONNECT_DATA=(SID=FREE)))"
        );
    }

    #[test]
    fn a_value_cannot_change_the_descriptor() {
        for (field, value) in [
            ("host", "db)(HOST=evil"),
            ("database", "FREE)(SERVER=DEDICATED"),
            ("database", "a b"),
            ("database", ""),
            ("host", "db\"x"),
        ] {
            let mut p = params();
            match field {
                "host" => p.host = value.into(),
                _ => p.database = value.into(),
            }
            assert!(descriptor(&p).is_err(), "{field} = {value:?}");
        }
    }

    #[test]
    fn unknown_options_and_client_certificates_are_refused() {
        let mut p = params();
        p.options.insert("wallet".into(), "x".into());
        assert!(
            config(&p)
                .unwrap_err()
                .to_string()
                .contains("wallet is not a setting")
        );
        let mut p = params();
        p.client_certificate = Some("cert.pem".into());
        p.client_key = Some("key.pem".into());
        assert!(
            config(&p)
                .unwrap_err()
                .to_string()
                .contains("client certificate")
        );
        let mut p = params();
        p.options.insert("sid".into(), "maybe".into());
        assert!(config(&p).is_err());
    }
}
