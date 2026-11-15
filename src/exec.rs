//! One statement on the connection's thread: its columns, its rows in batches, how it ended.
//!
//! The statement is sent as it arrived (SpelaionDB's core split and checked it): this module
//! never changes it, never decides what it is beyond what the driver parses (a query or not),
//! and never runs it again.

use oracledb::{ErrorKind, Row};
use spelaion_driver_api::thread_cancel::Work;
use spelaion_driver_api::{DriverError, ExecEvent, Result};
use tokio::sync::mpsc;

use crate::types::{Batch, schema};

/// Runs `sql` with `params` bound in order (`:1`, `:2`… are bound by position) and sends its
/// events. A send that fails means nobody reads any more: the statement stops there.
pub(crate) fn execute(
    conn: &oracledb::Connection,
    sql: &str,
    params: &[String],
    batch_size: usize,
    events: &mpsc::Sender<Result<ExecEvent>>,
    work: &Work,
) -> Result<()> {
    let fetch = u32::try_from(batch_size.clamp(1, 10_000)).unwrap_or(1_000);
    let statement = conn
        .statement(sql)
        .map_err(map_error)?
        .fetch_array_size(fetch)
        .build()
        .map_err(map_error)?;
    let binds: Vec<&dyn oracledb::ToDbValue> = params
        .iter()
        .map(|p| p as &dyn oracledb::ToDbValue)
        .collect();
    if statement.is_query() {
        let cursor = statement.query(&binds).map_err(map_error)?;
        let (schema, readers) = schema(cursor.columns())?;
        if events
            .blocking_send(Ok(ExecEvent::Columns(schema.clone())))
            .is_err()
        {
            return Ok(());
        }
        let mut batch = Batch::new(schema, readers);
        for row in cursor {
            let row: Row = row.map_err(map_error)?;
            batch.push(&row)?;
            if batch.rows() >= batch_size {
                work.check()?;
                if events
                    .blocking_send(Ok(ExecEvent::Batch(batch.take()?)))
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
        if batch.rows() > 0
            && events
                .blocking_send(Ok(ExecEvent::Batch(batch.take()?)))
                .is_err()
        {
            return Ok(());
        }
        let _ = events.blocking_send(Ok(ExecEvent::Done {
            rows_affected: None,
        }));
    } else {
        let dml = statement.is_dml();
        let mut statement = statement;
        let result = statement.execute(&binds).map_err(map_error)?;
        let _ = events.blocking_send(Ok(ExecEvent::Done {
            rows_affected: dml.then(|| result.rows_affected()),
        }));
    }
    Ok(())
}

/// The driver's error as the contract's: the server's own code and message (`ORA-00942: …`),
/// a cancel as a cancel, and a lost connection as a lost connection.
pub(crate) fn map_error(e: oracledb::Error) -> DriverError {
    match e.kind() {
        ErrorKind::DbError(db) => {
            let message = db.message().trim_end().to_string();
            DriverError::Database {
                code: Some(format!("ORA-{:05}", db.code())),
                message,
                detail: None,
                hint: None,
                position: u32::try_from(db.offset())
                    .ok()
                    .filter(|offset| *offset > 0)
                    .map(|offset| offset + 1),
            }
        }
        ErrorKind::Cancelled => DriverError::Cancelled,
        ErrorKind::DeadConnection | ErrorKind::NotConnected | ErrorKind::StreamOperation => {
            DriverError::Connection(with_causes(&e))
        }
        _ => DriverError::Other(with_causes(&e)),
    }
}

/// The error's text followed by its causes (a TLS certificate refused, a socket error…),
/// which the driver's own message leaves out.
fn with_causes(e: &oracledb::Error) -> String {
    let mut text = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

/// Whether an error means the session is gone: SpelaionDB then marks it lost (ADR-028) and
/// never runs anything on it again.
pub(crate) fn is_lost(e: &DriverError) -> bool {
    matches!(e, DriverError::Connection(_))
}
