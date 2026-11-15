//! Tests against a real Oracle Database Free.
//!
//! They need `SPELAION_TEST_ORACLE=host:port` (the plain listener) and, for TLS,
//! `SPELAION_TEST_ORACLE_TCPS=host:port` (TCPS with a certificate for `oracle.spelaion.test`
//! signed by `testing/keys/ca.crt`). User and password `spelaion`, service `FREEPDB1`.
//! Locally: `ci/services.sh` and
//! `SPELAION_TEST_ORACLE=127.0.0.1:11521 SPELAION_TEST_ORACLE_TCPS=127.0.0.1:12484 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, ConstraintKind, Driver, DriverError, ExecEvent, ExecOptions,
    LogicalType, ObjectKind, ResultSchema, Secret, TlsMode, TransactionState, Value,
};
use spelaion_driver_oracle::OracleDriver;

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("port")))
}

fn params(host: String, port: u16) -> ConnectParams {
    ConnectParams {
        host,
        port,
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: "FREEPDB1".into(),
        tls: TlsMode::Prefer,
        ..Default::default()
    }
}

async fn connect() -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_ORACLE")?;
    Some(
        OracleDriver
            .connect(&params(host, port))
            .await
            .expect("connects"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn exec(conn: &dyn Connection, sql: &str) {
    run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
}

/// Drops a table, whether or not it exists.
async fn drop_table(conn: &dyn Connection, table: &str) {
    exec(
        conn,
        &format!(
            "begin execute immediate 'drop table {table} cascade constraints purge'; \
             exception when others then if sqlcode != -942 then raise; end if; end;"
        ),
    )
    .await;
}

fn rows_of(events: Vec<ExecEvent>) -> (ResultSchema, Vec<Vec<Value>>) {
    let mut schema = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.expect("a result"), rows)
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    rows_of(
        run(conn, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}\n→ {e}")),
    )
}

/// Every type, its logical type, its native type and its text (SpelaionDB's cell text).
#[tokio::test]
async fn type_zoo() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    exec(conn, "alter session set time_zone = '+00:00'").await;
    let sql = "select \
        cast(42 as number(9)) as int9, \
        cast(-9007199254740993 as number(18)) as int18, \
        cast(123456789012345678901234567890 as number(38)) as int38, \
        cast(12.34 as number(10,2)) as dec, \
        cast(0.1 as number) as num, \
        cast(1.5 as float) as flt, \
        cast(1.5 as binary_float) as bf, \
        cast(2.25 as binary_double) as bd, \
        cast('texto ñ' as varchar2(20)) as v, \
        cast('ab' as char(4)) as c, \
        cast(n'ünï' as nvarchar2(10)) as nv, \
        to_clob('a long text') as cl, \
        hextoraw('AABB') as rw, \
        to_blob(hextoraw('01FF')) as bl, \
        date '2026-11-15' as d, \
        timestamp '2026-11-15 10:20:30.123456' as ts, \
        timestamp '2026-11-15 10:20:30 +02:00' as tz, \
        interval '3 04:05:06.000007' day to second as ids, \
        interval '1-2' year to month as iym, \
        true as yes, \
        json('{\"b\":[1,2],\"a\":\"x\"}') as js, \
        cast(null as number(9)) as nothing \
      from dual";
    let (schema, rows) = query(conn, sql).await;
    let cells: Vec<(String, LogicalType, String, String)> = schema
        .columns
        .iter()
        .zip(&rows[0])
        .map(|(c, v)| {
            (
                c.name.clone(),
                c.logical,
                c.native_type.clone(),
                v.to_string(),
            )
        })
        .collect();
    let expected: Vec<(&str, LogicalType, &str, &str)> = vec![
        ("INT9", LogicalType::Int, "NUMBER(9)", "42"),
        ("INT18", LogicalType::Int, "NUMBER(18)", "-9007199254740993"),
        (
            "INT38",
            LogicalType::Decimal,
            "NUMBER(38)",
            "123456789012345678901234567890",
        ),
        ("DEC", LogicalType::Decimal, "NUMBER(10,2)", "12.34"),
        ("NUM", LogicalType::Decimal, "NUMBER", "0.1"),
        ("FLT", LogicalType::Decimal, "FLOAT(126)", "1.5"),
        ("BF", LogicalType::Float, "BINARY_FLOAT", "1.5"),
        ("BD", LogicalType::Float, "BINARY_DOUBLE", "2.25"),
        ("V", LogicalType::Text, "VARCHAR2(20)", "texto ñ"),
        ("C", LogicalType::Text, "CHAR(4)", "ab  "),
        ("NV", LogicalType::Text, "NVARCHAR2(10)", "ünï"),
        ("CL", LogicalType::Text, "CLOB", "a long text"),
        ("RW", LogicalType::Bytes, "RAW(2)", "\\xaabb"),
        ("BL", LogicalType::Bytes, "BLOB", "\\x01ff"),
        ("D", LogicalType::Timestamp, "DATE", "2026-11-15 00:00:00"),
        (
            "TS",
            LogicalType::Timestamp,
            "TIMESTAMP(9)",
            "2026-11-15 10:20:30.123456",
        ),
        (
            "TZ",
            LogicalType::TimestampTz,
            "TIMESTAMP(9) WITH TIME ZONE",
            "2026-11-15 08:20:30+00",
        ),
        (
            "IDS",
            LogicalType::Interval,
            "INTERVAL DAY(2) TO SECOND(6)",
            "+03 04:05:06.000007",
        ),
        (
            "IYM",
            LogicalType::Interval,
            "INTERVAL YEAR(2) TO MONTH",
            "+01-02",
        ),
        ("YES", LogicalType::Bool, "BOOLEAN", "true"),
        ("JS", LogicalType::Json, "JSON", r#"{"a":"x","b":[1,2]}"#),
        ("NOTHING", LogicalType::Int, "NUMBER(9)", "NULL"),
    ];
    let differences: Vec<String> = expected
        .iter()
        .zip(&cells)
        .filter(|((name, logical, native, text), cell)| {
            (*name, *logical, *native, *text)
                != (cell.0.as_str(), cell.1, cell.2.as_str(), cell.3.as_str())
        })
        .map(|(e, cell)| format!("expected {e:?}\n     got {cell:?}"))
        .collect();
    assert!(differences.is_empty(), "\n{}", differences.join("\n"));
    assert_eq!(cells.len(), expected.len());
}

/// A refused statement says Oracle's code and message, points at where it failed, and leaves
/// the session usable.
#[tokio::test]
async fn errors_are_the_servers_and_the_session_survives() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    match run(conn, "select * from a_table_that_is_not_there").await {
        Err(DriverError::Database {
            code,
            message,
            position,
            ..
        }) => {
            assert_eq!(code.as_deref(), Some("ORA-00942"));
            assert!(message.contains("does not exist"), "{message}");
            assert_eq!(position, Some(15));
        }
        other => panic!("{other:?}"),
    }
    let (_, rows) = query(conn, "select 'still here' from dual").await;
    assert_eq!(rows[0][0].to_string(), "still here");
    assert!(!conn.is_closed());
}

#[tokio::test]
async fn a_wrong_password_never_shows_the_password() {
    let Some((host, port)) = addr("SPELAION_TEST_ORACLE") else {
        return;
    };
    let mut wrong = params(host, port);
    wrong.password = Some(Secret::new("not-the-Password-42"));
    let error = OracleDriver.connect(&wrong).await.err().expect("refused");
    let text = error.to_string();
    assert!(text.contains("ORA-01017"), "{text}");
    assert!(!text.contains("not-the-Password-42"), "{text}");
}

#[tokio::test]
async fn statements_report_rows_affected_and_transactions_are_read_from_the_driver() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let table = format!("spelaion_tx_{}", std::process::id());
    drop_table(conn, &table).await;
    exec(
        conn,
        &format!("create table {table} (id number(9) primary key)"),
    )
    .await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    let events = run(
        conn,
        &format!("insert into {table} select level from dual connect by level <= 3"),
    )
    .await
    .unwrap();
    assert!(matches!(
        events.last(),
        Some(ExecEvent::Done {
            rows_affected: Some(3)
        })
    ));
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Open
    );
    exec(conn, "rollback").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    let (_, rows) = query(conn, &format!("select count(*) from {table}")).await;
    assert_eq!(rows[0][0].to_string(), "0");
    drop_table(conn, &table).await;
}

#[tokio::test]
async fn bound_values_are_bound_by_position() {
    let Some(conn) = connect().await else { return };
    let events = conn
        .execute_bound(
            "select :1 || '-' || :2 from dual",
            vec!["it's".into(), "') or 1=1 --".into()],
            &ExecOptions::default(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let (_, rows) = rows_of(events);
    assert_eq!(rows[0][0].to_string(), "it's-') or 1=1 --");
}

#[tokio::test]
async fn batches_respect_the_batch_size() {
    let Some(conn) = connect().await else { return };
    let events: Vec<ExecEvent> = conn
        .execute(
            "select level from dual connect by level <= 2500",
            &ExecOptions {
                batch_size: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let sizes: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(sizes, [1000, 1000, 500]);
}

/// Waits for a statement's end; a cancel that is lost fails here in seconds, not never.
async fn finish(
    conn: Arc<dyn Connection>,
    sql: &'static str,
) -> Result<Vec<ExecEvent>, DriverError> {
    tokio::time::timeout(Duration::from_secs(20), async move {
        conn.execute(sql, &ExecOptions::default())
            .await?
            .try_collect::<Vec<_>>()
            .await
    })
    .await
    .expect("the statement ended within 20 s")
}

#[tokio::test]
async fn cancel_stops_a_busy_call_and_a_query_whose_rows_keep_coming() {
    let Some(conn) = connect().await else { return };
    let conn: Arc<dyn Connection> = Arc::from(conn);
    for sql in [
        "begin dbms_session.sleep(600); end;",
        "select a.object_name from all_objects a, all_objects b",
    ] {
        let running = tokio::spawn(finish(conn.clone(), sql));
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let started = Instant::now();
        conn.cancel_handle().cancel().await.unwrap();
        let result = running.await.unwrap();
        assert!(
            matches!(result, Err(DriverError::Cancelled)),
            "{sql}: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{sql}: {:?}",
            started.elapsed()
        );
        let (_, rows) = query(conn.as_ref(), "select 'after' from dual").await;
        assert_eq!(rows[0][0].to_string(), "after", "{sql}");
    }
}

#[tokio::test]
async fn a_cancel_never_reaches_a_later_statement() {
    let Some(conn) = connect().await else { return };
    conn.cancel_handle().cancel().await.unwrap();
    let (_, rows) = query(conn.as_ref(), "select 1 from dual").await;
    assert_eq!(rows[0][0].to_string(), "1");
}

#[tokio::test]
async fn the_explorer_reads_schemas_tables_keys_and_columns() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let suffix = std::process::id();
    let (parent, child, view) = (
        format!("SPELAION_PARENT_{suffix}"),
        format!("SPELAION_CHILD_{suffix}"),
        format!("SPELAION_VIEW_{suffix}"),
    );
    drop_table(conn, &child).await;
    drop_table(conn, &parent).await;
    exec(
        conn,
        &format!(
            "create table {parent} (id number(9) constraint {parent}_pk primary key, \
             code varchar2(10 char) not null constraint {parent}_uk unique, \
             amount number(10,2) default 0 constraint {parent}_ck check (amount >= 0))"
        ),
    )
    .await;
    exec(conn, &format!("comment on table {parent} is 'parents'")).await;
    exec(
        conn,
        &format!("comment on column {parent}.code is 'the code'"),
    )
    .await;
    exec(
        conn,
        &format!(
            "create table {child} (id number(9) primary key, \
             parent_id number(9) constraint {child}_fk references {parent} (id))"
        ),
    )
    .await;
    exec(
        conn,
        &format!("create index {child}_ix on {child} (parent_id)"),
    )
    .await;
    exec(
        conn,
        &format!("create or replace view {view} as select id from {parent}"),
    )
    .await;

    let schemas = conn.list_schemas().await.unwrap();
    assert!(schemas.iter().any(|s| s.name == "SPELAION" && !s.system));
    assert!(schemas.iter().any(|s| s.name == "SYS" && s.system));

    let objects = conn.list_objects("SPELAION").await.unwrap();
    let kind = |name: &str| objects.iter().find(|o| o.name == name).map(|o| o.kind);
    assert_eq!(kind(&parent), Some(ObjectKind::Table));
    assert_eq!(kind(&view), Some(ObjectKind::View));
    let parent_info = objects.iter().find(|o| o.name == parent).unwrap();
    assert_eq!(parent_info.comment.as_deref(), Some("parents"));

    let table = conn.describe_table("SPELAION", &parent).await.unwrap();
    let columns: Vec<(&str, &str, bool)> = table
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.native_type.as_str(), c.nullable))
        .collect();
    assert_eq!(
        columns,
        [
            ("ID", "NUMBER(9)", false),
            ("CODE", "VARCHAR2(10 CHAR)", false),
            ("AMOUNT", "NUMBER(10,2)", true),
        ]
    );
    assert_eq!(table.columns[1].comment.as_deref(), Some("the code"));
    assert_eq!(table.columns[2].default.as_deref(), Some("0"));
    assert_eq!(table.primary_key, ["ID"]);
    let kinds: Vec<ConstraintKind> = table.constraints.iter().map(|c| c.kind).collect();
    assert!(
        kinds.contains(&ConstraintKind::Unique),
        "{:?}",
        table.constraints
    );
    let check = table
        .constraints
        .iter()
        .find(|c| c.kind == ConstraintKind::Check)
        .expect("the check, without the generated NOT NULL ones");
    assert_eq!(check.definition, "CHECK (amount >= 0)");
    assert_eq!(
        table
            .constraints
            .iter()
            .filter(|c| c.kind == ConstraintKind::Check)
            .count(),
        1
    );

    let child_info = conn.describe_table("SPELAION", &child).await.unwrap();
    assert_eq!(child_info.foreign_keys.len(), 1);
    let fk = &child_info.foreign_keys[0];
    assert_eq!(
        (
            fk.columns.clone(),
            fk.referenced_schema.as_str(),
            fk.referenced_table.as_str(),
            fk.referenced_columns.clone()
        ),
        (
            vec!["PARENT_ID".to_string()],
            "SPELAION",
            parent.as_str(),
            vec!["ID".to_string()]
        )
    );
    assert!(
        child_info
            .indexes
            .iter()
            .any(|i| i.name == format!("{child}_IX") && !i.unique)
    );

    let referencing = conn
        .list_referencing_keys("SPELAION", &parent)
        .await
        .unwrap();
    assert_eq!(referencing.len(), 1);
    assert_eq!(
        (
            referencing[0].table.as_str(),
            referencing[0].columns.clone()
        ),
        (child.as_str(), vec!["PARENT_ID".to_string()])
    );

    let all = conn.list_columns("SPELAION").await.unwrap();
    assert!(
        all.iter()
            .any(|c| c.table == child && c.name == "PARENT_ID" && c.native_type == "NUMBER(9)")
    );

    exec(conn, &format!("drop view {view}")).await;
    drop_table(conn, &child).await;
    drop_table(conn, &parent).await;
}

fn tcps(host_name: &str) -> Option<ConnectParams> {
    let (host, port) = addr("SPELAION_TEST_ORACLE_TCPS")?;
    let mut p = params(host_name.into(), 0);
    p.connect_addr = Some(format!("{host}:{port}").parse().expect("an address"));
    p.tls = TlsMode::VerifyFull;
    p.ca_certificate = Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys/ca.crt"));
    Some(p)
}

/// TLS to the address a tunnel gives (127.0.0.1), the certificate checked against the
/// connection's host name: the name in the certificate works, any other is refused, and so
/// is a certificate no trusted authority signed.
#[tokio::test]
async fn tls_is_verified_against_the_host_name_not_the_address() {
    let Some(good) = tcps("oracle.spelaion.test") else {
        return;
    };
    let conn = OracleDriver
        .connect(&good)
        .await
        .expect("verify-full against the name");
    let (_, rows) = query(conn.as_ref(), "select 'over tls' from dual").await;
    assert_eq!(rows[0][0].to_string(), "over tls");

    let wrong = tcps("another.spelaion.test").unwrap();
    let error = OracleDriver
        .connect(&wrong)
        .await
        .err()
        .expect("another name is refused");
    assert!(
        error.to_string().to_lowercase().contains("certificate"),
        "{error}"
    );

    let mut untrusted = tcps("oracle.spelaion.test").unwrap();
    untrusted.ca_certificate = None;
    let error = OracleDriver
        .connect(&untrusted)
        .await
        .err()
        .expect("an unknown CA is refused");
    assert!(
        error.to_string().to_lowercase().contains("certificate"),
        "{error}"
    );
}
