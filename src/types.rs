//! Oracle's types as SpelaionDB's logical types, and rows of `oracledb` values as Arrow batches.
//!
//! Each column's logical type is decided from its metadata alone, before any row is read, so a
//! result's schema never depends on its values:
//!
//! - `NUMBER(p)` with `p <= 18` and no scale is an integer; every other `NUMBER` and `FLOAT` is
//!   an exact decimal, kept as Oracle's own text (`OracleNumber`), so nothing is rounded.
//! - `BINARY_FLOAT` and `BINARY_DOUBLE` are floats.
//! - `DATE` carries a time in Oracle, so it is a timestamp (to the second). `TIMESTAMP` is a
//!   timestamp, `TIMESTAMP WITH TIME ZONE` an instant (the driver reads it in UTC), and
//!   `TIMESTAMP WITH LOCAL TIME ZONE` the session's wall-clock time.
//! - Character types, `LONG`, `CLOB`, `NCLOB`, `ROWID` and `UROWID` are text. `RAW`,
//!   `LONG RAW` and `BLOB` are bytes; LOBs arrive whole (the driver does not fetch locators).
//! - `INTERVAL` is text as SQL*Plus writes it (`+03 04:05:06.000000`, `+01-02`), `BOOLEAN` a
//!   boolean, `JSON` its JSON text, and `VECTOR` its values as a JSON array.
//! - Anything else (objects, collections, `XMLTYPE`, `BFILE`) is refused when the query
//!   starts, naming the column and its type, instead of being shown wrongly.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use oracledb::{JsonValue, Metadata, OracleIntervalDS, OracleIntervalYM, OracleTimestamp, Row};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// How one column's values are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reader {
    Integer,
    Decimal,
    Float,
    Text,
    Bytes,
    Timestamp,
    TimestampUtc,
    IntervalDs,
    IntervalYm,
    Boolean,
    Json,
    Vector,
}

impl Reader {
    fn logical(self) -> LogicalType {
        match self {
            Reader::Integer => LogicalType::Int,
            Reader::Decimal => LogicalType::Decimal,
            Reader::Float => LogicalType::Float,
            Reader::Text => LogicalType::Text,
            Reader::Bytes => LogicalType::Bytes,
            Reader::Timestamp => LogicalType::Timestamp,
            Reader::TimestampUtc => LogicalType::TimestampTz,
            Reader::IntervalDs | Reader::IntervalYm => LogicalType::Interval,
            Reader::Boolean => LogicalType::Bool,
            Reader::Json => LogicalType::Json,
            // A vector is shown as the JSON array of its values.
            Reader::Vector => LogicalType::Native,
        }
    }
}

/// The reader for a column, from the driver's name of its type (`DB_TYPE_NUMBER`…).
pub(crate) fn reader(meta: &Metadata) -> Result<Reader> {
    Ok(match meta.db_type().name() {
        "DB_TYPE_NUMBER" | "DB_TYPE_BINARY_INTEGER" => {
            if meta.scale() == 0 && (1..=18).contains(&meta.precision()) {
                Reader::Integer
            } else {
                Reader::Decimal
            }
        }
        "DB_TYPE_BINARY_FLOAT" | "DB_TYPE_BINARY_DOUBLE" => Reader::Float,
        "DB_TYPE_VARCHAR"
        | "DB_TYPE_NVARCHAR"
        | "DB_TYPE_CHAR"
        | "DB_TYPE_NCHAR"
        | "DB_TYPE_LONG"
        | "DB_TYPE_LONG_NVARCHAR"
        | "DB_TYPE_CLOB"
        | "DB_TYPE_NCLOB"
        | "DB_TYPE_ROWID"
        | "DB_TYPE_UROWID" => Reader::Text,
        "DB_TYPE_RAW" | "DB_TYPE_LONG_RAW" | "DB_TYPE_BLOB" => Reader::Bytes,
        "DB_TYPE_DATE" | "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_LTZ" => Reader::Timestamp,
        "DB_TYPE_TIMESTAMP_TZ" => Reader::TimestampUtc,
        "DB_TYPE_INTERVAL_DS" => Reader::IntervalDs,
        "DB_TYPE_INTERVAL_YM" => Reader::IntervalYm,
        "DB_TYPE_BOOLEAN" => Reader::Boolean,
        "DB_TYPE_JSON" => Reader::Json,
        "DB_TYPE_VECTOR" => Reader::Vector,
        other => {
            return Err(DriverError::Decode {
                type_name: meta.data_type(),
                message: format!(
                    "column {:?} has a type SpelaionDB cannot show yet ({other}); select it as text, for example with TO_CHAR or JSON_SERIALIZE",
                    meta.name()
                ),
            });
        }
    })
}

/// The result's schema and each column's reader.
pub(crate) fn schema(columns: &[Metadata]) -> Result<(Arc<ResultSchema>, Vec<Reader>)> {
    let mut metas = Vec::with_capacity(columns.len());
    let mut readers = Vec::with_capacity(columns.len());
    for column in columns {
        let reader = reader(column)?;
        metas.push(ColumnMeta {
            name: column.name().to_string(),
            logical: reader.logical(),
            native_type: column.data_type(),
            native_id: None,
            origin: None,
        });
        readers.push(reader);
    }
    Ok((Arc::new(ResultSchema::new(metas)), readers))
}

/// One column being filled.
enum Builder {
    Int(Int64Builder),
    Float(Float64Builder),
    Text(StringBuilder),
    Bytes(BinaryBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampUtc(TimestampMicrosecondBuilder),
    Bool(BooleanBuilder),
}

/// Rows read into Arrow columns, a batch at a time.
pub(crate) struct Batch {
    schema: Arc<ResultSchema>,
    readers: Vec<Reader>,
    builders: Vec<Builder>,
    rows: usize,
}

impl Batch {
    pub(crate) fn new(schema: Arc<ResultSchema>, readers: Vec<Reader>) -> Self {
        let builders = readers.iter().map(|r| builder(*r)).collect();
        Self {
            schema,
            readers,
            builders,
            rows: 0,
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn push(&mut self, row: &Row) -> Result<()> {
        for (i, (reader, builder)) in self.readers.iter().zip(&mut self.builders).enumerate() {
            append(*reader, builder, row, i).map_err(|message| DriverError::Decode {
                type_name: self.schema.columns[i].native_type.clone(),
                message: format!("column {:?}: {message}", self.schema.columns[i].name),
            })?;
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows read so far as one batch; the builders start again empty.
    pub(crate) fn take(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|b| -> ArrayRef {
                match b {
                    Builder::Int(b) => Arc::new(b.finish()),
                    Builder::Float(b) => Arc::new(b.finish()),
                    Builder::Text(b) => Arc::new(b.finish()),
                    Builder::Bytes(b) => Arc::new(b.finish()),
                    Builder::Timestamp(b) => Arc::new(b.finish()),
                    Builder::TimestampUtc(b) => Arc::new(b.finish().with_timezone("UTC")),
                    Builder::Bool(b) => Arc::new(b.finish()),
                }
            })
            .collect();
        self.rows = 0;
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
    }
}

fn builder(reader: Reader) -> Builder {
    match reader {
        Reader::Integer => Builder::Int(Int64Builder::new()),
        Reader::Float => Builder::Float(Float64Builder::new()),
        Reader::Decimal
        | Reader::Text
        | Reader::IntervalDs
        | Reader::IntervalYm
        | Reader::Json
        | Reader::Vector => Builder::Text(StringBuilder::new()),
        Reader::Bytes => Builder::Bytes(BinaryBuilder::new()),
        Reader::Timestamp => Builder::Timestamp(TimestampMicrosecondBuilder::new()),
        Reader::TimestampUtc => Builder::TimestampUtc(TimestampMicrosecondBuilder::new()),
        Reader::Boolean => Builder::Bool(BooleanBuilder::new()),
    }
}

/// Reads one cell into its column; the error says what could not be read.
fn append(
    reader: Reader,
    builder: &mut Builder,
    row: &Row,
    i: usize,
) -> std::result::Result<(), String> {
    let err = |e: oracledb::Error| e.to_string();
    match (reader, builder) {
        (Reader::Integer, Builder::Int(b)) => match row.get::<Option<String>>(i).map_err(err)? {
            None => b.append_null(),
            Some(text) => b.append_value(
                text.parse()
                    .map_err(|_| format!("{text} does not fit in a 64-bit integer"))?,
            ),
        },
        (Reader::Decimal, Builder::Text(b)) => {
            b.append_option(row.get::<Option<String>>(i).map_err(err)?)
        }
        // BINARY_FLOAT comes as `f32` and BINARY_DOUBLE as `f64`.
        (Reader::Float, Builder::Float(b)) => match row.get::<Option<f64>>(i) {
            Ok(value) => b.append_option(value),
            Err(_) => b.append_option(row.get::<Option<f32>>(i).map_err(err)?.map(f64::from)),
        },
        (Reader::Text, Builder::Text(b)) => {
            b.append_option(row.get::<Option<String>>(i).map_err(err)?)
        }
        (Reader::Bytes, Builder::Bytes(b)) => {
            b.append_option(row.get::<Option<Vec<u8>>>(i).map_err(err)?)
        }
        (Reader::Timestamp, Builder::Timestamp(b))
        | (Reader::TimestampUtc, Builder::TimestampUtc(b)) => b.append_option(
            row.get::<Option<OracleTimestamp>>(i)
                .map_err(err)?
                .map(|t| micros(&t)),
        ),
        (Reader::IntervalDs, Builder::Text(b)) => b.append_option(
            row.get::<Option<OracleIntervalDS>>(i)
                .map_err(err)?
                .map(|v| interval_ds(&v)),
        ),
        (Reader::IntervalYm, Builder::Text(b)) => b.append_option(
            row.get::<Option<OracleIntervalYM>>(i)
                .map_err(err)?
                .map(|v| interval_ym(&v)),
        ),
        (Reader::Boolean, Builder::Bool(b)) => {
            b.append_option(row.get::<Option<bool>>(i).map_err(err)?)
        }
        (Reader::Json, Builder::Text(b)) => b.append_option(
            row.get::<Option<JsonValue>>(i)
                .map_err(err)?
                .map(|v| json_text(&v)),
        ),
        (Reader::Vector, Builder::Text(b)) => b.append_option(
            row.get::<Option<oracledb::Vector>>(i)
                .map_err(err)?
                .map(|v| vector_text(&v)),
        ),
        (reader, _) => return Err(format!("no column builder for {reader:?}")),
    }
    Ok(())
}

/// Days from 1970-01-01 to a civil date (proleptic Gregorian), Howard Hinnant's algorithm.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Microseconds since 1970-01-01 of a timestamp's fields (truncating nanoseconds).
pub(crate) fn micros(t: &OracleTimestamp) -> i64 {
    let days = days_from_civil(
        i64::from(t.year()),
        u32::from(t.month()),
        u32::from(t.day()),
    );
    let seconds = days * 86_400
        + i64::from(t.hour()) * 3_600
        + i64::from(t.minute()) * 60
        + i64::from(t.second());
    seconds * 1_000_000 + i64::from(t.nanoseconds()) / 1_000
}

/// `+DD HH:MI:SS.FFFFFF`, as SQL*Plus writes an `INTERVAL DAY TO SECOND`.
pub(crate) fn interval_ds(v: &OracleIntervalDS) -> String {
    let negative =
        v.days() < 0 || v.hours() < 0 || v.minutes() < 0 || v.seconds() < 0 || v.nanoseconds() < 0;
    format!(
        "{}{:02} {:02}:{:02}:{:02}.{:06}",
        if negative { '-' } else { '+' },
        v.days().unsigned_abs(),
        v.hours().unsigned_abs(),
        v.minutes().unsigned_abs(),
        v.seconds().unsigned_abs(),
        v.nanoseconds().unsigned_abs() / 1_000
    )
}

/// `+YY-MM`, as SQL*Plus writes an `INTERVAL YEAR TO MONTH`.
pub(crate) fn interval_ym(v: &OracleIntervalYM) -> String {
    let negative = v.years() < 0 || v.months() < 0;
    format!(
        "{}{:02}-{:02}",
        if negative { '-' } else { '+' },
        v.years().unsigned_abs(),
        v.months().unsigned_abs()
    )
}

/// JSON text of a `JSON` value: objects with their keys in order, Oracle's scalars as strings
/// where JSON has no type for them (dates, intervals, raw bytes in hexadecimal).
pub(crate) fn json_text(v: &JsonValue) -> String {
    let mut out = String::new();
    write_json(v, &mut out);
    out
}

fn write_json(v: &JsonValue, out: &mut String) {
    match v {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        JsonValue::Number(n) => out.push_str(&n.to_string()),
        JsonValue::BinaryDouble(f) => out.push_str(&float_json(*f)),
        JsonValue::BinaryFloat(f) => out.push_str(&float_json(f64::from(*f))),
        JsonValue::String(s) => write_json_string(s, out),
        JsonValue::IntervalDS(i) => write_json_string(&interval_ds(i), out),
        JsonValue::IntervalYM(i) => write_json_string(&interval_ym(i), out),
        JsonValue::Timestamp(t) => write_json_string(&timestamp_text(t), out),
        JsonValue::Raw(bytes) | JsonValue::JsonId(bytes) => write_json_string(&hex(bytes), out),
        JsonValue::Vector(vector) => out.push_str(&vector_text(vector)),
        JsonValue::JsonArray(items) => {
            out.push('[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.push(',');
                }
                write_json(item, out);
            }
            out.push(']');
        }
        JsonValue::JsonObject(fields) => {
            // The driver keeps fields in a map: sorted, so the text is always the same.
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();
            out.push('{');
            for (n, key) in keys.into_iter().enumerate() {
                if n > 0 {
                    out.push(',');
                }
                write_json_string(key, out);
                out.push(':');
                write_json(&fields[key], out);
            }
            out.push('}');
        }
    }
}

fn float_json(f: f64) -> String {
    if f.is_finite() {
        f.to_string()
    } else {
        format!("\"{f}\"")
    }
}

fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

/// `YYYY-MM-DD HH:MI:SS.FFFFFF`.
fn timestamp_text(t: &OracleTimestamp) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.nanoseconds() / 1_000
    )
}

/// A vector's values as a JSON array; a sparse one as `[dimensions,[indices],[values]]`, the
/// form Oracle's own `VECTOR` text uses.
pub(crate) fn vector_text(v: &oracledb::Vector) -> String {
    fn values(data: &oracledb::VectorData) -> Vec<String> {
        match data {
            oracledb::VectorData::Float32(v) => {
                v.iter().map(|x| float_json(f64::from(*x))).collect()
            }
            oracledb::VectorData::Float64(v) => v.iter().map(|x| float_json(*x)).collect(),
            oracledb::VectorData::Int8(v) => v.iter().map(i8::to_string).collect(),
            oracledb::VectorData::Binary(v) => v.iter().map(u8::to_string).collect(),
        }
    }
    match v {
        oracledb::Vector::Dense(data) => format!("[{}]", values(data).join(",")),
        oracledb::Vector::Sparse(sparse) => format!(
            "[{},[{}],[{}]]",
            sparse.num_dimensions(),
            sparse
                .indices()
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
            values(sparse.values()).join(",")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_days_match_known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(1900, 1, 1), -25_567);
    }

    #[test]
    fn intervals_are_written_as_sqlplus_does() {
        assert_eq!(
            interval_ds(&OracleIntervalDS::new(3, 4, 5, 6, 7_000)),
            "+03 04:05:06.000007"
        );
        assert_eq!(
            interval_ds(&OracleIntervalDS::new(-1, -2, 0, 0, 0)),
            "-01 02:00:00.000000"
        );
        assert_eq!(interval_ym(&OracleIntervalYM::new(1, 2)), "+01-02");
        assert_eq!(interval_ym(&OracleIntervalYM::new(-3, -4)), "-03-04");
    }

    #[test]
    fn json_strings_are_escaped() {
        let mut out = String::new();
        write_json_string("a\"b\\c\nd\u{1}", &mut out);
        assert_eq!(out, r#""a\"b\\c\nd\u0001""#);
    }
}
