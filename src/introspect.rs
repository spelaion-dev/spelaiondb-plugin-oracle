//! The explorer, from Oracle's `ALL_*` views: what the logged-in user may see. Every name is
//! bound as a parameter (`:1`, `:2`), never written into the query's text.
//!
//! Oracle's schemas are its users. A schema is a system one when Oracle maintains it
//! (`ORACLE_MAINTAINED = 'Y'`, Oracle 12.1 and later, which is also the thin driver's floor).
//! Tables that hold a materialized view's rows, the recycle bin (`BIN$…`), nested tables and
//! the secondary tables of domain indexes are not shown as tables.

use spelaion_driver_api::{
    ColumnInfo, ConstraintInfo, ConstraintKind, ForeignKeyInfo, IndexInfo, ObjectInfo, ObjectKind,
    ReferencingKey, Result, SchemaColumn, SchemaInfo, TableInfo, check_definition,
};

use crate::exec::map_error;

/// Every row of a catalog query, each cell as text (`None` for NULL).
fn rows(
    conn: &oracledb::Connection,
    sql: &str,
    params: &[&str],
) -> Result<Vec<Vec<Option<String>>>> {
    let binds: Vec<&dyn oracledb::ToDbValue> = params
        .iter()
        .map(|p| p as &dyn oracledb::ToDbValue)
        .collect();
    let cursor = conn.query(sql, &binds).map_err(map_error)?;
    let width = cursor.columns().len();
    let mut out = Vec::new();
    for row in cursor {
        let row = row.map_err(map_error)?;
        let mut cells = Vec::with_capacity(width);
        for i in 0..width {
            cells.push(row.get::<Option<String>>(i).map_err(map_error)?);
        }
        out.push(cells);
    }
    Ok(out)
}

fn text(cell: &Option<String>) -> String {
    cell.clone().unwrap_or_default()
}

pub(crate) fn list_schemas(conn: &oracledb::Connection) -> Result<Vec<SchemaInfo>> {
    Ok(rows(
        conn,
        "select username, oracle_maintained from all_users order by username",
        &[],
    )?
    .iter()
    .map(|r| SchemaInfo {
        name: text(&r[0]),
        system: r[1].as_deref() == Some("Y"),
    })
    .collect())
}

const OBJECTS: &str = "\
select o.object_name, o.object_type, t.num_rows, t.partitioned, c.comments
  from all_objects o
  left join all_tables t on t.owner = o.owner and t.table_name = o.object_name
  left join all_tab_comments c on c.owner = o.owner and c.table_name = o.object_name
 where o.owner = :1
   and o.object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
   and o.subobject_name is null
   and o.object_name not like 'BIN$%'
   and nvl(t.nested, 'NO') = 'NO'
   and nvl(t.secondary, 'N') = 'N'
   and not (o.object_type = 'TABLE' and exists (
         select 1 from all_mviews m where m.owner = o.owner and m.mview_name = o.object_name))
 order by o.object_name";

pub(crate) fn list_objects(conn: &oracledb::Connection, schema: &str) -> Result<Vec<ObjectInfo>> {
    Ok(rows(conn, OBJECTS, &[schema])?
        .iter()
        .map(|r| ObjectInfo {
            schema: schema.to_string(),
            name: text(&r[0]),
            kind: match (r[1].as_deref(), r[3].as_deref()) {
                (Some("VIEW"), _) => ObjectKind::View,
                (Some("MATERIALIZED VIEW"), _) => ObjectKind::MaterializedView,
                (_, Some("YES")) => ObjectKind::PartitionedTable,
                _ => ObjectKind::Table,
            },
            estimated_rows: r[2].as_deref().and_then(|n| n.parse().ok()),
            comment: r[4].clone(),
        })
        .collect())
}

/// A column's type as written in `CREATE TABLE`, from `ALL_TAB_COLUMNS`.
pub(crate) fn column_type(
    data_type: &str,
    data_length: Option<&str>,
    char_length: Option<&str>,
    char_used: Option<&str>,
    precision: Option<&str>,
    scale: Option<&str>,
) -> String {
    match data_type {
        "VARCHAR2" | "CHAR" => match (char_used, char_length) {
            (Some("C"), Some(n)) => format!("{data_type}({n} CHAR)"),
            _ => format!("{data_type}({})", data_length.unwrap_or("1")),
        },
        "NVARCHAR2" | "NCHAR" => format!("{data_type}({})", char_length.unwrap_or("1")),
        "RAW" => format!("RAW({})", data_length.unwrap_or("1")),
        "NUMBER" => match (precision, scale) {
            (None, None) => "NUMBER".into(),
            (None, Some("0")) => "INTEGER".into(),
            (None, Some(s)) => format!("NUMBER(*,{s})"),
            (Some(p), None | Some("0")) => format!("NUMBER({p})"),
            (Some(p), Some(s)) => format!("NUMBER({p},{s})"),
        },
        "FLOAT" => match precision {
            Some(p) => format!("FLOAT({p})"),
            None => "FLOAT".into(),
        },
        other => other.to_string(),
    }
}

const COLUMNS: &str = "\
select c.column_name, c.data_type, c.data_length, c.char_length, c.char_used,
       c.data_precision, c.data_scale, c.nullable, c.data_default, m.comments
  from all_tab_columns c
  left join all_col_comments m
    on m.owner = c.owner and m.table_name = c.table_name and m.column_name = c.column_name
 where c.owner = :1 and c.table_name = :2
 order by c.column_id";

/// A constraint's columns, in their order.
fn constraint_columns(
    conn: &oracledb::Connection,
    schema: &str,
    constraint: &str,
) -> Result<Vec<String>> {
    Ok(rows(
        conn,
        "select column_name from all_cons_columns \
          where owner = :1 and constraint_name = :2 order by position",
        &[schema, constraint],
    )?
    .iter()
    .map(|r| text(&r[0]))
    .collect())
}

pub(crate) fn describe_table(
    conn: &oracledb::Connection,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let object = list_objects(conn, schema)?
        .into_iter()
        .find(|o| o.name == name)
        .ok_or_else(|| spelaion_driver_api::DriverError::Database {
            code: None,
            message: format!("{schema}.{name} is not a table or view this user can see"),
            detail: None,
            hint: None,
            position: None,
        })?;
    let columns = rows(conn, COLUMNS, &[schema, name])?
        .iter()
        .map(|r| ColumnInfo {
            name: text(&r[0]),
            native_type: column_type(
                &text(&r[1]),
                r[2].as_deref(),
                r[3].as_deref(),
                r[4].as_deref(),
                r[5].as_deref(),
                r[6].as_deref(),
            ),
            nullable: r[7].as_deref() != Some("N"),
            default: r[8]
                .as_deref()
                .map(str::trim)
                .filter(|d| !d.is_empty() && !d.eq_ignore_ascii_case("null"))
                .map(str::to_string),
            comment: r[9].clone(),
        })
        .collect();

    let constraints = rows(
        conn,
        "select constraint_name, constraint_type, search_condition, generated, \
                r_owner, r_constraint_name \
           from all_constraints \
          where owner = :1 and table_name = :2 and constraint_type in ('P', 'U', 'C', 'R') \
          order by constraint_name",
        &[schema, name],
    )?;
    let mut primary_key = Vec::new();
    let mut checks = Vec::new();
    let mut foreign_keys = Vec::new();
    for c in &constraints {
        let constraint = text(&c[0]);
        let columns = constraint_columns(conn, schema, &constraint)?;
        match c[1].as_deref() {
            Some("P") => {
                checks.push(ConstraintInfo {
                    name: Some(constraint.clone()),
                    kind: ConstraintKind::PrimaryKey,
                    columns: columns.clone(),
                    definition: format!("PRIMARY KEY ({})", quoted(&columns)),
                });
                primary_key = columns;
            }
            Some("U") => checks.push(ConstraintInfo {
                name: Some(constraint),
                definition: format!("UNIQUE ({})", quoted(&columns)),
                kind: ConstraintKind::Unique,
                columns,
            }),
            Some("C") => {
                let condition = text(&c[2]);
                // `"A" IS NOT NULL`, which Oracle generates for every NOT NULL column, is
                // already the column's nullability.
                let not_null = c[3].as_deref() == Some("GENERATED NAME")
                    && columns.len() == 1
                    && condition.trim() == format!("\"{}\" IS NOT NULL", columns[0]);
                if !not_null {
                    checks.push(ConstraintInfo {
                        name: Some(constraint),
                        kind: ConstraintKind::Check,
                        columns,
                        definition: check_definition(&condition),
                    });
                }
            }
            Some("R") => {
                let referenced_schema = text(&c[4]);
                let referenced = text(&c[5]);
                let target = rows(
                    conn,
                    "select table_name from all_constraints where owner = :1 and constraint_name = :2",
                    &[&referenced_schema, &referenced],
                )?;
                foreign_keys.push(ForeignKeyInfo {
                    name: constraint,
                    columns,
                    referenced_table: target.first().map(|r| text(&r[0])).unwrap_or_default(),
                    referenced_columns: constraint_columns(conn, &referenced_schema, &referenced)?,
                    referenced_schema,
                });
            }
            _ => {}
        }
    }

    let mut indexes = Vec::new();
    for i in rows(
        conn,
        "select index_name, uniqueness from all_indexes \
          where table_owner = :1 and table_name = :2 and index_type not like 'LOB%' \
          order by index_name",
        &[schema, name],
    )? {
        let index = text(&i[0]);
        let columns: Vec<String> = rows(
            conn,
            "select column_name from all_ind_columns \
              where index_owner = :1 and index_name = :2 order by column_position",
            &[schema, &index],
        )?
        .iter()
        .map(|r| text(&r[0]))
        .collect();
        let unique = i[1].as_deref() == Some("UNIQUE");
        indexes.push(IndexInfo {
            primary: constraints
                .iter()
                .any(|c| c[1].as_deref() == Some("P") && c[0].as_deref() == Some(index.as_str())),
            definition: format!(
                "CREATE {}INDEX \"{}\" ON \"{}\".\"{}\" ({})",
                if unique { "UNIQUE " } else { "" },
                index.replace('"', "\"\""),
                schema.replace('"', "\"\""),
                name.replace('"', "\"\""),
                quoted(&columns)
            ),
            name: index,
            unique,
        });
    }

    Ok(TableInfo {
        object,
        columns,
        primary_key,
        indexes,
        foreign_keys,
        constraints: checks,
    })
}

/// Column names quoted and separated by commas.
fn quoted(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The foreign keys of every table that point to `schema.name`: `describe_table`'s foreign
/// keys seen from the other side.
pub(crate) fn list_referencing_keys(
    conn: &oracledb::Connection,
    schema: &str,
    name: &str,
) -> Result<Vec<ReferencingKey>> {
    let mut out = Vec::new();
    for r in rows(
        conn,
        "select f.owner, f.table_name, f.constraint_name, f.r_owner, f.r_constraint_name \
           from all_constraints f \
           join all_constraints k on k.owner = f.r_owner and k.constraint_name = f.r_constraint_name \
          where f.constraint_type = 'R' and k.owner = :1 and k.table_name = :2 \
          order by f.owner, f.table_name, f.constraint_name",
        &[schema, name],
    )? {
        let (owner, constraint) = (text(&r[0]), text(&r[2]));
        out.push(ReferencingKey {
            columns: constraint_columns(conn, &owner, &constraint)?,
            referenced_columns: constraint_columns(conn, &text(&r[3]), &text(&r[4]))?,
            schema: owner,
            table: text(&r[1]),
            name: constraint,
        });
    }
    Ok(out)
}

/// Every column of a schema's tables and views in one query (metadata search).
pub(crate) fn list_columns(conn: &oracledb::Connection, schema: &str) -> Result<Vec<SchemaColumn>> {
    Ok(rows(
        conn,
        "select table_name, column_name, data_type, data_length, char_length, char_used, \
                data_precision, data_scale \
           from all_tab_columns where owner = :1 and table_name not like 'BIN$%' \
          order by table_name, column_id",
        &[schema],
    )?
    .iter()
    .map(|r| SchemaColumn {
        table: text(&r[0]),
        name: text(&r[1]),
        native_type: column_type(
            &text(&r[2]),
            r[3].as_deref(),
            r[4].as_deref(),
            r[5].as_deref(),
            r[6].as_deref(),
            r[7].as_deref(),
        ),
    })
    .collect())
}

#[cfg(test)]
mod tests {
    use super::column_type;

    #[test]
    fn column_types_are_written_as_create_table_does() {
        let t = |dt, len, clen, used, p, s| column_type(dt, len, clen, used, p, s);
        assert_eq!(
            t("VARCHAR2", Some("20"), Some("20"), Some("B"), None, None),
            "VARCHAR2(20)"
        );
        assert_eq!(
            t("VARCHAR2", Some("80"), Some("20"), Some("C"), None, None),
            "VARCHAR2(20 CHAR)"
        );
        assert_eq!(
            t("NVARCHAR2", Some("40"), Some("20"), Some("C"), None, None),
            "NVARCHAR2(20)"
        );
        assert_eq!(t("NUMBER", Some("22"), None, None, None, None), "NUMBER");
        assert_eq!(
            t("NUMBER", Some("22"), None, None, None, Some("0")),
            "INTEGER"
        );
        assert_eq!(
            t("NUMBER", Some("22"), None, None, Some("10"), Some("0")),
            "NUMBER(10)"
        );
        assert_eq!(
            t("NUMBER", Some("22"), None, None, Some("10"), Some("2")),
            "NUMBER(10,2)"
        );
        assert_eq!(
            t("FLOAT", Some("22"), None, None, Some("126"), None),
            "FLOAT(126)"
        );
        assert_eq!(t("RAW", Some("16"), None, None, None, None), "RAW(16)");
        assert_eq!(
            t("TIMESTAMP(6)", Some("11"), None, None, None, Some("6")),
            "TIMESTAMP(6)"
        );
    }
}
