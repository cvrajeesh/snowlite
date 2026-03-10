use crate::{Error, Result, Value};
use std::collections::HashMap;

/// A single row returned from a query.
///
/// Values can be accessed either by zero-based column index or by column name.
///
/// ```rust,no_run
/// # use snowlite::{Connection, Row};
/// # fn main() -> snowlite::Result<()> {
/// # let conn = Connection::open_in_memory()?;
/// # conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])?;
/// let rows = conn.query("SELECT id, name FROM t", &[])?;
/// for row in rows {
///     let id: i64 = row.get(0)?;          // by index
///     let name: String = row.get_by_name("name")?;  // by name
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
    index: HashMap<String, usize>,
}

impl Row {
    /// Construct a new `Row` from column names and values.
    pub(crate) fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.to_ascii_lowercase(), i))
            .collect();
        Row {
            columns,
            values,
            index,
        }
    }

    /// Returns the number of columns in this row.
    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    /// Returns the column names for this row.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the raw [`Value`] at the given column index.
    pub fn value(&self, idx: usize) -> Result<&Value> {
        self.values.get(idx).ok_or(Error::ColumnIndexOutOfRange {
            index: idx,
            count: self.values.len(),
        })
    }

    /// Returns the raw [`Value`] for the given column name (case-insensitive).
    pub fn value_by_name(&self, name: &str) -> Result<&Value> {
        let idx = self
            .index
            .get(&name.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| Error::ColumnNotFound {
                name: name.to_owned(),
            })?;
        Ok(&self.values[idx])
    }

    /// Get a typed value at the given column index.
    pub fn get<T: FromValue>(&self, idx: usize) -> Result<T> {
        T::from_value(self.value(idx)?)
    }

    /// Get a typed value by column name (case-insensitive).
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T> {
        T::from_value(self.value_by_name(name)?)
    }
}

// ── FromValue trait ──────────────────────────────────────────────────────────

/// Trait for converting a [`Value`] into a concrete Rust type.
pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self>;
}

impl FromValue for Value {
    fn from_value(v: &Value) -> Result<Self> {
        Ok(v.clone())
    }
}

impl FromValue for i64 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Integer(i) => Ok(*i),
            Value::Real(r) => {
                if r.is_nan() || r.is_infinite() {
                    Err(Error::TypeConversion {
                        expected: "i64",
                        actual: format!("REAL({r}) is not representable as i64"),
                    })
                } else {
                    Ok(*r as i64)
                }
            }
            Value::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            Value::Text(s) => s.parse().map_err(|_| Error::TypeConversion {
                expected: "i64",
                actual: format!("TEXT('{s}')"),
            }),
            other => Err(Error::TypeConversion {
                expected: "i64",
                actual: other.type_name().to_owned(),
            }),
        }
    }
}

impl FromValue for i32 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            i32::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "i32",
                actual: format!("INTEGER({i}) is out of range for i32"),
            })
        })
    }
}

impl FromValue for u64 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            u64::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "u64",
                actual: format!("INTEGER({i}) is negative, cannot convert to u64"),
            })
        })
    }
}

impl FromValue for f64 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Real(r) => Ok(*r),
            Value::Integer(i) => Ok(*i as f64),
            Value::Text(s) => s.parse().map_err(|_| Error::TypeConversion {
                expected: "f64",
                actual: format!("TEXT('{s}')"),
            }),
            other => Err(Error::TypeConversion {
                expected: "f64",
                actual: other.type_name().to_owned(),
            }),
        }
    }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Boolean(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Text(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "yes" | "1" => Ok(true),
                "false" | "no" | "0" => Ok(false),
                _ => Err(Error::TypeConversion {
                    expected: "bool",
                    actual: format!("TEXT('{s}')"),
                }),
            },
            other => Err(Error::TypeConversion {
                expected: "bool",
                actual: other.type_name().to_owned(),
            }),
        }
    }
}

impl FromValue for String {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Text(s) => Ok(s.clone()),
            Value::Integer(i) => Ok(i.to_string()),
            Value::Real(r) => Ok(r.to_string()),
            Value::Boolean(b) => Ok(b.to_string()),
            Value::Null => Ok(String::new()),
            Value::Blob(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Blob(b) => Ok(b.clone()),
            Value::Text(s) => Ok(s.as_bytes().to_vec()),
            other => Err(Error::TypeConversion {
                expected: "Vec<u8>",
                actual: other.type_name().to_owned(),
            }),
        }
    }
}

impl FromValue for i16 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            i16::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "i16",
                actual: format!("INTEGER({i}) is out of range for i16"),
            })
        })
    }
}

impl FromValue for u32 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            u32::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "u32",
                actual: format!("INTEGER({i}) is out of range for u32"),
            })
        })
    }
}

impl FromValue for i8 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            i8::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "i8",
                actual: format!("INTEGER({i}) is out of range for i8"),
            })
        })
    }
}

impl FromValue for u8 {
    fn from_value(v: &Value) -> Result<Self> {
        i64::from_value(v).and_then(|i| {
            u8::try_from(i).map_err(|_| Error::TypeConversion {
                expected: "u8",
                actual: format!("INTEGER({i}) is out of range for u8"),
            })
        })
    }
}

impl FromValue for serde_json::Value {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Text(s) => serde_json::from_str(s).map_err(|_| Error::TypeConversion {
                expected: "serde_json::Value",
                actual: format!("TEXT is not valid JSON: '{s}'"),
            }),
            Value::Null => Ok(serde_json::Value::Null),
            Value::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
            Value::Real(r) => serde_json::Number::from_f64(*r)
                .map(serde_json::Value::Number)
                .ok_or_else(|| Error::TypeConversion {
                    expected: "serde_json::Value",
                    actual: format!("REAL({r}) is not representable as a JSON number"),
                }),
            Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
            Value::Blob(b) => Err(Error::TypeConversion {
                expected: "serde_json::Value",
                actual: format!("BLOB ({} bytes) cannot be converted to JSON", b.len()),
            }),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Null => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }
}
