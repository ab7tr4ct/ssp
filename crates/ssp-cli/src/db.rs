use duckdb::Connection;

pub struct DuckDB {
    connection: Connection,
    tables: Vec<String>,
}

impl DuckDB {
    pub fn open() -> Result<Self, anyhow::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDB {
            connection: conn,
            tables: Vec::new(),
        })
    }

    pub fn register_views_tui(&mut self) -> Result<Vec<(String, i64)>, anyhow::Error> {
        let candidates = [
            ("accounts", "accounts_*.parquet"),
            ("mints", "mints_*.parquet"),
            ("token_accounts", "token_accounts_*.parquet"),
        ];

        let mut result = Vec::new();
        for (name, glob) in &candidates {
            if parquet_exists(name) {
                self.connection
                    .execute_batch(&format!("CREATE VIEW {name} AS SELECT * FROM '{glob}'"))?;
                let count: i64 = self
                    .connection
                    .prepare(&format!("SELECT COUNT(*) FROM {name}"))?
                    .query_row([], |row| row.get(0))?;
                self.tables.push(name.to_string());
                result.push((name.to_string(), count));
            }
        }
        Ok(result)
    }

    pub fn execute_to_vecs(
        &self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), anyhow::Error> {
        let mut stmt = self.connection.prepare(sql)?;
        let mut rows = stmt.query([])?;

        let mut data: Vec<Vec<duckdb::types::Value>> = Vec::new();
        while let Some(row) = rows.next()? {
            let mut cols = Vec::new();
            let mut i = 0;
            while let Ok(val) = row.get::<_, duckdb::types::Value>(i) {
                cols.push(val);
                i += 1;
            }
            data.push(cols);
        }
        drop(rows);

        let names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let formatted: Vec<Vec<String>> = data
            .iter()
            .map(|row| row.iter().map(format_value).collect())
            .collect();

        Ok((names, formatted))
    }
}

fn format_value(val: &duckdb::types::Value) -> String {
    match val {
        duckdb::types::Value::Null => "NULL".into(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(n) => n.to_string(),
        duckdb::types::Value::SmallInt(n) => n.to_string(),
        duckdb::types::Value::Int(n) => n.to_string(),
        duckdb::types::Value::BigInt(n) => n.to_string(),
        duckdb::types::Value::HugeInt(n) => n.to_string(),
        duckdb::types::Value::UTinyInt(n) => n.to_string(),
        duckdb::types::Value::USmallInt(n) => n.to_string(),
        duckdb::types::Value::UInt(n) => n.to_string(),
        duckdb::types::Value::UBigInt(n) => n.to_string(),
        duckdb::types::Value::Float(n) => format!("{n:.4}"),
        duckdb::types::Value::Double(n) => format!("{n:.4}"),
        duckdb::types::Value::Text(s) => s.clone(),
        duckdb::types::Value::Blob(bytes) => bs58::encode(bytes).into_string(),
        other => format!("{other:?}"),
    }
}

fn parquet_exists(prefix: &str) -> bool {
    (0..4).any(|i| std::path::Path::new(&format!("{prefix}_{i}.parquet")).exists())
}
