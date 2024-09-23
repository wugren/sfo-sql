use std::str::FromStr;
use std::time::Duration;
use log::LevelFilter;
use sqlx::ConnectOptions;
use sqlx::mysql::MySqlSslMode;
use crate::errors::SqlResult;
pub use crate::db_helper::*;

pub type SqlDB = sqlx::MySql;
pub type SqlRawConnection = sqlx::MySqlConnection;
pub type SqlRowObject = <sqlx::MySql as sqlx::Database>::Row;
pub type SqlTransaction<'a> = sqlx::Transaction<'a, sqlx::MySql>;
pub type SqlQuery<'a> = sqlx::query::Query<'a, sqlx::MySql, <sqlx::MySql as sqlx::Database>::Arguments<'a>>;
pub type RawSqlPool = sqlx::MySqlPool;
pub type SqlArguments<'a> = <sqlx::MySql as sqlx::Database>::Arguments<'a>;

pub type SqlPool = crate::db_helper::SqlPool<sqlx::MySql, RawErrorToSqlError>;
pub type SqlConnection = crate::db_helper::SqlConnection<sqlx::MySql, RawErrorToSqlError>;

impl SqlPool {

    pub async fn open(uri: &str,
                      max_connections: u32,
    ) -> SqlResult<Self> {
        log::info!("open pool {} max_connections {}", uri, max_connections);
        #[cfg(feature = "mysql")]
        {
            let pool_options = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(max_connections)
                .acquire_timeout(Duration::from_secs(300))
                .min_connections(0)
                .idle_timeout(Duration::from_secs(300));
            let mut options = sqlx::mysql::MySqlConnectOptions::from_str(uri).map_err(|e| {
                RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?;
            options = options.log_slow_statements(LevelFilter::Error, Duration::from_secs(1));
            options = options.log_statements(LevelFilter::Off);
            options = options.ssl_mode(MySqlSslMode::Disabled);
            let pool = pool_options.connect_with(options).await.map_err(|e| RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str()))?;
            Ok(Self {
                pool,
                uri: uri.to_string(),
                _em: Default::default()
            })
        }
    }

}

impl SqlConnection {
    pub async fn open(uri: &str) -> SqlResult<Self> {
        let conn = {
            let mut options = sqlx::mysql::MySqlConnectOptions::from_str(uri).map_err(|e| {
                RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?;
            options = options.ssl_mode(MySqlSslMode::Disabled);
            options.connect().await.map_err(|e| RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str()))?
        };

        Ok(Self {
            conn: SqlConnectionType::Conn(conn),
            _em: Default::default(),
            trans: None
        })
    }
    pub async fn is_column_exist(&mut self, table_name: &str, column_name: &str, db_name: Option<&str>) -> SqlResult<bool> {
        {
            let row = if db_name.is_none() {
                let sql = "select count(*) as c from information_schema.columns where table_schema = database() and table_name = ? and column_name = ?";
                let row = self.query_one(sql_query(sql).bind(table_name).bind(column_name)).await?;
                row
            } else {
                let sql = "select count(*) as c from information_schema.columns where table_schema = ? and table_name = ? and column_name = ?";
                let row = self.query_one(sql_query(sql).bind(db_name.unwrap()).bind(table_name).bind(column_name)).await?;
                row
            };
            let count: i32 = row.get("c");
            if count == 0 {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }

    pub async fn is_index_exist(&mut self, table_name: &str, index_name: &str, db_name: Option<&str>) -> SqlResult<bool> {
        {
            let row = if db_name.is_none() {
                let sql = "select count(*) as c from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ?";
                let row = self.query_one(sql_query(sql).bind(table_name).bind(index_name)).await?;
                row
            } else {
                let sql = "select count(*) as c from information_schema.statistics where table_schema = ? and table_name = ? and index_name = ?";
                let row = self.query_one(sql_query(sql).bind(db_name.unwrap()).bind(table_name).bind(index_name)).await?;
                row
            };
            let count: i32 = row.get("c");
            if count == 0 {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }
}
