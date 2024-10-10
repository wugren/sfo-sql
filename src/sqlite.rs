use std::str::FromStr;
use std::time::Duration;
use log::LevelFilter;
use sqlx::ConnectOptions;
use crate::errors::{sql_err, SqlError, SqlErrorCode, SqlResult};
pub use crate::db_helper::*;

pub type SqlDB = sqlx::Sqlite;
pub type SqlRawConnection = sqlx::SqliteConnection;
pub type SqlRowObject = <sqlx::Sqlite as sqlx::Database>::Row;
pub type SqlTransaction<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub type SqlQuery<'a> = sqlx::query::Query<'a, sqlx::Sqlite, <sqlx::Sqlite as sqlx::Database>::Arguments<'a>>;
pub type RawSqlPool = sqlx::SqlitePool;
pub type SqlArguments<'a> = <sqlx::Sqlite as sqlx::Database>::Arguments<'a>;
pub type SqliteJournalMode = sqlx::sqlite::SqliteJournalMode;

#[derive(Clone)]
pub struct RawErrorToSqlError;

impl ErrorMap for RawErrorToSqlError {
    type OutError = SqlError;
    type InError = sqlx::Error;

    fn map(e: sqlx::Error, msg: &str) -> SqlError {
        match e {
            sqlx::Error::RowNotFound => {
                // let msg = format!("not found, {}", msg);
                sql_err!(SqlErrorCode::NotFound, "not found")
            },
            sqlx::Error::Database(ref err) => {
                let msg = format!("sql error: {:?} info:{}", e, msg);
                if cfg!(test) {
                    println!("{}", msg);
                } else {
                    log::error!("{}", msg);
                }

                if let Some(code) = err.code() {
                    if code.to_string().as_str() == "1555" {
                        return sql_err!(SqlErrorCode::AlreadyExists, "already exists");
                    }
                }
                sql_err!(SqlErrorCode::Failed, "{}", msg)
            }
            _ => {
                let msg = format!("sql error: {:?} info:{}", e, msg);
                if cfg!(test) {
                    println!("{}", msg);
                } else {
                    log::error!("{}", msg);
                }
                sql_err!(SqlErrorCode::Failed, "")
            }
        }
    }
}

pub type SqlPool = crate::db_helper::SqlPool<sqlx::Sqlite, RawErrorToSqlError>;
pub type SqlConnection = crate::db_helper::SqlConnection<sqlx::Sqlite, RawErrorToSqlError>;

impl SqlPool {

    pub async fn open(uri: &str,
                      max_connections: u32,
                      journal_mode: Option<sqlx::sqlite::SqliteJournalMode>,
    ) -> SqlResult<Self> {
        log::info!("open pool {} max_connections {}", uri, max_connections);
            let pool_options = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(max_connections)
                .acquire_timeout(Duration::from_secs(300))
                .min_connections(0)
                .idle_timeout(Duration::from_secs(300));
            let mut options = sqlx::sqlite::SqliteConnectOptions::from_str(uri).map_err(|e| {
                RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?
                .busy_timeout(Duration::from_secs(300))
                .create_if_missing(true);
            if let Some(journal_mode) = journal_mode {
                options = options.journal_mode(journal_mode);
            }
            #[cfg(target_os = "ios")]
            {
                options = options.serialized(true);
            }

            options = options.log_statements(LevelFilter::Off)
                .log_slow_statements(LevelFilter::Off, Duration::from_secs(10));
            let pool = pool_options.connect_with(options).await.map_err(|e| RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str()))?;
            Ok(Self {
                pool,
                uri: uri.to_string(),
                _em: Default::default(),
            })
    }

}

impl SqlConnection {
    pub async fn open(uri: &str) -> SqlResult<Self> {
        let conn = {
            let mut options = sqlx::sqlite::SqliteConnectOptions::from_str(uri).map_err(|e| RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str()))?
                .busy_timeout(Duration::from_secs(300));
            #[cfg(target_os = "ios")]
            {
                options = options.serialized(true);
            }

            options = options.log_statements(LevelFilter::Off)
                .log_slow_statements(LevelFilter::Off, Duration::from_secs(10));
            options.connect().await.map_err(|e| RawErrorToSqlError::map(e, format!("[{} {}]", line!(), uri).as_str()))?
        };

        Ok(Self {
            conn: SqlConnectionType::Conn(conn),
            _em: Default::default(),
            trans: None
        })
    }
    pub async fn is_column_exist(&mut self, table_name: &str, column_name: &str, _db_name: Option<&str>) -> SqlResult<bool> {
        {
            let sql = r#"select * from sqlite_master where type='table' and tbl_name=?1 and sql like ?2"#;
            let ret = self.query_one(sql_query(sql)
                .bind(table_name).bind(format!("%{}%", column_name))).await;
            if let Err(_) = &ret {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }

    pub async fn is_index_exist(&mut self, table_name: &str, index_name: &str, _db_name: Option<&str>) -> SqlResult<bool> {
        {
            let sql = r#"select * from sqlite_master where type='index' and tbl_name=?1 and name=?2"#;
            let ret = self.query_one(sql_query(sql)
                .bind(table_name).bind(index_name)).await;
            if let Err(_) = &ret {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }
}
