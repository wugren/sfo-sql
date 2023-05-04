use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;
use sqlx::{Transaction, Connection, Executor, ConnectOptions};
use log::LevelFilter;
use sqlx::pool::PoolConnection;
use sqlx::Execute;
use sqlx::mysql::MySqlSslMode;
pub use sqlx::Row as SqlRow;
pub use sqlx::Arguments as TSqlArguments;

pub trait ErrorMap: 'static + Clone + Send + Sync {
    type OutError;
    type InError;
    fn map(e: Self::InError, msg: &str) -> Self::OutError;
}

#[cfg(feature = "sqlite")]
pub type SqlDB = sqlx::Sqlite;
#[cfg(feature = "sqlite")]
pub type SqlRawConnection = sqlx::SqliteConnection;
#[cfg(feature = "sqlite")]
pub type SqlResult = <sqlx::Sqlite as sqlx::Database>::QueryResult;
#[cfg(feature = "sqlite")]
pub type SqlRowObject = <sqlx::Sqlite as sqlx::Database>::Row;
#[cfg(feature = "sqlite")]
pub type SqlTransaction<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
#[cfg(feature = "sqlite")]
pub type SqlQuery<'a> = sqlx::query::Query<'a, sqlx::Sqlite, <sqlx::Sqlite as sqlx::database::HasArguments<'a>>::Arguments>;
#[cfg(feature = "sqlite")]
pub type RawSqlPool = sqlx::Sqlite;
#[cfg(feature = "sqlite")]
pub type SqlArguments<'a> = <sqlx::Sqlite as sqlx::database::HasArguments<'a>>::Arguments;
pub type SqlError = sqlx::Error;


#[cfg(feature = "mysql")]
pub type SqlDB = sqlx::MySql;
#[cfg(feature = "mysql")]
pub type SqlRawConnection = sqlx::MySqlConnection;
#[cfg(feature = "mysql")]
pub type SqlResult = <sqlx::MySql as sqlx::Database>::QueryResult;
#[cfg(feature = "mysql")]
pub type SqlRowObject = <sqlx::MySql as sqlx::Database>::Row;
#[cfg(feature = "mysql")]
pub type SqlTransaction<'a> = sqlx::Transaction<'a, sqlx::MySql>;
#[cfg(feature = "mysql")]
pub type SqlQuery<'a> = sqlx::query::Query<'a, sqlx::MySql, <sqlx::MySql as sqlx::database::HasArguments<'a>>::Arguments>;
#[cfg(feature = "mysql")]
pub type RawSqlPool = sqlx::MySqlPool;
#[cfg(feature = "mysql")]
pub type SqlArguments<'a> = <sqlx::MySql as sqlx::database::HasArguments<'a>>::Arguments;

#[macro_export]
macro_rules! sql_query {
    ($query:expr) => ({
        sqlx::query!($query)
    });

    ($query:expr, $($args:tt)*) => ({
        sqlx::query!($query, $($args)*)
    })
}

#[derive(Clone)]
pub struct SqlPool<EM: ErrorMap<InError = sqlx::Error>> {
    pool: RawSqlPool,
    uri: String,
    _em: PhantomData<EM>,
}

impl <EM: ErrorMap<InError = sqlx::Error>> Deref for SqlPool<EM> {
    type Target = RawSqlPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<EM: 'static + ErrorMap<InError = sqlx::Error>> SqlPool<EM> {
    pub fn from_raw_pool(pool: RawSqlPool) -> Self {
        Self { pool, uri: "".to_string(), _em: Default::default() }
    }

    pub async fn open(uri: &str, max_connections: u32) -> Result<Self, EM::OutError> {
        log::info!("open pool {} max_connections {}", uri, max_connections);
        #[cfg(feature = "mysql")]
        {
            let pool_options = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(max_connections)
                .connect_timeout(Duration::from_secs(300))
                .min_connections(0)
                .idle_timeout(Duration::from_secs(300));
            let mut options = sqlx::mysql::MySqlConnectOptions::from_str(uri).map_err(|e| {
                EM::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?;
            options.log_slow_statements(LevelFilter::Error, Duration::from_secs(1));
            options.log_statements(LevelFilter::Off);
            options = options.ssl_mode(MySqlSslMode::Disabled);
            let pool = pool_options.connect_with(options).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), uri).as_str()))?;
            Ok(Self {
                pool,
                uri: uri.to_string(),
                _em: Default::default()
            })
        }
        #[cfg(feature = "sqlite")]
        {
            let pool_options = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(max_connections)
                .connect_timeout(Duration::from_secs(300))
                .min_connections(0)
                .idle_timeout(Duration::from_secs(300));
            let mut options = sqlx::sqlite::SqliteConnectOptions::from_str(uri).map_err(|e| {
                EM::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?
                .busy_timeout(Duration::from_secs(300))
                .create_if_missing(true);
            #[cfg(target_os = "ios")]
            {
                options = options.serialized(true);
            }

            options.log_statements(LevelFilter::Off)
                .log_slow_statements(LevelFilter::Off, Duration::from_secs(10));
            let pool = pool_options.connect_with(options).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), uri).as_str()))?;
            Ok(Self {
                pool,
                uri: uri.to_string(),
                _em: Default::default(),
            })
        }
    }

    pub async fn raw_pool(&self) -> RawSqlPool {
        self.pool.clone()
    }

    pub async fn get_conn(&self) -> Result<SqlConnection<EM>, EM::OutError> {
        let conn = self.pool.acquire().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), self.uri.as_str()).as_str()))?;
        Ok(SqlConnection::<EM>::from(conn))
    }
}

pub fn sql_query(sql: &str) -> SqlQuery<'_> {
    sqlx::query::<SqlDB>(sql)
}

pub fn sql_query_with<'a>(sql: &'a str, arguments: SqlArguments<'a>) -> SqlQuery<'a> {
    sqlx::query_with(sql, arguments)
}

pub enum SqlConnectionType {
    PoolConn(PoolConnection<SqlDB>),
    Conn(SqlRawConnection),
}
pub struct SqlConnection<EM: ErrorMap<InError = sqlx::Error>> {
    conn: SqlConnectionType,
    trans: Option<Transaction<'static, SqlDB>>,
    _em: PhantomData<EM>,
}

impl <EM: 'static + ErrorMap<InError = SqlError>> From<sqlx::pool::PoolConnection<SqlDB>> for SqlConnection<EM> {
    fn from(conn: sqlx::pool::PoolConnection<SqlDB>) -> Self {
        Self { conn: SqlConnectionType::PoolConn(conn), _em: Default::default(), trans: None }
    }
}

impl<EM: 'static + ErrorMap<InError = sqlx::Error>> SqlConnection<EM> {
    pub async fn open(uri: &str) -> Result<Self, EM::OutError> {
        #[cfg(feature = "sqlite")]
        let conn = {
            let mut options = sqlx::sqlite::SqliteConnectOptions::from_str(uri).map_err(|e| EM::map(e, format!("[{} {}]", line!(), uri).as_str()))?
                .busy_timeout(Duration::from_secs(300));
            #[cfg(target_os = "ios")]
            {
                options = options.serialized(true);
            }

            options.log_statements(LevelFilter::Off)
                .log_slow_statements(LevelFilter::Off, Duration::from_secs(10));
            options.connect().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), uri).as_str()))?
        };
        #[cfg(feature = "mysql")]
        let conn = {
            let mut options = sqlx::mysql::MySqlConnectOptions::from_str(uri).map_err(|e| {
                EM::map(e, format!("[{} {}]", line!(), uri).as_str())
            })?;
            options = options.ssl_mode(MySqlSslMode::Disabled);
            options.connect().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), uri).as_str()))?
        };

        Ok(Self {
            conn: SqlConnectionType::Conn(conn),
            _em: Default::default(),
            trans: None
        })
    }

    pub async fn execute_sql(&mut self, query: SqlQuery<'_>) -> Result<SqlResult, EM::OutError> {
        let sql = query.sql();
        if self.trans.is_none() {
            match &mut self.conn {
                SqlConnectionType::PoolConn(conn) => {
                    conn.execute(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                },
                SqlConnectionType::Conn(conn) => {
                    conn.execute(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                }
            }
        } else {
            self.trans.as_mut().unwrap().execute(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
        }
    }

    pub async fn query_one(&mut self, query: SqlQuery<'_>) -> Result<SqlRowObject, EM::OutError> {
        let sql = query.sql();
        if self.trans.is_none() {
            match &mut self.conn {
                SqlConnectionType::PoolConn(conn) => {
                    conn.fetch_one(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                },
                SqlConnectionType::Conn(conn) => {
                    conn.fetch_one(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                }
            }
        } else {
            self.trans.as_mut().unwrap().fetch_one(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
        }
    }

    pub async fn query_all(&mut self, query: SqlQuery<'_>) -> Result<Vec<SqlRowObject>, EM::OutError> {
        let sql = query.sql();
        if self.trans.is_none() {
            match &mut self.conn {
                SqlConnectionType::PoolConn(conn) => {
                    conn.fetch_all(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                },
                SqlConnectionType::Conn(conn) => {
                    conn.fetch_all(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
                }
            }
        } else {
            self.trans.as_mut().unwrap().fetch_all(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
        }
    }

    pub async fn begin_transaction(&mut self) -> Result<(), EM::OutError> {
        let this: &'static mut Self = unsafe {std::mem::transmute(self)};
        let trans = match &mut this.conn {
            SqlConnectionType::PoolConn(conn) => {
                conn.begin().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), "begin trans").as_str()))
            },
            SqlConnectionType::Conn(conn) => {
                conn.begin().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), "begin trans").as_str()))
            }
        }?;
        this.trans = Some(trans);
        Ok(())
    }

    pub async fn rollback_transaction(&mut self) -> Result<(), EM::OutError> {
        if self.trans.is_none() {
            return Ok(())
        } else {
            self.trans.take().unwrap().rollback().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), "rollback trans").as_str()))
        }
    }

    pub async fn commit_transaction(&mut self) -> Result<(), EM::OutError> {
        if self.trans.is_none() {
            return Ok(())
        } else {
            self.trans.take().unwrap().commit().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), "commit trans").as_str()))
        }
    }

    #[cfg(feature = "mysql")]
    pub async fn is_column_exist(&mut self, table_name: &str, column_name: &str, db_name: Option<&str>) -> Result<bool, EM::OutError> {
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

    #[cfg(feature = "mysql")]
    pub async fn is_index_exist(&mut self, table_name: &str, index_name: &str, db_name: Option<&str>) -> Result<bool, EM::OutError> {
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

impl<EM: ErrorMap<InError=sqlx::Error>> Drop for SqlConnection<EM> {
    fn drop(&mut self) {
        if self.trans.is_some() {
            let trans = self.trans.take().unwrap();
            async_std::task::block_on(async move {
                let _ = trans.rollback().await;
            });
        }
    }
}
