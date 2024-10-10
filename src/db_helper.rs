use std::marker::PhantomData;
use std::ops::Deref;
use sqlx::{Transaction, Connection, Executor, Database};
use sqlx::pool::PoolConnection;
use sqlx::Execute;
pub use sqlx::Row as SqlRow;
use crate::errors::{sql_err, SqlError, SqlErrorCode};

pub trait ErrorMap: 'static + Clone + Send + Sync {
    type OutError;
    type InError;
    fn map(e: Self::InError, msg: &str) -> Self::OutError;
}

#[macro_export]
macro_rules! sql_query {
    ($query:expr) => ({
        sfo_sql::query!($query)
    });

    ($query:expr, $($args:tt)*) => ({
        sfo_sql::query!($query, $($args)*)
    })
}

pub struct SqlPool<DB: sqlx::Database, EM: ErrorMap<InError = sqlx::Error>>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    pub(crate) pool: sqlx::pool::Pool<DB>,
    pub(crate) uri: String,
    pub(crate) _em: PhantomData<EM>,
}

impl<DB: sqlx::Database, EM: ErrorMap<InError = sqlx::Error>> Clone for SqlPool<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {

    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            uri: self.uri.clone(),
            _em: self._em.clone()
        }
    }
}

impl <DB: sqlx::Database, EM: ErrorMap<InError = sqlx::Error>> Deref for SqlPool<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    type Target = sqlx::pool::Pool<DB>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<DB: sqlx::Database, EM: 'static + ErrorMap<InError = sqlx::Error>> SqlPool<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    pub fn from_raw_pool(pool: sqlx::pool::Pool<DB>) -> Self {
        Self { pool, uri: "".to_string(), _em: Default::default() }
    }

    pub async fn raw_pool(&self) -> sqlx::pool::Pool<DB> {
        self.pool.clone()
    }

    pub async fn get_conn(&self) -> Result<SqlConnection<DB, EM>, EM::OutError> {
        let conn = self.pool.acquire().await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), self.uri.as_str()).as_str()))?;
        Ok(SqlConnection::<DB, EM>::from(conn))
    }
}

pub fn sql_query<DB: Database>(sql: &str) -> sqlx::query::Query<DB, DB::Arguments<'_>>
where for<'b> <DB as sqlx::Database>::Arguments<'b>: sqlx::IntoArguments<'b, DB>,{
    sqlx::query(sql)
}

pub fn sql_query_with<'a, DB: Database>(sql: &'a str, arguments: DB::Arguments<'a>) -> sqlx::query::Query<'a, DB, DB::Arguments<'a>>
where for<'b> <DB as sqlx::Database>::Arguments<'b>: sqlx::IntoArguments<'b, DB>,{
    sqlx::query_with(sql, arguments)
}

pub enum SqlConnectionType<DB: Database>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,{
    PoolConn(PoolConnection<DB>),
    Conn(DB::Connection),
}
pub struct SqlConnection<DB: Database, EM: ErrorMap<InError = sqlx::Error>>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    pub(crate) trans: Option<Transaction<'static, DB>>,
    pub(crate) conn: SqlConnectionType<DB>,
    pub(crate) _em: PhantomData<EM>,
}

impl <DB: Database, EM: 'static + ErrorMap<InError = sqlx::Error>> From<sqlx::pool::PoolConnection<DB>> for SqlConnection<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    fn from(conn: sqlx::pool::PoolConnection<DB>) -> Self {
        Self { conn: SqlConnectionType::PoolConn(conn), _em: Default::default(), trans: None }
    }
}

impl<DB: Database, EM: 'static + ErrorMap<InError = sqlx::Error>> SqlConnection<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
      for<'b> <DB as sqlx::Database>::Arguments<'b>: sqlx::IntoArguments<'b, DB>, {
    pub async fn execute_sql<'a>(&mut self, query: sqlx::query::Query<'a, DB, <DB as Database>::Arguments<'a>>) -> Result<DB::QueryResult, EM::OutError>
    {
        let sql = query.sql();
        match &mut self.conn {
            SqlConnectionType::PoolConn(conn) => {
                conn.execute(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            },
            SqlConnectionType::Conn(conn) => {
                conn.execute(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            }
        }
    }

    pub async fn query_one<'a>(&mut self, query: sqlx::query::Query<'a, DB, DB::Arguments<'a>>) -> Result<DB::Row, EM::OutError> {
        let sql = query.sql();
        match &mut self.conn {
            SqlConnectionType::PoolConn(conn) => {
                conn.fetch_one(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            },
            SqlConnectionType::Conn(conn) => {
                conn.fetch_one(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            }
        }
    }

    pub async fn query_all<'a>(&mut self, query: sqlx::query::Query<'a, DB, DB::Arguments<'a>>) -> Result<Vec<DB::Row>, EM::OutError> {
        let sql = query.sql();
        match &mut self.conn {
            SqlConnectionType::PoolConn(conn) => {
                conn.fetch_all(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            },
            SqlConnectionType::Conn(conn) => {
                conn.fetch_all(query).await.map_err(|e| EM::map(e, format!("[{} {}]", line!(), sql).as_str()))
            }
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
            Ok(())
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

}

impl<DB: sqlx::Database,EM: ErrorMap<InError=sqlx::Error>> Drop for SqlConnection<DB, EM>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>, {
    fn drop(&mut self) {
        if self.trans.is_some() {
            let _ = self.trans.take();
        }
    }
}
