pub use sfo_result::err as sql_err;

#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum SqlErrorCode {
    #[default]
    Failed,
    NotFound,
    AlreadyExists,
}

pub type SqlError = sfo_result::Error<SqlErrorCode>;
pub type SqlResult<T> = sfo_result::Result<T, SqlErrorCode>;
