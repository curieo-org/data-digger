use color_eyre::eyre;
use deadpool_postgres::PoolError;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use tokio_postgres::error::Error as DbError;

#[derive(Debug)]
pub enum AppError {
    DeadPoolError(PoolError),
    Postgres(DbError),
    GenericError(color_eyre::eyre::Error),
}

impl From<eyre::Error> for AppError {
    fn from(inner: eyre::Error) -> Self {
        AppError::GenericError(inner)
    }
}

impl From<DbError> for AppError {
    fn from(inner: DbError) -> Self {
        AppError::Postgres(inner)
    }
}

impl From<PoolError> for AppError {
    fn from(inner: PoolError) -> Self {
        AppError::DeadPoolError(inner)
    }
}

impl Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::GenericError(e) => write!(f, "{}", e),
            AppError::Postgres(e) => write!(f, "{}", e),
            AppError::DeadPoolError(e) => write!(f, "{}", e),
        }
    }
}

#[derive(serde::Serialize, Debug)]
pub struct ErrorMap {
    errors: HashMap<Cow<'static, str>, Vec<Cow<'static, str>>>,
}

impl<K, V, I> From<I> for ErrorMap
where
    K: Into<Cow<'static, str>>,
    V: Into<Cow<'static, str>>,
    I: IntoIterator<Item = (K, V)>,
{
    fn from(i: I) -> Self {
        let mut errors = HashMap::new();

        for (key, val) in i {
            errors
                .entry(key.into())
                .or_insert_with(Vec::new)
                .push(val.into());
        }

        ErrorMap { errors }
    }
}
