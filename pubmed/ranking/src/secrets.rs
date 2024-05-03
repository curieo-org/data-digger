use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Debug, Display};

pub struct Secret<T>(T);

impl<T> Clone for Secret<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Secret(self.0.clone())
    }
}

impl<T> Default for Secret<T>
where
    T: Default,
{
    fn default() -> Self {
        Secret(T::default())
    }
}

impl<T> Secret<T> {
    pub fn new(t: T) -> Secret<T> {
        Secret(t)
    }
    pub fn expose(&self) -> &T {
        &self.0
    }
    pub fn expose_owned(self) -> T {
        self.0
    }
}

impl<T> Display for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[redacted]")
    }
}

impl<T> Debug for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[redacted]")
    }
}

impl<'de, T> Deserialize<'de> for Secret<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Secret)
    }
}

impl<T> Serialize for Secret<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<T> From<T> for Secret<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}
