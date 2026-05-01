use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use serde::Serializer;

#[allow(clippy::ref_option)]
pub fn serialize_optional_timestamp<S>(
    value: &Option<Timestamp>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(timestamp) => serialize_timestamp(timestamp, serializer),
        None => serializer.serialize_none(),
    }
}

fn serialize_timestamp<S>(timestamp: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let nanos = u32::try_from(timestamp.nanos)
        .map_err(|_| serde::ser::Error::custom("invalid protobuf timestamp"))?;
    let datetime = DateTime::<Utc>::from_timestamp(timestamp.seconds, nanos)
        .ok_or_else(|| serde::ser::Error::custom("invalid protobuf timestamp"))?;
    serializer.serialize_str(&datetime.to_rfc3339())
}
