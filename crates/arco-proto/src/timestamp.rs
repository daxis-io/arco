use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::Visitor;

/// Protobuf `google.protobuf.Timestamp` with protobuf-JSON serde and stable `Eq`/`Hash`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

impl TryFrom<Timestamp> for DateTime<Utc> {
    type Error = &'static str;

    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        Self::from_timestamp(
            value.seconds,
            value
                .nanos
                .try_into()
                .map_err(|_| "out of range integral type conversion attempted")?,
        )
        .ok_or("invalid or out-of-range datetime")
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self {
            seconds: value.timestamp(),
            nanos: value.timestamp_subsec_nanos() as i32,
        }
    }
}

impl From<prost_types::Timestamp> for Timestamp {
    fn from(value: prost_types::Timestamp) -> Self {
        Self {
            seconds: value.seconds,
            nanos: value.nanos,
        }
    }
}

impl From<Timestamp> for prost_types::Timestamp {
    fn from(value: Timestamp) -> Self {
        Self {
            seconds: value.seconds,
            nanos: value.nanos,
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let timestamp: DateTime<Utc> = (*self).try_into().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(timestamp.to_rfc3339().as_str())
    }
}

struct TimestampVisitor;

impl<'de> Visitor<'de> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an RFC 3339 timestamp string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let parsed = DateTime::parse_from_rfc3339(value).map_err(serde::de::Error::custom)?;
        Ok(parsed.with_timezone(&Utc).into())
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TimestampVisitor)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Timestamp;

    #[test]
    fn timestamp_json_uses_rfc3339_strings() {
        let timestamp = Timestamp {
            seconds: 1_742_770_800,
            nanos: 0,
        };

        assert_eq!(
            serde_json::to_string(&timestamp).expect("timestamp should serialize"),
            "\"2025-03-23T23:00:00+00:00\""
        );

        let decoded: Timestamp =
            serde_json::from_str("\"2025-03-23T23:00:00Z\"").expect("timestamp should parse");
        assert_eq!(decoded, timestamp);
    }

    #[test]
    fn timestamp_supports_hash_semantics() {
        let timestamp = Timestamp {
            seconds: 1_742_770_800,
            nanos: 0,
        };

        let mut seen = HashSet::new();
        seen.insert(timestamp);

        assert!(seen.contains(&timestamp));
    }
}
