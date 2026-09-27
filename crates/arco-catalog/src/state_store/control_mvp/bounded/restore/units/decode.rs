//! Decode-only fixed field sets; the parent enums retain their wire serializers.
use serde::{Deserialize, Deserializer, de};

use super::{
    DirectoryPosition, GlobalCut, SideCursor, SingletonPhase, SingletonState,
    SingletonValueWitness, UnitReservationV1,
};

#[derive(Default)]
enum Seen<T> {
    #[default]
    Missing,
    Present(T),
}
use Seen::{Missing, Present};

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Seen<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Present)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CutKind {
    Start,
    After,
    End,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GlobalCutWire {
    kind: CutKind,
    #[serde(default)]
    key_b64url: Seen<String>,
}

impl TryFrom<GlobalCutWire> for GlobalCut {
    type Error = &'static str;

    fn try_from(wire: GlobalCutWire) -> Result<Self, Self::Error> {
        match (wire.kind, wire.key_b64url) {
            (CutKind::Start, Missing) => Ok(Self::Start),
            (CutKind::End, Missing) => Ok(Self::End),
            (CutKind::After, Present(key_b64url)) => Ok(Self::After { key_b64url }),
            _ => Err("global cut fields differ from its kind"),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SideCursorWire {
    kind: CutKind,
    #[serde(default)]
    key_b64url: Seen<String>,
    #[serde(default)]
    position: Seen<DirectoryPosition>,
}

impl TryFrom<SideCursorWire> for SideCursor {
    type Error = &'static str;

    fn try_from(wire: SideCursorWire) -> Result<Self, Self::Error> {
        match (wire.kind, wire.key_b64url, wire.position) {
            (CutKind::Start, Missing, Missing) => Ok(Self::Start),
            (CutKind::End, Missing, Missing) => Ok(Self::End),
            (CutKind::After, Present(key_b64url), Present(position)) => Ok(Self::After {
                key_b64url,
                position,
            }),
            _ => Err("side cursor fields differ from its kind"),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SingletonKind {
    None,
    Pending,
    Complete,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SingletonStateWire {
    kind: SingletonKind,
    #[serde(default)]
    key_b64url: Seen<String>,
    #[serde(default)]
    source: Seen<Option<Box<SingletonValueWitness>>>,
    #[serde(default)]
    current: Seen<Option<Box<SingletonValueWitness>>>,
    #[serde(default)]
    phase: Seen<SingletonPhase>,
}

impl TryFrom<SingletonStateWire> for SingletonState {
    type Error = &'static str;

    fn try_from(wire: SingletonStateWire) -> Result<Self, Self::Error> {
        match (
            wire.kind,
            wire.key_b64url,
            wire.source,
            wire.current,
            wire.phase,
        ) {
            (SingletonKind::None, Missing, Missing, Missing, Missing) => Ok(Self::None),
            (SingletonKind::Complete, Present(key_b64url), Missing, Missing, Missing) => {
                Ok(Self::Complete { key_b64url })
            }
            (
                SingletonKind::Pending,
                Present(key_b64url),
                Present(source),
                Present(current),
                Present(phase),
            ) => Ok(Self::Pending {
                key_b64url,
                source,
                current,
                phase,
            }),
            _ => Err("singleton state fields differ from its kind"),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReservationKind {
    Standard,
    Singleton,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnitReservationWire {
    kind: ReservationKind,
    #[serde(default)]
    combined_leaf_limit: Seen<u64>,
    #[serde(default)]
    decode_limit: Seen<u64>,
    #[serde(default)]
    input_byte_limit: Seen<u64>,
    #[serde(default)]
    output_block_limit: Seen<u64>,
    #[serde(default)]
    packed_output_byte_limit: Seen<u64>,
    #[serde(default)]
    phase: Seen<SingletonPhase>,
    #[serde(default)]
    authenticated_payload_bytes: Seen<u64>,
    #[serde(default)]
    segment_byte_limit: Seen<u64>,
    #[serde(default)]
    scratch_base_bytes: Seen<u64>,
    #[serde(default)]
    scratch_payload_multiplier: Seen<u64>,
}

impl TryFrom<UnitReservationWire> for UnitReservationV1 {
    type Error = &'static str;

    fn try_from(wire: UnitReservationWire) -> Result<Self, Self::Error> {
        match wire {
            UnitReservationWire {
                kind: ReservationKind::Standard,
                combined_leaf_limit: Present(combined_leaf_limit),
                decode_limit: Present(decode_limit),
                input_byte_limit: Present(input_byte_limit),
                output_block_limit: Present(output_block_limit),
                packed_output_byte_limit: Present(packed_output_byte_limit),
                phase: Missing,
                authenticated_payload_bytes: Missing,
                segment_byte_limit: Missing,
                scratch_base_bytes: Missing,
                scratch_payload_multiplier: Missing,
            } => Ok(Self::Standard {
                combined_leaf_limit,
                decode_limit,
                input_byte_limit,
                output_block_limit,
                packed_output_byte_limit,
            }),
            UnitReservationWire {
                kind: ReservationKind::Singleton,
                combined_leaf_limit: Missing,
                decode_limit: Missing,
                input_byte_limit: Present(input_byte_limit),
                output_block_limit: Missing,
                packed_output_byte_limit: Missing,
                phase: Present(phase),
                authenticated_payload_bytes: Present(authenticated_payload_bytes),
                segment_byte_limit: Present(segment_byte_limit),
                scratch_base_bytes: Present(scratch_base_bytes),
                scratch_payload_multiplier: Present(scratch_payload_multiplier),
            } => Ok(Self::Singleton {
                phase,
                authenticated_payload_bytes,
                input_byte_limit,
                segment_byte_limit,
                scratch_base_bytes,
                scratch_payload_multiplier,
            }),
            _ => Err("unit reservation fields differ from its kind"),
        }
    }
}

const LIST_LIMIT: &str = "restore record list exceeds its declared bound";

struct RejectExtra;

impl<'de> Deserialize<'de> for RejectExtra {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom(LIST_LIMIT))
    }
}

pub(super) fn list<'de, D, T, const MAX: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedList<T, const MAX: usize>(std::marker::PhantomData<T>);

    impl<'de, T: Deserialize<'de>, const MAX: usize> de::Visitor<'de> for BoundedList<T, MAX> {
        type Value = Vec<T>;

        fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            out.write_str("a bounded restore witness list")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::with_capacity(MAX);
            while values.len() < MAX {
                match seq.next_element()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }
            // End-of-array returns None. Any extra value invokes the rejecting
            // marker before its bytes can become a typed witness or JSON tree.
            let _ = seq.next_element::<RejectExtra>()?;
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedList::<T, MAX>(std::marker::PhantomData))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};

    fn check_wire<T: DeserializeOwned + Serialize>(valid: Value, universe: &Value) {
        let fields = valid.as_object().expect("object");
        let mut ordered = vec![format!("\"kind\":{}", fields["kind"])];
        ordered.extend(
            fields
                .iter()
                .filter(|(key, _)| *key != "kind")
                .map(|(key, value)| format!("\"{key}\":{value}")),
        );
        let canonical = serde_jcs::to_vec(&valid).expect("canonical literal");
        for _ in 0..2 {
            let raw = format!("{{{}}}", ordered.join(","));
            let parsed: T = serde_json::from_slice(raw.as_bytes()).expect("tag order");
            assert_eq!(
                serde_jcs::to_vec(&parsed).expect("canonical typed"),
                canonical
            );
            ordered.reverse();
        }
        for (key, value) in fields {
            let mut missing = valid.clone();
            missing.as_object_mut().expect("object").remove(key);
            assert!(
                serde_json::from_value::<T>(missing).is_err(),
                "missing {key}"
            );
            let raw = format!(
                "{},\"{key}\":{value}}}",
                std::str::from_utf8(&canonical[..canonical.len() - 1]).expect("UTF8")
            );
            assert!(
                serde_json::from_slice::<T>(raw.as_bytes()).is_err(),
                "duplicate {key}"
            );
        }
        for (key, value) in universe.as_object().expect("universe") {
            if !fields.contains_key(key) {
                let mut extra = valid.clone();
                extra[key] = value.clone();
                assert!(serde_json::from_value::<T>(extra).is_err(), "extra {key}");
            }
        }
        let mut unknown_kind = valid;
        unknown_kind["kind"] = json!("unknown");
        assert!(serde_json::from_value::<T>(unknown_kind).is_err());
    }

    #[test]
    fn bounded_unit_decode_preserves_variant_fields_nulls_tag_order_and_jcs() {
        let position = json!({
            "role":"kv", "root_b64":"AA", "path":[],
            "leaf":{"first_b64url":"YQ", "last_b64url":"YQ", "rows":1, "bytes":1, "digest":"00"}
        });
        let cut_fields = json!({"key_b64url":"YQ"});
        let side_fields = json!({"key_b64url":"YQ", "position":position});
        for kind in ["start", "end", "after"] {
            let mut cut = json!({"kind":kind});
            let mut side = cut.clone();
            if kind == "after" {
                cut["key_b64url"] = json!("YQ");
                side["key_b64url"] = json!("YQ");
                side["position"] = position.clone();
            }
            check_wire::<GlobalCut>(cut, &cut_fields);
            check_wire::<SideCursor>(side, &side_fields);
        }
        let singleton_fields =
            json!({"key_b64url":"YQ", "source":null, "current":null, "phase":"emit"});
        check_wire::<SingletonState>(json!({"kind":"none"}), &singleton_fields);
        check_wire::<SingletonState>(
            json!({"kind":"complete", "key_b64url":"YQ"}),
            &singleton_fields,
        );
        for phase in ["compare_source", "compare_current", "emit"] {
            let mut pending = singleton_fields.clone();
            pending["kind"] = json!("pending");
            pending["phase"] = json!(phase);
            check_wire::<SingletonState>(pending, &singleton_fields);
        }
        let object = json!({"path":"x", "byte_size":1, "sha256":"00"});
        let witness = json!({
            "generation":1, "tombstone":false, "value_length":1, "value_sha256":"00",
            "descriptor":object, "index":object,
            "block":{"offset":0,"length":1,"sha256":"00","rows":1,"min_key_b64url":"YQ","max_key_b64url":"YQ"},
            "row_ordinal":0
        });
        for source in [Value::Null, witness.clone()] {
            for current in [Value::Null, witness.clone()] {
                check_wire::<SingletonState>(
                    json!({"kind":"pending", "key_b64url":"YQ", "source":source, "current":current, "phase":"emit"}),
                    &singleton_fields,
                );
            }
        }
        let standard = json!({"kind":"standard", "combined_leaf_limit":16, "decode_limit":64, "input_byte_limit":1, "output_block_limit":32, "packed_output_byte_limit":1});
        let singleton = json!({"kind":"singleton", "phase":"emit", "authenticated_payload_bytes":1, "input_byte_limit":1, "segment_byte_limit":1, "scratch_base_bytes":1, "scratch_payload_multiplier":24});
        let mut all = standard.clone();
        all.as_object_mut()
            .expect("fields")
            .extend(singleton.as_object().expect("fields").clone());
        check_wire::<UnitReservationV1>(standard, &all);
        check_wire::<UnitReservationV1>(singleton, &all);
    }

    #[test]
    fn bounded_unit_decode_known_nonnullable_fields_reject_null() {
        for raw in [
            br#"{"kind":"start","key_b64url":null}"#.as_slice(),
            br#"{"kind":"after","key_b64url":null}"#.as_slice(),
        ] {
            assert!(serde_json::from_slice::<GlobalCut>(raw).is_err());
            assert!(serde_json::from_slice::<SideCursor>(raw).is_err());
        }
        for raw in [
            br#"{"kind":"none","source":null}"#.as_slice(),
            br#"{"kind":"complete","key_b64url":"YQ","source":null}"#.as_slice(),
        ] {
            assert!(serde_json::from_slice::<SingletonState>(raw).is_err());
        }
    }

    #[test]
    fn bounded_unit_decode_duplicate_expensive_value_is_not_parsed() {
        let raw = format!(
            "{{\"source\":null,\"source\":[{}0],\"kind\":\"pending\"}}",
            "0,".repeat(10_000)
        );
        let observed = allocation_counter::measure(|| {
            assert!(serde_json::from_slice::<SingletonState>(raw.as_bytes()).is_err());
        });
        assert!(
            observed.bytes_total < 64 * 1024,
            "duplicate value allocated {}",
            observed.bytes_total
        );
    }
}
