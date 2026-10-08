//! Forged restore notices shared by the kernel and catalog restore-notice
//! contract suites: notice-shaped outbox records committed by plain control
//! transactions, never by a restore.
use arco_catalog::{
    ArcoStateAdmin as _, ControlMvpProjectionOutboxRecord, ControlMvpStateStore, TxnOptions,
};
use bytes::Bytes;

/// Renders a forged outbox payload for the sequence it commits at.
pub type ForgedPayload = Box<dyn FnOnce(u64) -> Vec<u8>>;

/// The canonical restore notice payload shape: the field set and order the
/// kernel writes.
#[derive(serde::Serialize)]
struct ForgedRestoreNotice<'a> {
    restore_id: &'a str,
    participant_attempt: u64,
    domain: &'a str,
    source_logical_sequence: u64,
    result_logical_sequence: u64,
}

/// The canonical bytes of a forged restore notice of attempt 1.
pub fn forged_notice(restore_id: &str, domain: &str, source: u64, result: u64) -> Vec<u8> {
    serde_json::to_vec(&ForgedRestoreNotice {
        restore_id,
        participant_attempt: 1,
        domain,
        source_logical_sequence: source,
        result_logical_sequence: result,
    })
    .expect("encode forged notice")
}

/// Commits one raw outbox record through a plain control transaction and
/// returns it as read back from the outbox at the new head, so it carries
/// its origin sequence and authenticated observed root. `payload` receives
/// the sequence the record commits at (1 on an empty store).
pub async fn commit_raw_outbox_record(
    store: &ControlMvpStateStore,
    record_id: &str,
    options: TxnOptions,
    payload: impl FnOnce(u64) -> Vec<u8>,
) -> ControlMvpProjectionOutboxRecord {
    let origin = store
        .current_state_token()
        .await
        .map_or(1, |token| token.logical_sequence() + 1);
    let mut txn = store
        .begin_control_txn(options)
        .await
        .expect("begin raw transaction");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        record_id,
        Bytes::from(payload(origin)),
    ))
    .await
    .expect("stage raw outbox record");
    txn.commit().await.expect("commit raw outbox record");
    let record = store
        .current_projection_outbox()
        .await
        .expect("outbox")
        .into_iter()
        .find(|record| record.record_id() == record_id)
        .expect("the committed record");
    assert_eq!(Some(origin), record.origin_sequence());
    record
}
