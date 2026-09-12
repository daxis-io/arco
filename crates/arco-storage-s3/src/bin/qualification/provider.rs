//! Manifest-bound provider execution with conservative adapter request reservations.
use super::{INVENTORY, Result};
use arco_core::storage::{ListPage, ObjectMeta};
use arco_core::{StorageBackend, WritePrecondition, WriteResult};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const BASE: &str = "92fd19f11a547ece5004ac94cad83a3527471812";
pub const FAULT: &str = "client-lost-response-exact-reconciliation";

pub fn inventory() -> serde_json::Value {
    json!({"scenarios":INVENTORY, "client_faults":[FAULT], "repetitions":5,
        "cold_observations_per_operation_cache_repetition":200,
        "physical_accounting":"ordinary request reservations plus one hundred thousand prepaid listing connections; listing HTTP/1 Connection:close idle-pool:0", "automatic_cleanup":false})
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    path: PathBuf,
    sha256: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
pub struct Manifest {
    schema: u32,
    phase: String,
    pub run_id: String,
    base_sha: String,
    pub source_root: PathBuf,
    source_manifest: Artifact,
    binary_patch: Artifact,
    contract: Artifact,
    build_inputs: Artifact,
    supervisor: Artifact,
    pub executable_sha256: String,
    scenarios: serde_json::Value,
    pub account: String,
    pub role: String,
    pub instance_id: String,
    pub region: String,
    pub bucket: String,
    pub tenant: String,
    pub workspace: String,
    pub evidence_dir: PathBuf,
    pub listing_proxy: String,
    pub reserved_listing_requests: u64,
    expires_utc: chrono::DateTime<chrono::Utc>,
    pub ceilings: Ceilings,
    pub fixed_cost_microusd: u64,
    pub request_cost_microusd: u64,
    pub stored_byte_cost_picousd: u64,
    aws_cli: Artifact,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Ceilings {
    pub elapsed_seconds: u64,
    pub requests: u64,
    pub submitted_bytes: u64,
    pub cost_microusd: u64,
    pub evidence_bytes: u64,
}
#[derive(Deserialize)]
struct SourceFile {
    sha256: String,
    size: u64,
    mode: String,
}

pub fn hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(buffer.get(..count).ok_or("invalid file read length")?);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
impl Artifact {
    fn verify(&self) -> Result<()> {
        if !self.path.is_absolute()
            || fs::symlink_metadata(&self.path)?.file_type().is_symlink()
            || !digest(&self.sha256)
            || hash(&self.path)? != self.sha256
        {
            return Err("artifact identity mismatch".into());
        }
        Ok(())
    }
}
fn paths(root: &Path, here: &Path, output: &mut std::collections::BTreeSet<String>) -> Result<()> {
    for entry in fs::read_dir(here)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            paths(root, &path, output)?;
        } else {
            output.insert(
                path.strip_prefix(root)?
                    .to_str()
                    .ok_or("non-UTF8 source path")?
                    .to_owned(),
            );
        }
    }
    Ok(())
}

impl Manifest {
    pub fn load(path: &Path, expected: &str) -> Result<Self> {
        if !digest(expected) || fs::metadata(path)?.len() > 1024 * 1024 || hash(path)? != expected {
            return Err("manifest digest/size mismatch".into());
        }
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }
    pub fn validate(&self, real: bool) -> Result<()> {
        if self.schema != 1
            || self.phase != "provider"
            || self.base_sha != BASE
            || self.scenarios != inventory()
            || !token(&self.run_id)
            || !self.run_id.starts_with("gate7-")
            || !token(&self.tenant)
            || !self.tenant.starts_with("gate7-")
            || !token(&self.workspace)
            || self.workspace.len() > 64
            || !token(&self.bucket)
            || self.bucket.len() < 3
            || self.bucket.len() > 63
            || self.bucket.ends_with("--x-s3")
            || self.region != "us-east-2"
            || self.account.len() != 12
            || !self.account.bytes().all(|b| b.is_ascii_digit())
            || !token(&self.role)
            || !self.role.starts_with("arco-gate7-")
            || !token(&self.instance_id)
            || !self.instance_id.starts_with("i-")
            || self.expires_utc <= chrono::Utc::now()
            || !self.source_root.is_absolute()
            || !self.evidence_dir.is_absolute()
            || self.evidence_dir.starts_with(&self.source_root)
            || self.evidence_dir.exists()
        {
            return Err(
                "invalid/incomplete provider configuration or reused evidence directory".into(),
            );
        }
        let c = &self.ceilings;
        let proxy_port = self
            .listing_proxy
            .strip_prefix("http://127.0.0.1:")
            .ok_or("numeric local listing proxy required")?
            .parse::<u16>()?;
        if !(1..=21600).contains(&c.elapsed_seconds)
            || !(1..=2_000_000).contains(&c.requests)
            || !(1..=20 * 1024_u64.pow(3)).contains(&c.submitted_bytes)
            || !(1..=25_000_000).contains(&c.cost_microusd)
            || !(8 * 1024 * 1024..=512 * 1024 * 1024).contains(&c.evidence_bytes)
            || proxy_port == 0
            || self.reserved_listing_requests != 100_000
            || c.requests <= self.reserved_listing_requests
            || u128::from(self.fixed_cost_microusd)
                + u128::from(self.reserved_listing_requests)
                    * u128::from(self.request_cost_microusd)
                >= u128::from(c.cost_microusd)
            || self.fixed_cost_microusd >= c.cost_microusd
            || self.request_cost_microusd < 5
            || self.stored_byte_cost_picousd < 44
        {
            return Err("provider ceiling or conservative price admission invalid".into());
        }
        self.verify_sources(real)
    }
    pub fn verify_sources(&self, real: bool) -> Result<()> {
        for artifact in [
            &self.source_manifest,
            &self.binary_patch,
            &self.contract,
            &self.build_inputs,
            &self.supervisor,
            &self.aws_cli,
        ] {
            artifact.verify()?;
        }
        if !digest(&self.executable_sha256)
            || hash(&std::env::current_exe()?)? != self.executable_sha256
        {
            return Err("executable identity mismatch".into());
        }
        let files: BTreeMap<String, SourceFile> =
            serde_json::from_slice(&fs::read(&self.source_manifest.path)?)?;
        if files.len() < 1013 {
            return Err("incomplete source inventory".into());
        }
        let mut actual = std::collections::BTreeSet::new();
        paths(&self.source_root, &self.source_root, &mut actual)?;
        if actual != files.keys().cloned().collect() {
            return Err("source inventory membership drift".into());
        }
        if files
            .get("scripts/gate7_provider.py")
            .map(|entry| entry.sha256.as_str())
            != Some(self.supervisor.sha256.as_str())
        {
            return Err("supervisor does not match candidate source".into());
        }
        for (name, record) in files {
            if Path::new(&name).is_absolute()
                || name.split('/').any(|p| matches!(p, "" | "." | ".."))
            {
                return Err("source path escape".into());
            }
            let path = self.source_root.join(name);
            let meta = fs::symlink_metadata(&path)?;
            if !meta.is_file() || meta.len() != record.size || hash(&path)? != record.sha256 {
                return Err("source drift".into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if format!("0o{:o}", meta.permissions().mode() & 0o777) != record.mode {
                    return Err("source mode drift".into());
                }
            }
        }
        let build: serde_json::Value = serde_json::from_slice(&fs::read(&self.build_inputs.path)?)?;
        if build.get("rust").and_then(serde_json::Value::as_str) != Some("1.88.0")
            || build
                .get("cargo_lock_sha256")
                .and_then(serde_json::Value::as_str)
                != Some(hash(&self.source_root.join("Cargo.lock"))?.as_str())
        {
            return Err("build inputs mismatch".into());
        }
        if real
            && (!cfg!(target_os = "linux")
                || !cfg!(target_arch = "x86_64")
                || build.get("target").and_then(serde_json::Value::as_str)
                    != Some("x86_64-unknown-linux-gnu"))
        {
            return Err("verified Linux x86_64 executable required".into());
        }
        if real {
            let python: Artifact = serde_json::from_value(
                build
                    .get("supervisor_python")
                    .ok_or("missing Python identity")?
                    .clone(),
            )?;
            python.verify()?;
            if !build
                .get("supervisor_python_version")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|version| version.starts_with("3.11."))
            {
                return Err("bound Python 3.11 interpreter required".into());
            }
        }
        Ok(())
    }
    pub fn prefix(&self, repetition: usize) -> String {
        format!(
            "tenant={}/workspace={}-r{repetition}/",
            self.tenant, self.workspace
        )
    }
    pub fn supervised(&self, expected: &str) -> Result<()> {
        let pid = std::env::var("ARCO_GATE7_SUPERVISOR_PID")?.parse::<u32>()?;
        let status = fs::read_to_string("/proc/self/status")?;
        if !status.lines().any(|line| {
            line.strip_prefix("PPid:")
                .is_some_and(|value| value.trim() == pid.to_string())
        }) {
            return Err("bound supervisor is not the parent process".into());
        }
        let parent = fs::read(format!("/proc/{pid}/cmdline"))?;
        let build: serde_json::Value = serde_json::from_slice(&fs::read(&self.build_inputs.path)?)?;
        let python: Artifact = serde_json::from_value(
            build
                .get("supervisor_python")
                .ok_or("missing Python identity")?
                .clone(),
        )?;
        python.verify()?;
        let parent_exe = fs::canonicalize(format!("/proc/{pid}/exe"))?;
        if parent_exe != python.path || hash(&parent_exe)? != python.sha256 {
            return Err("parent Python interpreter identity mismatch".into());
        }
        if parent.split(|byte| *byte == 0).nth(1) != Some(b"-B")
            || parent.split(|byte| *byte == 0).nth(2)
                != Some(self.supervisor.path.as_os_str().as_encoded_bytes())
        {
            return Err("parent supervisor executable identity mismatch".into());
        }
        let directory = self.evidence_dir.with_file_name(format!(
            "{}-supervision",
            self.evidence_dir
                .file_name()
                .ok_or("evidence directory name missing")?
                .to_string_lossy()
        ));
        let proof: serde_json::Value =
            serde_json::from_slice(&fs::read(directory.join("proxy-bound.json"))?)?;
        let state = proof.get("state").ok_or("proxy binding missing")?;
        if proof.get("sha256").and_then(serde_json::Value::as_str)
            != Some(format!("{:x}", Sha256::digest(serde_json::to_vec(state)?)).as_str())
            || state.get("pid").and_then(serde_json::Value::as_u64) != Some(u64::from(pid))
            || state
                .get("manifest_sha256")
                .and_then(serde_json::Value::as_str)
                != Some(expected)
            || state
                .get("listing_proxy")
                .and_then(serde_json::Value::as_str)
                != Some(self.listing_proxy.as_str())
        {
            return Err("supervised listing proxy binding mismatch".into());
        }
        Ok(())
    }
    pub async fn identity(&self) -> Result<serde_json::Value> {
        for (key, _) in std::env::vars_os() {
            let key = key.to_string_lossy();
            if (key.starts_with("AWS_") && key != "AWS_REGION")
                || ["http_proxy", "https_proxy", "all_proxy"]
                    .contains(&key.to_ascii_lowercase().as_str())
            {
                return Err(format!(
                    "unexpected provider credential/endpoint configuration: {key}"
                )
                .into());
            }
        }
        if std::env::var("AWS_REGION")? != self.region {
            return Err("provider region drift".into());
        }
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(&self.aws_cli.path)
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .env("AWS_CONFIG_FILE", "/dev/null")
                .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
                .env("AWS_MAX_ATTEMPTS", "1")
                .args([
                    "sts",
                    "get-caller-identity",
                    "--region",
                    &self.region,
                    "--output",
                    "json",
                    "--cli-connect-timeout",
                    "5",
                    "--cli-read-timeout",
                    "10",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() {
            return Err("instance-profile credential acquisition failed".into());
        }
        let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let expected = format!(
            "arn:aws:sts::{}:assumed-role/{}/{}",
            self.account, self.role, self.instance_id
        );
        if value.get("Account").and_then(serde_json::Value::as_str) != Some(self.account.as_str())
            || value.get("Arn").and_then(serde_json::Value::as_str) != Some(expected.as_str())
        {
            return Err("account/role/host identity mismatch".into());
        }
        Ok(value)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Counters {
    pub requests_upper_bound: u64,
    pub transport_attempts: Option<u64>,
    pub submitted_bytes: u64,
    pub adapter_calls: u64,
    pub evidence_bytes: u64,
    pub in_flight: u64,
    pub stop_reason: Option<String>,
}
#[derive(Debug)]
pub struct Journal {
    pub manifest: Arc<Manifest>,
    pub started: Instant,
    pub counters: Mutex<Counters>,
    file: Mutex<File>,
    lost_responses: Mutex<std::collections::BTreeSet<String>>,
}
impl Journal {
    pub fn create(manifest: Arc<Manifest>, config: &Path, config_sha: &str) -> Result<Arc<Self>> {
        fs::create_dir(&manifest.evidence_dir)?;
        fs::copy(config, manifest.evidence_dir.join("manifest.json"))?;
        fs::write(manifest.evidence_dir.join("manifest.sha256"), config_sha)?;
        for name in ["manifest.json", "manifest.sha256"] {
            File::open(manifest.evidence_dir.join(name))?.sync_all()?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(manifest.evidence_dir.join("operations.jsonl"))?;
        let reserved_listing_requests = manifest.reserved_listing_requests;
        let journal = Arc::new(Self {
            manifest,
            started: Instant::now(),
            counters: Mutex::new(Counters {
                requests_upper_bound: reserved_listing_requests,
                ..Counters::default()
            }),
            file: Mutex::new(file),
            lost_responses: Mutex::new(std::collections::BTreeSet::new()),
        });
        journal.record(&json!({"kind":"run-start", "utc":chrono::Utc::now(), "pid":std::process::id(), "run_id":journal.manifest.run_id}), true)?;
        journal.state("running")?;
        Ok(journal)
    }
    pub fn cost(&self, requests: u64, bytes: u64) -> u128 {
        u128::from(self.manifest.fixed_cost_microusd)
            + u128::from(requests) * u128::from(self.manifest.request_cost_microusd)
            + (u128::from(bytes) * u128::from(self.manifest.stored_byte_cost_picousd))
                .div_ceil(1_000_000)
    }
    pub fn record(&self, value: &serde_json::Value, durable: bool) -> Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        bytes.push(b'\n');
        let mut counters = self.counters.lock().unwrap();
        let journal_limit = self
            .manifest
            .ceilings
            .evidence_bytes
            .saturating_sub(4 * 1024 * 1024)
            * 3
            / 4;
        if counters.evidence_bytes + bytes.len() as u64 > journal_limit {
            counters.stop_reason = Some("evidence ceiling".into());
            return Err("evidence ceiling".into());
        }
        let mut file = self.file.lock().unwrap();
        file.write_all(&bytes)?;
        counters.evidence_bytes += bytes.len() as u64;
        drop(counters);
        if durable {
            file.sync_data()?;
        }
        drop(file);
        Ok(())
    }
    pub fn state(&self, state: &str) -> Result<()> {
        self.file.lock().unwrap().sync_data()?;
        let counters = self.counters.lock().unwrap();
        let value = json!({"state":state, "run_id":self.manifest.run_id, "utc":chrono::Utc::now(),
            "elapsed_ns":self.started.elapsed().as_nanos(), "counters":*counters,
            "cost_upper_microusd":self.cost(counters.requests_upper_bound, counters.submitted_bytes),
            "automatic_resume":false, "automatic_cleanup":false, "pilot_qualified":false});
        drop(counters);
        let temp = self.manifest.evidence_dir.join("state.tmp");
        let mut file = File::create(&temp)?;
        let bytes = serde_json::to_vec(&value)?;
        if bytes.len() > 64 * 1024 {
            return Err("state envelope ceiling".into());
        }
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temp, self.manifest.evidence_dir.join("state.json"))?;
        File::open(&self.manifest.evidence_dir)?.sync_all()?;
        Ok(())
    }
    fn admit(&self, requests: u64, bytes: u64) -> arco_core::Result<u64> {
        let mut c = self.counters.lock().unwrap();
        let requests = c
            .requests_upper_bound
            .checked_add(requests)
            .ok_or_else(|| arco_core::Error::storage("request counter overflow"))?;
        let submitted = c
            .submitted_bytes
            .checked_add(bytes)
            .ok_or_else(|| arco_core::Error::storage("byte counter overflow"))?;
        let limit = &self.manifest.ceilings;
        let reason = if c.stop_reason.is_some() {
            c.stop_reason.clone()
        } else if self.manifest.evidence_dir.join("stop-request").exists() {
            Some("requested stop".into())
        } else if self.started.elapsed() >= Duration::from_secs(limit.elapsed_seconds)
            || chrono::Utc::now() >= self.manifest.expires_utc
        {
            Some("elapsed/expiry ceiling".into())
        } else if requests > limit.requests
            || submitted > limit.submitted_bytes
            || self.cost(requests, submitted) > u128::from(limit.cost_microusd)
        {
            Some("resource/cost ceiling".into())
        } else {
            None
        };
        if let Some(reason) = reason {
            c.stop_reason = Some(reason.clone());
            return Err(arco_core::Error::storage(reason));
        }
        c.requests_upper_bound = requests;
        c.submitted_bytes = submitted;
        c.in_flight += 1;
        c.adapter_calls += 1;
        Ok(c.adapter_calls)
    }
}

fn contained(path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|p| path.starts_with(p))
        && !path.contains(['\\', '%', '\0'])
        && !path.split('/').any(|part| matches!(part, "." | ".."))
}

pub struct Observed {
    pub inner: Arc<dyn StorageBackend>,
    pub journal: Arc<Journal>,
}
impl Observed {
    async fn call<T>(
        &self,
        operation: &str,
        path: &str,
        requests: u64,
        bytes: u64,
        future: impl Future<Output = arco_core::Result<T>>,
    ) -> arco_core::Result<T> {
        if !contained(
            path,
            &(0..5)
                .map(|r| self.journal.manifest.prefix(r))
                .collect::<Vec<_>>(),
        ) {
            return Err(arco_core::Error::InvalidInput(
                "disposable namespace escape".into(),
            ));
        }
        let id = self.journal.admit(requests, bytes)?;
        self.journal
            .record(
                &json!({"kind":"adapter-intent", "id":id, "operation":operation, "path":path,
            "requests_reserved":requests, "submitted_bytes_reserved":bytes}),
                operation == "put",
            )
            .map_err(|e| {
                self.journal.counters.lock().unwrap().in_flight -= 1;
                arco_core::Error::storage(e.to_string())
            })?;
        let start = Instant::now();
        let result = future.await;
        self.journal.counters.lock().unwrap().in_flight -= 1;
        self.journal
            .record(
                &json!({"kind":"adapter-outcome", "id":id, "operation":operation, "path":path,
            "elapsed_ns":start.elapsed().as_nanos(), "success":result.is_ok()}),
                false,
            )
            .map_err(|e| arco_core::Error::storage(e.to_string()))?;
        result
    }
}

#[async_trait]
impl StorageBackend for Observed {
    async fn get(&self, p: &str) -> arco_core::Result<Bytes> {
        self.call("get", p, 2, 0, self.inner.get(p)).await
    }
    async fn get_range(&self, p: &str, r: Range<u64>) -> arco_core::Result<Bytes> {
        self.call("range", p, 4, 0, self.inner.get_range(p, r))
            .await
    }
    async fn head(&self, p: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.call("head", p, 2, 0, self.inner.head(p)).await
    }
    async fn delete(&self, p: &str) -> arco_core::Result<()> {
        self.call("delete", p, 2, 0, self.inner.delete(p)).await
    }
    async fn put(
        &self,
        p: &str,
        b: Bytes,
        condition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.journal.record(&json!({"kind":"candidate-write", "path":p, "sha256":format!("{:x}", Sha256::digest(&b)), "size":b.len(), "precondition":format!("{condition:?}")}), true).map_err(|e| arco_core::Error::storage(e.to_string()))?;
        let result = self
            .call(
                "put",
                p,
                3,
                2 * b.len() as u64,
                self.inner.put(p, b, condition),
            )
            .await;
        if p.ends_with("/conformance/client-lost-response")
            && matches!(&result, Ok(WriteResult::Success { .. }))
            && self
                .journal
                .lost_responses
                .lock()
                .unwrap()
                .insert(p.to_owned())
        {
            self.journal.record(&json!({"kind":"injected-client-fault", "fault":FAULT, "path":p, "underlying_write_completed":true}), true).map_err(|e| arco_core::Error::storage(e.to_string()))?;
            return Err(arco_core::Error::storage(
                "injected lost client response after successful write",
            ));
        }
        result
    }
    async fn list(&self, p: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        let mut result = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(p, cursor.as_deref(), 128).await?;
            result.extend(page.objects);
            cursor = page.next_start_after;
            if cursor.is_none() {
                return Ok(result);
            }
        }
    }
    async fn list_page(
        &self,
        p: &str,
        after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        if after.is_some_and(|a| !a.starts_with(p)) {
            return Err(arco_core::Error::InvalidInput("cursor escape".into()));
        }
        let limit = limit.min(1000);
        // The independently capped listing proxy is prepaid before any admission.
        let page = self
            .call("list_page", p, 0, 0, self.inner.list_page(p, after, limit))
            .await?;
        let mut previous = after;
        for object in &page.objects {
            if !object.path.starts_with(p) || previous.is_some_and(|s| object.path.as_str() <= s) {
                return Err(arco_core::Error::storage(
                    "listing did not make ordered progress",
                ));
            }
            previous = Some(&object.path);
        }
        if page
            .next_start_after
            .as_deref()
            .is_some_and(|s| Some(s) != previous || Some(s) == after)
        {
            return Err(arco_core::Error::storage(
                "listing cursor did not make progress",
            ));
        }
        Ok(page)
    }
    async fn signed_url(&self, _: &str, _: Duration) -> arco_core::Result<String> {
        Err(arco_core::Error::InvalidInput(
            "qualification forbids signed URL escape".into(),
        ))
    }
}

pub async fn client_fault(storage: arco_core::ScopedStorage) -> Result<()> {
    let path = "conformance/client-lost-response";
    if storage
        .put(
            path,
            Bytes::from_static(b"exact-candidate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .is_ok()
    {
        return Err("lost response injection not delivered".into());
    }
    let observed = storage.head(path).await?.ok_or("lost publication absent")?;
    if storage.get(path).await? != Bytes::from_static(b"exact-candidate") {
        return Err("publication reconciliation mismatch".into());
    }
    if !matches!(storage.put(path, Bytes::from_static(b"exact-candidate"), WritePrecondition::DoesNotExist).await?, WriteResult::PreconditionFailed { current_version } if current_version == observed.version)
    {
        return Err("reconciled publication was not fenced".into());
    }
    storage.delete(path).await?;
    super::JOURNAL.get().ok_or("missing evidence journal")?.record(&json!({"kind":"client-publication-reconciled", "fault":FAULT, "path":format!("tenant={}/workspace={}/{path}", storage.tenant_id(), storage.workspace_id())}), true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Arc<Journal> {
        let root = std::env::temp_dir().join(format!("gate7-provider-unit-{}", ulid::Ulid::new()));
        fs::create_dir(&root).unwrap();
        let config = root.join("config.json");
        fs::write(&config, b"{}").unwrap();
        let artifact = || Artifact {
            path: config.clone(),
            sha256: hash(&config).unwrap(),
        };
        let manifest = Manifest {
            schema: 1,
            phase: "provider".into(),
            run_id: "gate7-test".into(),
            base_sha: BASE.into(),
            source_root: root.join("source"),
            source_manifest: artifact(),
            binary_patch: artifact(),
            contract: artifact(),
            build_inputs: artifact(),
            supervisor: artifact(),
            executable_sha256: String::new(),
            scenarios: inventory(),
            account: "012832591253".into(),
            role: "arco-gate7-test".into(),
            instance_id: "i-test".into(),
            region: "us-east-2".into(),
            bucket: "gate7-loopback".into(),
            tenant: "gate7-test".into(),
            workspace: "qualification".into(),
            evidence_dir: root.join("evidence"),
            listing_proxy: "http://127.0.0.1:1".into(),
            reserved_listing_requests: 0,
            expires_utc: chrono::Utc::now() + chrono::Duration::hours(1),
            ceilings: Ceilings {
                elapsed_seconds: 60,
                requests: 10000,
                submitted_bytes: 1024,
                cost_microusd: 1_000_000,
                evidence_bytes: 8 * 1024 * 1024,
            },
            fixed_cost_microusd: 0,
            request_cost_microusd: 5,
            stored_byte_cost_picousd: 44,
            aws_cli: artifact(),
        };
        Journal::create(Arc::new(manifest), &config, &hash(&config).unwrap()).unwrap()
    }
    #[tokio::test]
    async fn failed_intent_write_does_not_leave_phantom_in_flight_work() {
        let journal = fixture();
        *journal.file.lock().unwrap() =
            File::open(journal.manifest.evidence_dir.join("manifest.json")).unwrap();
        let observed = Observed {
            inner: Arc::new(arco_core::MemoryBackend::new()),
            journal: journal.clone(),
        };
        assert!(
            observed
                .get(&(journal.manifest.prefix(0) + "key"))
                .await
                .is_err()
        );
        assert_eq!(journal.counters.lock().unwrap().in_flight, 0);
        assert_eq!(journal.counters.lock().unwrap().requests_upper_bound, 2);
        fs::remove_dir_all(journal.manifest.evidence_dir.parent().unwrap()).unwrap();
    }
    #[tokio::test]
    async fn namespace_and_cursor_fail_before_storage_calls() {
        let journal = fixture();
        let inner = Arc::new(arco_core::MemoryBackend::new());
        let observed = Observed {
            inner: inner.clone(),
            journal: journal.clone(),
        };
        for path in [
            "other/key",
            "tenant=gate7-test/workspace=qualification-r0evil/key",
            "tenant=gate7-test/workspace=qualification-r0/../key",
            "tenant=gate7-test/workspace=qualification-r0/%2e%2e/key",
        ] {
            assert!(
                observed
                    .put(path, Bytes::from_static(b"x"), WritePrecondition::None)
                    .await
                    .is_err()
            );
            assert!(inner.head(path).await.unwrap().is_none());
        }
        assert!(
            observed
                .list_page(&journal.manifest.prefix(0), Some("other/key"), 10)
                .await
                .is_err()
        );
        assert_eq!(journal.counters.lock().unwrap().adapter_calls, 0);
        fs::remove_dir_all(journal.manifest.evidence_dir.parent().unwrap()).unwrap();
    }
    #[tokio::test]
    async fn reservations_stop_before_dispatch_and_are_never_refunded() {
        let journal = fixture();
        let observed = Observed {
            inner: Arc::new(arco_core::MemoryBackend::new()),
            journal: journal.clone(),
        };
        let path = journal.manifest.prefix(0) + "key";
        assert!(observed.get(&path).await.is_err());
        assert_eq!(journal.counters.lock().unwrap().requests_upper_bound, 2);
        assert!(
            observed
                .put(&path, Bytes::from(vec![0; 513]), WritePrecondition::None)
                .await
                .is_err()
        );
        assert!(observed.inner.head(&path).await.unwrap().is_none());
        assert_eq!(journal.counters.lock().unwrap().requests_upper_bound, 2);
        assert!(
            observed.head(&path).await.is_err(),
            "ceiling stops remain latched"
        );
        assert_eq!(journal.counters.lock().unwrap().in_flight, 0);
        fs::remove_dir_all(journal.manifest.evidence_dir.parent().unwrap()).unwrap();
    }
    #[tokio::test]
    async fn stop_and_lost_response_preserve_exact_candidate() {
        let journal = fixture();
        let observed = Observed {
            inner: Arc::new(arco_core::MemoryBackend::new()),
            journal: journal.clone(),
        };
        let path = journal.manifest.prefix(0) + "conformance/client-lost-response";
        assert!(
            observed
                .put(
                    &path,
                    Bytes::from_static(b"exact"),
                    WritePrecondition::DoesNotExist
                )
                .await
                .is_err()
        );
        assert_eq!(
            observed.get(&path).await.unwrap(),
            Bytes::from_static(b"exact")
        );
        assert!(matches!(
            observed
                .put(
                    &path,
                    Bytes::from_static(b"exact"),
                    WritePrecondition::DoesNotExist
                )
                .await
                .unwrap(),
            WriteResult::PreconditionFailed { .. }
        ));
        fs::write(journal.manifest.evidence_dir.join("stop-request"), b"stop").unwrap();
        assert!(observed.delete(&path).await.is_err());
        assert!(observed.inner.head(&path).await.unwrap().is_some());
        journal.state("failed-recovery-required").unwrap();
        let records =
            fs::read_to_string(journal.manifest.evidence_dir.join("operations.jsonl")).unwrap();
        assert!(records.contains("candidate-write") && records.contains("injected-client-fault"));
        fs::remove_dir_all(journal.manifest.evidence_dir.parent().unwrap()).unwrap();
    }
    #[test]
    fn manifest_and_evidence_identity_fail_closed() {
        let journal = fixture();
        let path = journal.manifest.evidence_dir.join("manifest.json");
        assert!(Manifest::load(&path, &"0".repeat(64)).is_err());
        assert!(
            journal.manifest.validate(false).is_err(),
            "reused evidence/configuration must fail"
        );
        assert_eq!(journal.cost(2, 1), 11);
        fs::remove_dir_all(journal.manifest.evidence_dir.parent().unwrap()).unwrap();
    }
}
