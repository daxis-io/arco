#![allow(dead_code)]

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arco_core::error::{Error, Result};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptedOpKind {
    Get,
    Head,
    Put,
    Delete,
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreconditionMatcher {
    Any,
    None,
    DoesNotExist,
    MatchesVersion,
}

impl PreconditionMatcher {
    fn matches(&self, precondition: &WritePrecondition) -> bool {
        match self {
            Self::Any => true,
            Self::None => matches!(precondition, WritePrecondition::None),
            Self::DoesNotExist => matches!(precondition, WritePrecondition::DoesNotExist),
            Self::MatchesVersion => matches!(precondition, WritePrecondition::MatchesVersion(_)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedEffect {
    Error(String),
    PreconditionFailed { current_version: String },
}

#[derive(Debug)]
pub struct ScriptedRule {
    kind: ScriptedOpKind,
    path_prefix: String,
    precondition: PreconditionMatcher,
    skip: AtomicUsize,
    remaining: AtomicUsize,
    effect: ScriptedEffect,
}

impl ScriptedRule {
    pub fn put(
        path_prefix: impl Into<String>,
        precondition: PreconditionMatcher,
        times: usize,
        effect: ScriptedEffect,
    ) -> Self {
        Self {
            kind: ScriptedOpKind::Put,
            path_prefix: path_prefix.into(),
            precondition,
            skip: AtomicUsize::new(0),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    pub fn put_after(
        path_prefix: impl Into<String>,
        precondition: PreconditionMatcher,
        skip: usize,
        times: usize,
        effect: ScriptedEffect,
    ) -> Self {
        Self {
            kind: ScriptedOpKind::Put,
            path_prefix: path_prefix.into(),
            precondition,
            skip: AtomicUsize::new(skip),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    pub fn get(path_prefix: impl Into<String>, times: usize, effect: ScriptedEffect) -> Self {
        Self {
            kind: ScriptedOpKind::Get,
            path_prefix: path_prefix.into(),
            precondition: PreconditionMatcher::Any,
            skip: AtomicUsize::new(0),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    pub fn head(path_prefix: impl Into<String>, times: usize, effect: ScriptedEffect) -> Self {
        Self {
            kind: ScriptedOpKind::Head,
            path_prefix: path_prefix.into(),
            precondition: PreconditionMatcher::Any,
            skip: AtomicUsize::new(0),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    pub fn list(path_prefix: impl Into<String>, times: usize, effect: ScriptedEffect) -> Self {
        Self {
            kind: ScriptedOpKind::List,
            path_prefix: path_prefix.into(),
            precondition: PreconditionMatcher::Any,
            skip: AtomicUsize::new(0),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    pub fn delete(path_prefix: impl Into<String>, times: usize, effect: ScriptedEffect) -> Self {
        Self {
            kind: ScriptedOpKind::Delete,
            path_prefix: path_prefix.into(),
            precondition: PreconditionMatcher::Any,
            skip: AtomicUsize::new(0),
            remaining: AtomicUsize::new(times),
            effect,
        }
    }

    fn matches(
        &self,
        kind: ScriptedOpKind,
        path: &str,
        precondition: Option<&WritePrecondition>,
    ) -> bool {
        self.kind == kind
            && path.starts_with(&self.path_prefix)
            && precondition.is_none_or(|value| self.precondition.matches(value))
            && self.remaining.load(Ordering::SeqCst) > 0
    }

    fn consume(&self) -> Option<ScriptedEffect> {
        let skip = self.skip.load(Ordering::SeqCst);
        if skip > 0 {
            self.skip.fetch_sub(1, Ordering::SeqCst);
            return None;
        }

        let remaining = self.remaining.load(Ordering::SeqCst);
        if remaining == 0 {
            return None;
        }
        self.remaining.fetch_sub(1, Ordering::SeqCst);
        Some(self.effect.clone())
    }
}

#[derive(Debug, Default)]
pub struct ScriptedBackend {
    inner: MemoryBackend,
    rules: std::sync::Mutex<Vec<ScriptedRule>>,
}

impl ScriptedBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_rule(&self, rule: ScriptedRule) {
        self.rules.lock().expect("scripted rules lock").push(rule);
    }

    fn matching_effect(
        &self,
        kind: ScriptedOpKind,
        path: &str,
        precondition: Option<&WritePrecondition>,
    ) -> Option<ScriptedEffect> {
        self.rules
            .lock()
            .expect("scripted rules lock")
            .iter()
            .find(|rule| rule.matches(kind, path, precondition))
            .and_then(ScriptedRule::consume)
    }

    fn apply_non_put_effect(effect: ScriptedEffect) -> Result<()> {
        match effect {
            ScriptedEffect::Error(message) => Err(Error::storage(message)),
            ScriptedEffect::PreconditionFailed { current_version } => Err(Error::storage(format!(
                "unexpected precondition failure effect for non-put op: {current_version}"
            ))),
        }
    }
}

#[async_trait]
impl StorageBackend for ScriptedBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        if let Some(effect) = self.matching_effect(ScriptedOpKind::Get, path, None) {
            Self::apply_non_put_effect(effect)?;
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        if let Some(effect) = self.matching_effect(ScriptedOpKind::Put, path, Some(&precondition)) {
            return match effect {
                ScriptedEffect::Error(message) => Err(Error::storage(message)),
                ScriptedEffect::PreconditionFailed { current_version } => {
                    Ok(WriteResult::PreconditionFailed { current_version })
                }
            };
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        if let Some(effect) = self.matching_effect(ScriptedOpKind::Delete, path, None) {
            Self::apply_non_put_effect(effect)?;
        }
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        if let Some(effect) = self.matching_effect(ScriptedOpKind::List, prefix, None) {
            Self::apply_non_put_effect(effect)?;
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        if let Some(effect) = self.matching_effect(ScriptedOpKind::Head, path, None) {
            Self::apply_non_put_effect(effect)?;
        }
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}
