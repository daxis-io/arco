//! Amazon S3 adapter for Arco storage contracts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{Error, Result};
use arco_storage_object_store::{ObjectStoreBackend, no_automatic_request_retries};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::DynObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::signer::Signer as ObjectStoreSigner;

/// Amazon S3 implementation of the Arco storage contract.
#[derive(Debug, Clone)]
pub struct S3StorageBackend {
    inner: ObjectStoreBackend,
    #[cfg(feature = "qualification")]
    qualification_listing: Option<ObjectStoreBackend>,
}

impl S3StorageBackend {
    /// Creates an Amazon S3 adapter from a bucket name or `s3://`/`s3a://`
    /// bucket reference.
    ///
    /// Credentials and endpoint configuration are read by the upstream S3
    /// builder from its standard environment.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty bucket or invalid S3 configuration.
    pub fn new(bucket: &str) -> Result<Self> {
        let bucket = normalize_bucket(bucket)?;
        let (s3, conditional_s3) = build_s3_clients(
            configured_s3_builder(&bucket),
            configured_s3_builder(&bucket),
            &bucket,
        )?;
        Ok(Self::from_clients(&bucket, s3, conditional_s3))
    }

    /// Creates a qualification adapter with bounded request time and ordinary retries.
    ///
    /// Conditional writes remain single-attempt. The production constructor is unchanged.
    /// # Errors
    /// Returns an error for invalid bucket/client configuration.
    #[cfg(feature = "qualification")]
    pub fn for_qualification(bucket: &str, listing_proxy: &str) -> Result<Self> {
        let bucket = normalize_bucket(bucket)?;
        let options = object_store::ClientOptions::new()
            .with_no_redirects()
            .with_timeout(Duration::from_secs(30))
            .with_allow_http(std::env::var("AWS_ALLOW_HTTP").is_ok_and(|s| s == "true"));
        let builder = || {
            configured_s3_builder(&bucket)
                .with_client_options(options.clone())
                .with_retry(object_store::RetryConfig {
                    max_retries: 1,
                    ..Default::default()
                })
        };
        let (s3, conditional_s3) = build_s3_clients(builder(), builder(), &bucket)?;
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONNECTION,
            http::HeaderValue::from_static("close"),
        );
        let listing = configured_s3_builder(&bucket)
            .with_client_options(
                options
                    .with_http1_only()
                    .with_pool_max_idle_per_host(0)
                    .with_default_headers(headers)
                    .with_proxy_url(listing_proxy)
                    .with_proxy_excludes("169.254.169.254"),
            )
            .with_retry(object_store::RetryConfig {
                max_retries: 1,
                ..Default::default()
            })
            .build()
            .map_err(|error| {
                Error::storage_with_source("failed to configure bounded listing", error)
            })?;
        let listing_conditional_store: Arc<DynObjectStore> = conditional_s3.clone();
        let mut backend = Self::from_clients(&bucket, s3, conditional_s3);
        let listing: Arc<DynObjectStore> = Arc::new(listing);
        backend.qualification_listing = Some(
            ObjectStoreBackend::new_with_ordered_listing_and_conditional_write_store(
                listing,
                listing_conditional_store,
                None,
            ),
        );
        Ok(backend)
    }

    fn from_clients(bucket: &str, s3: Arc<AmazonS3>, conditional_s3: Arc<AmazonS3>) -> Self {
        let store: Arc<DynObjectStore> = s3.clone();
        let conditional_write_store: Arc<DynObjectStore> = conditional_s3;
        let signer: Arc<dyn ObjectStoreSigner> = s3;
        let inner = if supports_ordered_listing(bucket) {
            ObjectStoreBackend::new_with_ordered_listing_and_conditional_write_store(
                store,
                conditional_write_store,
                Some(signer),
            )
        } else {
            ObjectStoreBackend::new_with_conditional_write_store(
                store,
                conditional_write_store,
                Some(signer),
            )
        };
        Self {
            inner,
            #[cfg(feature = "qualification")]
            qualification_listing: None,
        }
    }

    /// Returns whether this S3 bucket supports bounded lexicographic listing.
    #[must_use]
    pub const fn ordered_listing_enabled(&self) -> bool {
        self.inner.ordered_listing_enabled()
    }
}

fn configured_s3_builder(bucket: &str) -> AmazonS3Builder {
    AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
}

fn build_s3_clients(
    normal_builder: AmazonS3Builder,
    conditional_builder: AmazonS3Builder,
    bucket: &str,
) -> Result<(Arc<AmazonS3>, Arc<AmazonS3>)> {
    let normal = Arc::new(normal_builder.build().map_err(|error| {
        Error::storage_with_source(format!("failed to configure S3 bucket '{bucket}'"), error)
    })?);
    let conditional = Arc::new(
        conditional_builder
            .with_retry(no_automatic_request_retries())
            .build()
            .map_err(|error| {
                Error::storage_with_source(
                    format!("failed to configure conditional S3 bucket '{bucket}'"),
                    error,
                )
            })?,
    );
    Ok((normal, conditional))
}

fn normalize_bucket(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let without_scheme = trimmed
        .strip_prefix("s3://")
        .or_else(|| trimmed.strip_prefix("s3a://"))
        .unwrap_or(trimmed);
    let bucket = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(bucket, _)| bucket)
        .trim();
    if bucket.is_empty() {
        return Err(Error::InvalidInput(
            "S3 bucket name cannot be empty".to_string(),
        ));
    }
    Ok(bucket.to_string())
}

fn supports_ordered_listing(bucket: &str) -> bool {
    !bucket.ends_with("--x-s3")
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
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
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        #[cfg(feature = "qualification")]
        if let Some(listing) = &self.qualification_listing {
            return listing.list(prefix).await;
        }
        self.inner.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        #[cfg(feature = "qualification")]
        if let Some(listing) = &self.qualification_listing {
            return listing.list_page(prefix, start_after, limit).await;
        }
        self.inner.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arco_core::storage::{StorageBackend, WritePrecondition};
    use arco_storage_object_store::ObjectStoreBackend;
    use bytes::Bytes;
    use object_store::DynObjectStore;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    use super::{
        S3StorageBackend, build_s3_clients, configured_s3_builder, normalize_bucket,
        supports_ordered_listing,
    };

    #[derive(Debug, Default)]
    struct RequestCounts {
        gets: AtomicUsize,
        puts: AtomicUsize,
    }

    async fn read_request(socket: &mut TcpStream) -> io::Result<String> {
        const MAX_REQUEST_BYTES: usize = 1024 * 1024;

        let mut buffer = Vec::new();
        let mut expected_length = None;
        loop {
            let read = socket.read_buf(&mut buffer).await?;
            if read == 0 {
                break;
            }
            if buffer.len() > MAX_REQUEST_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "mock S3 request exceeded test cap",
                ));
            }

            if expected_length.is_none()
                && let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let content_length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                expected_length = Some(header_end + 4 + content_length);
            }
            if expected_length.is_some_and(|expected| buffer.len() >= expected) {
                break;
            }
        }

        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }

    async fn serve_retry_probe(listener: TcpListener, counts: Arc<RequestCounts>) {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let Ok(request) = read_request(&mut socket).await else {
                continue;
            };
            let request_line = request.lines().next().unwrap_or_default();
            if request_line.starts_with("GET ") {
                let attempt = counts.gets.fetch_add(1, Ordering::SeqCst) + 1;
                let response = if attempt == 1 {
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .as_bytes()
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nETag: \"read-v1\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\nvisible"
                };
                let _ = socket.write_all(response).await;
            } else if request_line.starts_with("PUT ") {
                let attempt = counts.puts.fetch_add(1, Ordering::SeqCst) + 1;
                // The first request body is consumed, but its connection is
                // closed before a response, modeling an ambiguous outcome.
                if attempt > 1 {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"write-v2\"\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                }
            } else {
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            }
            let _ = socket.shutdown().await;
        }
    }

    fn assert_storage_backend<T: StorageBackend>() {}

    #[test]
    fn s3_backend_implements_storage_contract() {
        assert_storage_backend::<S3StorageBackend>();
    }

    #[test]
    fn normalizes_s3_bucket_references() {
        assert_eq!(
            normalize_bucket("s3://authority/path").unwrap(),
            "authority"
        );
        assert_eq!(normalize_bucket("s3a://authority").unwrap(), "authority");
        assert_eq!(normalize_bucket("authority").unwrap(), "authority");
        assert!(normalize_bucket("  ").is_err());
    }

    #[test]
    fn directory_buckets_disable_ordered_listing() {
        assert!(supports_ordered_listing("ordinary-bucket"));
        assert!(!supports_ordered_listing("events--usw2-az1--x-s3"));
    }

    #[tokio::test]
    async fn safe_get_retries_but_ambiguous_conditional_put_is_attempted_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let counts = Arc::new(RequestCounts::default());
        let server = tokio::spawn(serve_retry_probe(listener, counts.clone()));

        let test_builder = || {
            configured_s3_builder("retry-probe")
                .with_endpoint(&endpoint)
                .with_allow_http(true)
                .with_access_key_id("test-access-key")
                .with_secret_access_key("test-secret-key")
                .with_region("us-east-1")
        };
        let (normal, conditional) =
            build_s3_clients(test_builder(), test_builder(), "retry-probe").unwrap();
        let normal_store: Arc<DynObjectStore> = normal;
        let conditional_write_store: Arc<DynObjectStore> = conditional;
        let backend = ObjectStoreBackend::new_with_conditional_write_store(
            normal_store,
            conditional_write_store,
            None,
        );

        let bytes = timeout(Duration::from_secs(5), backend.get("retry.txt"))
            .await
            .expect("GET retry probe timed out")
            .expect("default S3 client must retry one 503");
        assert_eq!(bytes, Bytes::from_static(b"visible"));
        assert_eq!(counts.gets.load(Ordering::SeqCst), 2);

        let write = timeout(
            Duration::from_secs(5),
            backend.put(
                "authority/head.json",
                Bytes::from_static(b"candidate"),
                WritePrecondition::DoesNotExist,
            ),
        )
        .await
        .expect("conditional PUT probe timed out");
        assert!(
            write.is_err(),
            "lost conditional response must stay ambiguous"
        );
        assert_eq!(counts.puts.load(Ordering::SeqCst), 1);

        server.abort();
    }
}
