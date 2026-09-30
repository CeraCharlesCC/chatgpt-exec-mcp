//! Outbound MCP Events callbacks. Every attempt resolves, validates, and pins its
//! destination independently; neither proxies nor redirects can bypass that check.
use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hmac::{Hmac, Mac};
use rand::Rng;
use reqwest::{
    Client, Response, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use url::{Host, Url};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DNS_TIMEOUT: Duration = Duration::from_secs(5);
const VERIFICATION_TTL: Duration = Duration::from_secs(300);
const MAX_VERIFICATIONS: usize = 256;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_CHALLENGE_RESPONSE_BYTES: usize = 4096;

/// Deliberately contains no URL, body, or secret, including in its Debug output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebhookError {
    InvalidUrl,
    NonPublicAddress,
    Dns,
    Timeout,
    InvalidSecret,
    InvalidRequest,
    Network,
    ChallengeFailed,
}

impl WebhookError {
    pub fn reason(self) -> &'static str {
        match self {
            Self::InvalidUrl => "invalid_url",
            Self::NonPublicAddress => "non_public_address",
            Self::Dns => "dns_failed",
            Self::Timeout => "timeout",
            Self::InvalidSecret => "invalid_secret",
            Self::InvalidRequest => "invalid_request",
            Self::Network => "network_failed",
            Self::ChallengeFailed => "challenge_failed",
        }
    }
}

impl fmt::Display for WebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for WebhookError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Delivered,
    Retryable,
    PermanentFailure,
    Gone,
}

#[derive(Default)]
pub struct WebhookClient {
    verified: Mutex<HashMap<(String, String), Instant>>,
}

impl WebhookClient {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn cache_verified_for_test(&self, principal: &str, url: &str) {
        self.cache_verification(principal, url);
    }

    /// Verify ownership before activation. Cache entries are isolated by principal
    /// and callback URL and never skip destination checks on subsequent delivery.
    pub async fn verify(
        &self,
        subscription_id: &str,
        principal: &str,
        callback_url: &str,
        secret: &str,
    ) -> Result<(), WebhookError> {
        validate_secret(secret)?;
        let url = callback_url_checked(callback_url)?;
        if self.verification_cached(principal, url.as_str()) {
            return Ok(());
        }
        let client = secure_client(&url).await?;
        let challenge = random_token();
        let webhook_id = format!("msg_verification_{}", random_token());
        let body = serde_json::json!({"type": "verification", "challenge": challenge}).to_string();
        let response = post_signed(
            &client,
            &url,
            subscription_id,
            secret,
            None,
            &webhook_id,
            &body,
        )
        .await?;
        check_challenge(response, &challenge).await?;
        self.cache_verification(principal, url.as_str());
        Ok(())
    }

    /// One attempt only. The persistent queue owns bounded backoff and reuses the
    /// same event ID/body; each call creates a fresh signing timestamp/signature.
    pub async fn deliver(
        &self,
        subscription_id: &str,
        callback_url: &str,
        secret: &str,
        previous_secret: Option<&str>,
        event_id: &str,
        body: &str,
    ) -> DeliveryOutcome {
        let result = async {
            let url = callback_url_checked(callback_url)?;
            let client = secure_client(&url).await?;
            post_signed(
                &client,
                &url,
                subscription_id,
                secret,
                previous_secret,
                event_id,
                body,
            )
            .await
        }
        .await;
        match result {
            Ok(response) => classify_status(response.status()),
            Err(WebhookError::Dns | WebhookError::Timeout | WebhookError::Network) => {
                DeliveryOutcome::Retryable
            }
            Err(_) => DeliveryOutcome::PermanentFailure,
        }
    }

    fn verification_cached(&self, principal: &str, url: &str) -> bool {
        let now = Instant::now();
        let mut cache = self.verified.lock().unwrap_or_else(|e| e.into_inner());
        cache.retain(|_, verified_at| now.duration_since(*verified_at) < VERIFICATION_TTL);
        cache.contains_key(&(principal.to_owned(), url.to_owned()))
    }

    fn cache_verification(&self, principal: &str, url: &str) {
        let now = Instant::now();
        let mut cache = self.verified.lock().unwrap_or_else(|e| e.into_inner());
        cache.retain(|_, verified_at| now.duration_since(*verified_at) < VERIFICATION_TTL);
        if cache.len() >= MAX_VERIFICATIONS
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, time)| **time)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert((principal.to_owned(), url.to_owned()), now);
    }
}

pub fn validate_secret(secret: &str) -> Result<(), WebhookError> {
    decode_secret(secret).map(|_| ())
}

fn decode_secret(secret: &str) -> Result<Vec<u8>, WebhookError> {
    if secret.len() > 128 {
        return Err(WebhookError::InvalidSecret);
    }
    let encoded = secret
        .strip_prefix("whsec_")
        .ok_or(WebhookError::InvalidSecret)?;
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| WebhookError::InvalidSecret)?;
    if !(24..=64).contains(&decoded.len()) {
        return Err(WebhookError::InvalidSecret);
    }
    Ok(decoded)
}

fn callback_url_checked(callback_url: &str) -> Result<Url, WebhookError> {
    if callback_url.len() > 4096 || callback_url.chars().any(|c| c.is_control()) {
        return Err(WebhookError::InvalidUrl);
    }
    let url = Url::parse(callback_url).map_err(|_| WebhookError::InvalidUrl)?;
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(WebhookError::InvalidUrl);
    }
    match url.host() {
        Some(Host::Ipv4(ip)) if !is_public_address(ip.into()) => {
            Err(WebhookError::NonPublicAddress)
        }
        Some(Host::Ipv6(ip)) if !is_public_address(ip.into()) => {
            Err(WebhookError::NonPublicAddress)
        }
        _ => Ok(url),
    }
}

/// Conservative public unicast policy. IPv6 transition, local, multicast, and
/// special-purpose ranges are excluded rather than translated into IPv4 targets.
fn is_public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && b == 168)
                || (a == 192 && b == 0 && (c == 0 || c == 2))
                || (a == 192 && b == 88 && c == 99)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x0200 || s[1] == 0x0db8))
                && s[0] != 0x2002
                && s[0] != 0x3fff
        }
    }
}

fn validate_addresses(addresses: &[SocketAddr]) -> Result<(), WebhookError> {
    if addresses.is_empty() {
        return Err(WebhookError::Dns);
    }
    if addresses.len() > 64
        || addresses
            .iter()
            .any(|address| !is_public_address(address.ip()))
    {
        return Err(WebhookError::NonPublicAddress);
    }
    Ok(())
}

async fn secure_client(url: &Url) -> Result<Client, WebhookError> {
    let port = url
        .port_or_known_default()
        .ok_or(WebhookError::InvalidUrl)?;
    let (host, addresses) = match url.host().ok_or(WebhookError::InvalidUrl)? {
        Host::Ipv4(ip) => (ip.to_string(), vec![SocketAddr::new(ip.into(), port)]),
        Host::Ipv6(ip) => (ip.to_string(), vec![SocketAddr::new(ip.into(), port)]),
        Host::Domain(domain) => {
            let resolved =
                tokio::time::timeout(DNS_TIMEOUT, tokio::net::lookup_host((domain, port)))
                    .await
                    .map_err(|_| WebhookError::Timeout)?
                    .map_err(|_| WebhookError::Dns)?;
            (domain.to_owned(), resolved.collect())
        }
    };
    validate_addresses(&addresses)?;
    // A fresh client has no reused connection or alternate DNS answer. TLS still
    // verifies the original URL host because only its address resolution is pinned.
    Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(DNS_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| WebhookError::Network)
}

fn signature(secret: &str, id: &str, timestamp: &str, body: &str) -> Result<String, WebhookError> {
    let key = decode_secret(secret)?;
    let mut signer =
        Hmac::<Sha256>::new_from_slice(&key).map_err(|_| WebhookError::InvalidSecret)?;
    signer.update(id.as_bytes());
    signer.update(b".");
    signer.update(timestamp.as_bytes());
    signer.update(b".");
    signer.update(body.as_bytes());
    Ok(format!(
        "v1,{}",
        STANDARD.encode(signer.finalize().into_bytes())
    ))
}

fn signed_headers(
    subscription_id: &str,
    secret: &str,
    previous_secret: Option<&str>,
    id: &str,
    timestamp: &str,
    body: &str,
) -> Result<HeaderMap, WebhookError> {
    let mut signatures = signature(secret, id, timestamp, body)?;
    if let Some(previous) = previous_secret.filter(|previous| *previous != secret) {
        signatures.push(' ');
        signatures.push_str(&signature(previous, id, timestamp, body)?);
    }
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("content-type", "application/json"),
        ("webhook-id", id),
        ("webhook-timestamp", timestamp),
        ("webhook-signature", signatures.as_str()),
        ("x-mcp-subscription-id", subscription_id),
    ] {
        headers.insert(
            name,
            HeaderValue::from_str(value).map_err(|_| WebhookError::InvalidRequest)?,
        );
    }
    Ok(headers)
}

async fn post_signed(
    client: &Client,
    url: &Url,
    subscription_id: &str,
    secret: &str,
    previous_secret: Option<&str>,
    id: &str,
    body: &str,
) -> Result<Response, WebhookError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(WebhookError::InvalidRequest);
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WebhookError::InvalidRequest)?
        .as_secs()
        .to_string();
    let headers = signed_headers(
        subscription_id,
        secret,
        previous_secret,
        id,
        &timestamp,
        body,
    )?;
    client
        .post(url.clone())
        .headers(headers)
        .body(body.to_owned())
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                WebhookError::Timeout
            } else {
                WebhookError::Network
            }
        })
}

async fn check_challenge(mut response: Response, expected: &str) -> Result<(), WebhookError> {
    if !response.status().is_success() {
        return Err(WebhookError::ChallengeFailed);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        if error.is_timeout() {
            WebhookError::Timeout
        } else {
            WebhookError::ChallengeFailed
        }
    })? {
        if body.len() + chunk.len() > MAX_CHALLENGE_RESPONSE_BYTES {
            return Err(WebhookError::ChallengeFailed);
        }
        body.extend_from_slice(&chunk);
    }
    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|_| WebhookError::ChallengeFailed)?;
    let actual = value
        .get("challenge")
        .and_then(|v| v.as_str())
        .ok_or(WebhookError::ChallengeFailed)?;
    if bool::from(actual.as_bytes().ct_eq(expected.as_bytes())) {
        Ok(())
    } else {
        Err(WebhookError::ChallengeFailed)
    }
}

fn classify_status(status: StatusCode) -> DeliveryOutcome {
    if status.is_success() {
        DeliveryOutcome::Delivered
    } else if status == StatusCode::GONE {
        DeliveryOutcome::Gone
    } else if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        DeliveryOutcome::Retryable
    } else {
        DeliveryOutcome::PermanentFailure
    }
}

fn random_token() -> String {
    URL_SAFE_NO_PAD.encode(rand::rng().random::<[u8; 32]>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const SECRET: &str = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    #[test]
    fn standard_webhooks_signature_matches_independent_hmac_vector() {
        let body = r#"{"eventId":"evt_stable","data":{"message":"日本語"}}"#;
        assert_eq!(
            signature(SECRET, "evt_stable", "1780000000", body).unwrap(),
            "v1,tJMrgmAyL0VE/Lb2RxHfbEZ4sTLV0TgtDrFcsUzzjlM="
        );
        assert_ne!(
            signature(SECRET, "evt_stable", "1780000001", body).unwrap(),
            signature(SECRET, "evt_stable", "1780000000", body).unwrap()
        );
        assert_ne!(
            signature(SECRET, "evt_stable", "1780000000", "{}").unwrap(),
            signature(SECRET, "evt_stable", "1780000000", body).unwrap()
        );
    }

    #[test]
    fn rotation_emits_two_space_separated_signatures() {
        let previous = format!("whsec_{}", STANDARD.encode([42; 32]));
        let headers = signed_headers(
            "sub_1",
            SECRET,
            Some(&previous),
            "evt_1",
            "1780000000",
            "{}",
        )
        .unwrap();
        let signatures = headers["webhook-signature"].to_str().unwrap();
        assert_eq!(
            signatures,
            format!(
                "{} {}",
                signature(SECRET, "evt_1", "1780000000", "{}").unwrap(),
                signature(&previous, "evt_1", "1780000000", "{}").unwrap()
            )
        );
        let same =
            signed_headers("sub_1", SECRET, Some(SECRET), "evt_1", "1780000000", "{}").unwrap();
        assert_eq!(
            same["webhook-signature"]
                .to_str()
                .unwrap()
                .split(' ')
                .count(),
            1
        );
    }

    #[test]
    fn signing_secret_bounds_are_enforced() {
        for len in [24, 32, 64] {
            assert!(validate_secret(&format!("whsec_{}", STANDARD.encode(vec![1; len]))).is_ok());
        }
        for len in [0, 23, 65] {
            assert_eq!(
                validate_secret(&format!("whsec_{}", STANDARD.encode(vec![1; len]))),
                Err(WebhookError::InvalidSecret)
            );
        }
        for invalid in ["plain_secret", "whsec_!notbase64", "whsec_"] {
            assert_eq!(validate_secret(invalid), Err(WebhookError::InvalidSecret));
        }
    }

    #[test]
    fn non_public_addresses_and_ipv6_transition_ranges_are_rejected() {
        for address in [
            "0.1.2.3",
            "10.1.2.3",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:8.8.8.8",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
            "fc00::1",
            "fe80::1",
            "ff02::1",
            "2001::1",
            "2001:db8::1",
            "2002:7f00:1::1",
            "3fff::1",
        ] {
            assert!(!is_public_address(address.parse().unwrap()), "{address}");
        }
        for address in [
            "8.8.8.8",
            "1.1.1.1",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.255.255",
            "172.32.0.1",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(is_public_address(address.parse().unwrap()), "{address}");
        }
    }

    #[test]
    fn mixed_dns_answers_are_rejected_before_connecting() {
        assert_eq!(
            validate_addresses(&[
                "8.8.8.8:443".parse().unwrap(),
                "127.0.0.1:443".parse().unwrap()
            ]),
            Err(WebhookError::NonPublicAddress)
        );
        assert_eq!(validate_addresses(&[]), Err(WebhookError::Dns));
        assert!(
            validate_addresses(&[
                "8.8.8.8:443".parse().unwrap(),
                "[2606:4700:4700::1111]:443".parse().unwrap()
            ])
            .is_ok()
        );
    }

    #[test]
    fn callback_url_validation_blocks_unsafe_forms() {
        for url in [
            "http://example.com/secret",
            "ftp://example.com/secret",
            "https://name:secret@example.com",
            "https://example.com/#secret",
            "https://127.1/secret",
            "https://2130706433/secret",
            "https://[::ffff:127.0.0.1]/secret",
            "https://10.1.2.3/secret",
        ] {
            assert!(callback_url_checked(url).is_err(), "{url}");
        }
        assert!(callback_url_checked("https://example.com/path?token=secret").is_ok());
    }

    #[test]
    fn delivery_status_classification_stops_on_redirect_and_410_413() {
        for status in [200, 201, 204] {
            assert_eq!(
                classify_status(StatusCode::from_u16(status).unwrap()),
                DeliveryOutcome::Delivered
            );
        }
        assert_eq!(classify_status(StatusCode::GONE), DeliveryOutcome::Gone);
        for status in [301, 302, 307, 400, 401, 403, 404, 413] {
            assert_eq!(
                classify_status(StatusCode::from_u16(status).unwrap()),
                DeliveryOutcome::PermanentFailure
            );
        }
        for status in [408, 429, 500, 502, 503, 504] {
            assert_eq!(
                classify_status(StatusCode::from_u16(status).unwrap()),
                DeliveryOutcome::Retryable
            );
        }
    }

    #[test]
    fn verification_cache_is_bounded_expiring_and_principal_scoped() {
        let client = WebhookClient::new();
        client.cache_verification("owner_a", "https://example.com/secret");
        assert!(client.verification_cached("owner_a", "https://example.com/secret"));
        assert!(!client.verification_cached("owner_b", "https://example.com/secret"));
        client.verified.lock().unwrap().insert(
            ("expired".into(), "https://example.com/old".into()),
            Instant::now() - VERIFICATION_TTL - Duration::from_secs(1),
        );
        assert!(!client.verification_cached("expired", "https://example.com/old"));
        for n in 0..MAX_VERIFICATIONS + 10 {
            client.cache_verification("owner_a", &format!("https://example.com/{n}"));
        }
        assert_eq!(client.verified.lock().unwrap().len(), MAX_VERIFICATIONS);
    }

    // The local HTTP fixture exercises exact signed bytes/echo handling. Production
    // connections always pass through secure_client's HTTPS/public-address policy.
    async fn callback_fixture(
        status: &str,
        response_body: String,
        expected_body_len: usize,
        extra_headers: &str,
    ) -> (Url, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/callback",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n{extra_headers}\r\n{response_body}",
            response_body.len()
        );
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let len = stream.read(&mut chunk).await.unwrap();
                assert!(len > 0);
                request.extend_from_slice(&chunk[..len]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    && request.len() >= end + 4 + expected_body_len
                {
                    break;
                }
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (url, task)
    }

    fn fixture_client() -> Client {
        Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn signed_verification_posts_required_headers_and_compares_echo() {
        let challenge = random_token();
        let body = serde_json::json!({"type":"verification", "challenge":challenge}).to_string();
        let (url, task) = callback_fixture(
            "200 OK",
            serde_json::json!({"challenge":challenge}).to_string(),
            body.len(),
            "",
        )
        .await;
        let response = post_signed(
            &fixture_client(),
            &url,
            "sub_1",
            SECRET,
            None,
            "msg_verification_1",
            &body,
        )
        .await
        .unwrap();
        check_challenge(response, &challenge).await.unwrap();
        let request = task.await.unwrap();
        assert!(request.contains("webhook-id: msg_verification_1\r\n"));
        assert!(request.contains("x-mcp-subscription-id: sub_1\r\n"));
        assert!(request.ends_with(&body));
        let timestamp = request
            .lines()
            .find_map(|line| line.strip_prefix("webhook-timestamp: "))
            .unwrap();
        let expected = signature(SECRET, "msg_verification_1", timestamp, &body).unwrap();
        assert!(request.contains(&format!("webhook-signature: {expected}\r\n")));
    }

    #[tokio::test]
    async fn verification_rejects_wrong_echo_non_success_and_oversized_response() {
        for (status, response_body) in [
            ("200 OK", r#"{"challenge":"wrong"}"#.to_owned()),
            ("403 Forbidden", r#"{"challenge":"expected"}"#.to_owned()),
            ("200 OK", "x".repeat(MAX_CHALLENGE_RESPONSE_BYTES + 1)),
        ] {
            let (url, task) = callback_fixture(status, response_body, 2, "").await;
            let response = post_signed(
                &fixture_client(),
                &url,
                "sub_1",
                SECRET,
                None,
                "verification_1",
                "{}",
            )
            .await
            .unwrap();
            assert_eq!(
                check_challenge(response, "expected").await,
                Err(WebhookError::ChallengeFailed)
            );
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn redirect_response_is_not_followed() {
        let (url, task) = callback_fixture(
            "302 Found",
            String::new(),
            2,
            "Location: http://127.0.0.1:1/private\r\n",
        )
        .await;
        let response = post_signed(
            &fixture_client(),
            &url,
            "sub_1",
            SECRET,
            None,
            "evt_1",
            "{}",
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            classify_status(response.status()),
            DeliveryOutcome::PermanentFailure
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn production_paths_reject_private_callbacks_without_leaking_url_or_secret() {
        let client = WebhookClient::new();
        let error = client
            .verify("sub_1", "owner", "https://127.0.0.1/private_token", SECRET)
            .await
            .unwrap_err();
        assert_eq!(error, WebhookError::NonPublicAddress);
        assert!(!format!("{error:?} {error}").contains("private_token"));
        assert!(!format!("{error:?} {error}").contains(SECRET));
        assert_eq!(
            client
                .deliver(
                    "sub_1",
                    "https://[::ffff:127.0.0.1]/private",
                    SECRET,
                    None,
                    "evt_1",
                    "{}"
                )
                .await,
            DeliveryOutcome::PermanentFailure
        );
    }
}
