//! TLS, bearer authentication and explicit browser-origin policy.
use crate::{EngineError, EngineResult};
use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{path::PathBuf, sync::Arc};
use subtle::ConstantTimeEq;

/// PEM certificate chain and unencrypted PEM private key for the receiving server.
#[derive(Debug, Clone)]
pub struct TlsIdentity {
    /// Certificate chain, with leaf first.
    pub certificate: PathBuf,
    /// Private key matching the leaf certificate.
    pub private_key: PathBuf,
}
/// Security is opt-in for compatibility with private-network HTTP deployments.
/// Bearer tokens and browser access require TLS; certificate checks cannot be disabled.
#[derive(Clone, Default)]
pub struct SecurityConfig {
    /// HTTPS identity for the receiving listener.
    pub identity: Option<TlsIdentity>,
    /// Use HTTPS for all outgoing peer requests. Never falls back to HTTP.
    pub https: bool,
    /// Additional PEM CA/leaf certificates trusted for outgoing connections.
    pub trusted_certificates: Vec<PathBuf>,
    /// Bearer secret required for every incoming HTTP endpoint (at least 32 bytes).
    pub auth_token: Option<String>,
    /// Bearer secret sent to the target peer (requires `https`).
    pub peer_token: Option<String>,
    /// Exact browser origins permitted to call authenticated HTTPS endpoints.
    pub allowed_origins: Vec<String>,
}
impl std::fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecurityConfig")
            .field("identity", &self.identity)
            .field("https", &self.https)
            .field("trusted_certificates", &self.trusted_certificates)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "peer_token",
                &self.peer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}
impl SecurityConfig {
    /// Generate a strong random pairing secret. Share it over a secure channel.
    pub fn generate_token() -> String {
        format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )
    }
    pub(crate) fn validate_server(&self) -> EngineResult<()> {
        if let Some(token) = &self.auth_token {
            if token.len() < 32 || self.identity.is_none() {
                return Err(EngineError::InvalidConfig(
                    "Incoming authentication requires TLS and a token of at least 32 bytes".into(),
                ));
            }
        }
        if !self.allowed_origins.is_empty()
            && (self.auth_token.is_none() || self.identity.is_none())
        {
            return Err(EngineError::InvalidConfig(
                "Browser access requires authenticated HTTPS".into(),
            ));
        }
        for origin in &self.allowed_origins {
            let url = reqwest::Url::parse(origin)
                .map_err(|_| EngineError::InvalidConfig("Invalid browser origin".into()))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.origin().ascii_serialization() != *origin
            {
                return Err(EngineError::InvalidConfig(
                    "Use an exact http(s) origin without a trailing slash or wildcard".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn client(&self) -> EngineResult<reqwest::Client> {
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(30))
            .read_timeout(std::time::Duration::from_secs(60));
        if let Some(token) = &self.peer_token {
            if !self.https || token.len() < 32 {
                return Err(EngineError::InvalidConfig(
                    "Outgoing authentication requires HTTPS and a token of at least 32 bytes"
                        .into(),
                ));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| EngineError::InvalidConfig("Invalid bearer token".into()))?;
            value.set_sensitive(true);
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(header::AUTHORIZATION, value);
            builder = builder.default_headers(headers);
        }
        for path in &self.trusted_certificates {
            let pem = std::fs::read(path).map_err(|e| {
                EngineError::InvalidConfig(format!("Cannot read trusted certificate: {e}"))
            })?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|_| {
                EngineError::InvalidConfig("Invalid trusted certificate PEM".into())
            })?;
            if certs.is_empty() {
                return Err(EngineError::InvalidConfig(
                    "Empty trusted certificate PEM".into(),
                ));
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        builder.build().map_err(EngineError::from)
    }
    pub(crate) async fn server_tls(
        &self,
    ) -> EngineResult<Option<axum_server::tls_rustls::RustlsConfig>> {
        self.validate_server()?;
        let Some(identity) = &self.identity else {
            return Ok(None);
        };
        let cert = tokio::fs::read(&identity.certificate).await?;
        let key = tokio::fs::read(&identity.private_key).await?;
        let certs = rustls_pemfile::certs(&mut &cert[..]).collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut &key[..])?
            .ok_or_else(|| EngineError::InvalidConfig("No private key in PEM".into()))?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| EngineError::InvalidConfig(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| EngineError::InvalidConfig(e.to_string()))?;
        Ok(Some(axum_server::tls_rustls::RustlsConfig::from_config(
            Arc::new(config),
        )))
    }
}
/// Verified pairing principal; a shared token represents one trusted identity.
#[derive(Clone)]
pub(crate) struct Principal(pub String);

pub(crate) async fn authorize(
    State(state): State<Arc<crate::ServerState>>,
    mut request: Request,
    next: Next,
) -> Response {
    use sha2::{Digest, Sha256};
    let config = state.config.read().await.security.clone();
    if config.validate_server().is_err()
        || (config.auth_token.is_some()
            && !state.listener_tls.load(std::sync::atomic::Ordering::SeqCst))
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Invalid live security configuration",
        )
            .into_response();
    }
    let origin = request.headers().get(header::ORIGIN).cloned();
    if let Some(value) = &origin {
        if !value
            .to_str()
            .is_ok_and(|s| config.allowed_origins.iter().any(|o| o == s))
        {
            return (StatusCode::FORBIDDEN, "Browser origin is not allowed").into_response();
        }
    }
    let preflight = request.method() == Method::OPTIONS && origin.is_some();
    let mut response = if preflight {
        StatusCode::NO_CONTENT.into_response()
    } else {
        if let Some(expected) = &config.auth_token {
            let supplied = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.strip_prefix("Bearer "))
                .unwrap_or("");
            let digest = Sha256::digest(supplied.as_bytes());
            if !bool::from(digest.ct_eq(&Sha256::digest(expected.as_bytes()))) {
                let mut response =
                    (StatusCode::UNAUTHORIZED, "Bearer authentication required").into_response();
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
                if let Some(origin) = origin {
                    response
                        .headers_mut()
                        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                    response
                        .headers_mut()
                        .insert(header::VARY, HeaderValue::from_static("Origin"));
                }
                return response;
            }
            request
                .extensions_mut()
                .insert(Principal(format!("token:{digest:x}")));
        }
        next.run(request).await
    };
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(header::VARY, HeaderValue::from_static("Origin"));
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, OPTIONS"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Authorization, Content-Type, X-Transfer-Token"),
        );
    }
    response
}
