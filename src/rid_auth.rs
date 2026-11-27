use crate::error::MissionError;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RidClaims {
    pub sub: String,
    pub iss: String,
    pub aud: Option<serde_json::Value>,
    pub exp: i64,
    pub nbf: Option<i64>,
    pub scope: Option<String>,
    pub client_id: Option<String>,
    pub token_use: Option<String>,
}

#[derive(Clone)]
pub struct RidAuthVerifier {
    jwks_url: Option<String>,
    expected_issuer: Option<String>,
    expected_audience: Option<String>,
    http_client: reqwest::Client,
    cached_keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    last_fetched: Arc<RwLock<Option<Instant>>>,
    cache_ttl: Duration,
}

impl RidAuthVerifier {
    pub fn from_env() -> Self {
        let jwks_url = std::env::var("MW_RID_JWKS_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let expected_issuer = std::env::var("MW_RID_ISSUER")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let expected_audience = std::env::var("MW_RID_AUDIENCE")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            jwks_url,
            expected_issuer,
            expected_audience,
            http_client,
            cached_keys: Arc::new(RwLock::new(HashMap::new())),
            last_fetched: Arc::new(RwLock::new(None)),
            cache_ttl: Duration::from_secs(300), // 5 min TTL
        }
    }

    pub fn new(jwks_url: Option<String>, issuer: Option<String>, audience: Option<String>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            jwks_url: jwks_url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            expected_issuer: issuer.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            expected_audience: audience.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            http_client,
            cached_keys: Arc::new(RwLock::new(HashMap::new())),
            last_fetched: Arc::new(RwLock::new(None)),
            cache_ttl: Duration::from_secs(300),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.jwks_url.is_some()
    }

    pub fn set_key(&self, kid: &str, decoding_key: DecodingKey) {
        if let Ok(mut lock) = self.cached_keys.write() {
            lock.insert(kid.to_string(), decoding_key);
        }
    }

    pub async fn fetch_jwks_if_needed(&self) -> Result<(), MissionError> {
        let url = match &self.jwks_url {
            Some(u) => u.clone(),
            None => return Ok(()),
        };

        let should_fetch = {
            let last = self.last_fetched.read().unwrap();
            match *last {
                Some(inst) => inst.elapsed() > self.cache_ttl,
                None => true,
            }
        };

        if !should_fetch {
            return Ok(());
        }

        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| MissionError::Internal(format!("failed to fetch JWKS: {e}")))?;

        if !resp.status().is_success() {
            return Err(MissionError::Internal(format!(
                "JWKS endpoint returned status {}",
                resp.status()
            )));
        }

        let jwks: JwkSet = resp
            .json()
            .await
            .map_err(|e| MissionError::Internal(format!("invalid JWKS payload: {e}")))?;

        let mut new_keys = HashMap::new();
        for key in jwks.keys {
            if let Some(kid) = &key.common.key_id {
                match DecodingKey::from_jwk(&key) {
                    Ok(decoding_key) => {
                        new_keys.insert(kid.clone(), decoding_key);
                    }
                    Err(err) => {
                        warn!("skipping unsupported JWK key {kid}: {err}");
                    }
                }
            }
        }

        if let Ok(mut keys_lock) = self.cached_keys.write() {
            *keys_lock = new_keys;
        }
        if let Ok(mut last_lock) = self.last_fetched.write() {
            *last_lock = Some(Instant::now());
        }

        Ok(())
    }

    pub async fn verify_token(&self, token: &str) -> Result<RidClaims, MissionError> {
        let header = decode_header(token)
            .map_err(|e| MissionError::Validation(format!("invalid JWT header: {e}")))?;

        // Strict ADR-001: RS256 algorithm only
        if header.alg != Algorithm::RS256 {
            return Err(MissionError::Validation(
                "unsupported JWT algorithm: only RS256 is permitted".to_string(),
            ));
        }

        let kid = header.kid.ok_or_else(|| {
            MissionError::Validation("JWT header missing required 'kid' attribute".to_string())
        })?;

        // 1. Try from cache
        let maybe_key = {
            let keys = self.cached_keys.read().unwrap();
            keys.get(&kid).cloned()
        };

        // 2. Refresh JWKS if missing and URL configured
        let decoding_key = match maybe_key {
            Some(key) => key,
            None => {
                self.fetch_jwks_if_needed().await?;
                let keys = self.cached_keys.read().unwrap();
                keys.get(&kid)
                    .cloned()
                    .ok_or_else(|| MissionError::Validation(format!("signing key not found in JWKS: {kid}")))?
            }
        };

        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;

        if let Some(issuer) = &self.expected_issuer {
            validation.set_issuer(&[issuer]);
        }
        if let Some(audience) = &self.expected_audience {
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }

        let token_data = decode::<RidClaims>(token, &decoding_key, &validation)
            .map_err(|e| MissionError::Validation(format!("JWT validation failed: {e}")))?;

        // Token use validation: Reject id_token, require access token if present
        if let Some(use_claim) = &token_data.claims.token_use {
            if use_claim == "id" {
                return Err(MissionError::Validation(
                    "oidc id tokens are rejected; access token required".to_string(),
                ));
            }
        }

        Ok(token_data.claims)
    }
}

pub fn extract_bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get("Authorization")
        .and_then(|val| val.to_str().ok())
        .and_then(|h| {
            if h.starts_with("Bearer ") {
                Some(h[7..].trim())
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::json;

    #[tokio::test]
    async fn test_rs256_jwks_token_validation() {
        // Generate test RSA key pair
        let mut rng = rand::thread_rng();
        let rsa = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("gen rsa");
        let priv_pem = rsa::pkcs8::EncodePrivateKey::to_pkcs8_pem(&rsa, rsa::pkcs8::LineEnding::LF)
            .expect("priv pem");
        let pub_pem = rsa::pkcs8::EncodePublicKey::to_public_key_pem(
            &rsa.to_public_key(),
            rsa::pkcs8::LineEnding::LF,
        )
        .expect("pub pem");

        let encoding_key = EncodingKey::from_rsa_pem(priv_pem.as_bytes()).expect("enc key");
        let decoding_key = DecodingKey::from_rsa_pem(pub_pem.as_bytes()).expect("dec key");

        let verifier = RidAuthVerifier::new(
            None,
            Some("https://rid.moonwitness.local".to_string()),
            Some("mw-mission".to_string()),
        );
        verifier.set_key("key-1", decoding_key);

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("key-1".to_string());

        let now = Utc::now().timestamp();
        let valid_claims = RidClaims {
            sub: "usr_alice".to_string(),
            iss: "https://rid.moonwitness.local".to_string(),
            aud: Some(json!("mw-mission")),
            exp: now + 3600,
            nbf: Some(now - 10),
            scope: Some("mission:admin".to_string()),
            client_id: Some("mw-portal".to_string()),
            token_use: Some("access".to_string()),
        };

        let token = jsonwebtoken::encode(&header, &valid_claims, &encoding_key).expect("encode");

        let parsed = verifier.verify_token(&token).await.expect("verify valid");
        assert_eq!(parsed.sub, "usr_alice");
        assert_eq!(parsed.scope.as_deref(), Some("mission:admin"));

        // Reject ID token
        let mut id_claims = valid_claims.clone();
        id_claims.token_use = Some("id".to_string());
        let id_token = jsonwebtoken::encode(&header, &id_claims, &encoding_key).expect("encode id");
        let id_err = verifier.verify_token(&id_token).await.expect_err("reject id token");
        assert!(id_err.to_string().contains("access token required"));

        // Reject algorithm mismatch (HS256)
        let hs_header = Header::new(Algorithm::HS256);
        let hs_token = jsonwebtoken::encode(
            &hs_header,
            &valid_claims,
            &EncodingKey::from_secret(b"secret"),
        )
        .expect("encode hs");
        let hs_err = verifier.verify_token(&hs_token).await.expect_err("reject HS256");
        assert!(hs_err.to_string().contains("only RS256 is permitted"));
    }
}
