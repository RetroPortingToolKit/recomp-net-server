//! Short-lived JWT session helpers for optional client authentication.
//!
//! Signing material comes **only** from [`Config`](crate::config::Config) —
//! never from literals in this file. See `docs/SECURITY.md`.

use crate::config::Config;
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("JWT signing is not configured (set JWT_SECRET_CURRENT)")]
    SigningNotConfigured,
    #[error("JWT encode failed: {0}")]
    Encode(#[from] jsonwebtoken::errors::Error),
    #[error("invalid or expired token")]
    InvalidToken,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sub: String,
    pub iat: i64,
    pub exp: i64,
}

const DEFAULT_TTL_SECS: i64 = 45 * 60;

pub fn issue_session_token(config: &Config, player_id: &Uuid) -> Result<String, AuthError> {
    let secret = config
        .jwt_secret_current
        .as_deref()
        .ok_or(AuthError::SigningNotConfigured)?;

    let now = Utc::now();
    let claims = SessionClaims {
        sub: player_id.to_string(),
        iat: now.timestamp(),
        exp: (now + Duration::seconds(DEFAULT_TTL_SECS)).timestamp(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(AuthError::from)
}

pub fn verify_session_token(config: &Config, token: &str) -> Result<SessionClaims, AuthError> {
    let keys = config.jwt_verification_keys();
    if keys.is_empty() {
        return Err(AuthError::SigningNotConfigured);
    }

    let mut validation = Validation::default();
    validation.validate_exp = true;

    for key in keys {
        if let Ok(data) = decode::<SessionClaims>(
            token,
            &DecodingKey::from_secret(key.as_bytes()),
            &validation,
        ) {
            return Ok(data.claims);
        }
    }
    Err(AuthError::InvalidToken)
}
