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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(current: &str, previous: Option<&str>) -> Config {
        Config {
            jwt_secret_current: Some(current.into()),
            jwt_secret_previous: previous.map(Into::into),
            ..Default::default()
        }
    }

    /* jsonwebtoken only compiles its crypto backend in by feature, and with
     * none selected it panics at sign time. This is the test that notices. */
    #[test]
    fn an_issued_token_verifies_to_its_player() {
        let cfg = config("current", None);
        let player = Uuid::new_v4();
        let token = issue_session_token(&cfg, &player).unwrap();
        assert_eq!(
            verify_session_token(&cfg, &token).unwrap().sub,
            player.to_string()
        );
    }

    #[test]
    fn a_rotated_out_secret_verifies_only_while_it_is_previous() {
        let token = issue_session_token(&config("old", None), &Uuid::new_v4()).unwrap();
        assert!(verify_session_token(&config("new", Some("old")), &token).is_ok());
        assert!(verify_session_token(&config("new", None), &token).is_err());
    }

    #[test]
    fn an_expired_token_is_refused() {
        let now = Utc::now().timestamp();
        let claims = SessionClaims {
            sub: Uuid::new_v4().to_string(),
            iat: now - 7200,
            exp: now - 3600,
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"current"),
        )
        .unwrap();
        assert!(matches!(
            verify_session_token(&config("current", None), &token),
            Err(AuthError::InvalidToken)
        ));
    }
}
