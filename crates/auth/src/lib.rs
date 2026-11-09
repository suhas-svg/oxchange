//! Login tokens (spec: "Security").
//!
//! - Access tokens are JWTs signed with Ed25519 (`alg: EdDSA`) and expire in
//!   15 minutes. Accounts signs them; the Gateway only needs the public key.
//! - Refresh tokens are random 256-bit values. Only their SHA-256 hash is
//!   stored. They expire in 30 days and work once.
//!
//! Callers pass the current time in, so this crate never reads a clock.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const ACCESS_TOKEN_TTL_SECS: i64 = 15 * 60;
pub const REFRESH_TOKEN_TTL_SECS: i64 = 30 * 24 * 60 * 60;

/// The only header this crate writes or accepts. Checking it exactly rules
/// out algorithm-confusion tricks such as `alg: none`.
const HEADER: &str = r#"{"alg":"EdDSA","typ":"JWT"}"#;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenError {
    #[error("malformed token")]
    Malformed,
    #[error("unsupported token header")]
    BadHeader,
    #[error("bad signature")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("invalid key: {0}")]
    Key(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// The user ID.
    pub sub: Uuid,
    pub iat: i64,
    pub exp: i64,
    /// Token ID, so a logout can revoke this one token before it expires.
    pub jti: Uuid,
}

#[derive(Debug, Clone)]
pub struct AccessToken {
    pub token: String,
    pub claims: Claims,
}

pub struct TokenSigner {
    key: SigningKey,
}

impl TokenSigner {
    pub fn from_pem(pem: &str) -> Result<Self, TokenError> {
        SigningKey::from_pkcs8_pem(pem)
            .map(|key| Self { key })
            .map_err(|e| TokenError::Key(e.to_string()))
    }

    /// A new key pair as (private PKCS#8 PEM, public SPKI PEM).
    pub fn generate_pem() -> Result<(String, String), TokenError> {
        let key = SigningKey::generate(&mut rand_core::OsRng);
        let private = key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| TokenError::Key(e.to_string()))?;
        let public = key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .map_err(|e| TokenError::Key(e.to_string()))?;
        Ok((private.to_string(), public))
    }

    pub fn verifier(&self) -> TokenVerifier {
        TokenVerifier {
            key: self.key.verifying_key(),
        }
    }

    pub fn issue(&self, user: Uuid, now_secs: i64) -> AccessToken {
        let claims = Claims {
            sub: user,
            iat: now_secs,
            exp: now_secs + ACCESS_TOKEN_TTL_SECS,
            jti: Uuid::new_v4(),
        };
        let body = serde_json::to_vec(&claims).expect("claims serialize");
        let signing_input = format!("{}.{}", B64.encode(HEADER), B64.encode(body));
        let sig = self.key.sign(signing_input.as_bytes());
        AccessToken {
            token: format!("{signing_input}.{}", B64.encode(sig.to_bytes())),
            claims,
        }
    }
}

#[derive(Clone)]
pub struct TokenVerifier {
    key: VerifyingKey,
}

impl TokenVerifier {
    pub fn from_pem(pem: &str) -> Result<Self, TokenError> {
        VerifyingKey::from_public_key_pem(pem)
            .map(|key| Self { key })
            .map_err(|e| TokenError::Key(e.to_string()))
    }

    pub fn verify(&self, token: &str, now_secs: i64) -> Result<Claims, TokenError> {
        let mut parts = token.split('.');
        let (Some(h), Some(b), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(TokenError::Malformed);
        };
        let header = B64.decode(h).map_err(|_| TokenError::Malformed)?;
        if header != HEADER.as_bytes() {
            return Err(TokenError::BadHeader);
        }
        let sig: [u8; 64] = B64
            .decode(s)
            .map_err(|_| TokenError::Malformed)?
            .try_into()
            .map_err(|_| TokenError::Malformed)?;
        let signing_input = &token[..h.len() + 1 + b.len()];
        self.key
            .verify_strict(signing_input.as_bytes(), &Signature::from_bytes(&sig))
            .map_err(|_| TokenError::BadSignature)?;
        let body = B64.decode(b).map_err(|_| TokenError::Malformed)?;
        let claims: Claims = serde_json::from_slice(&body).map_err(|_| TokenError::Malformed)?;
        if now_secs >= claims.exp {
            return Err(TokenError::Expired);
        }
        Ok(claims)
    }
}

/// A new refresh token and the hash to store for it.
pub fn new_refresh_token() -> (String, Vec<u8>) {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).expect("OS random source");
    let token = B64.encode(raw);
    let hash = hash_refresh_token(&token);
    (token, hash)
}

pub fn hash_refresh_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> TokenSigner {
        let (private, _) = TokenSigner::generate_pem().unwrap();
        TokenSigner::from_pem(&private).unwrap()
    }

    #[test]
    fn issued_token_verifies_with_the_public_key_until_it_expires() {
        let (private, public) = TokenSigner::generate_pem().unwrap();
        let s = TokenSigner::from_pem(&private).unwrap();
        let v = TokenVerifier::from_pem(&public).unwrap();
        let user = Uuid::new_v4();
        let t = s.issue(user, 1_000);
        let c = v.verify(&t.token, 1_000).unwrap();
        assert_eq!(c, t.claims);
        assert_eq!(c.sub, user);
        assert_eq!(v.verify(&t.token, 1_000 + ACCESS_TOKEN_TTL_SECS - 1), Ok(c));
        assert_eq!(
            v.verify(&t.token, 1_000 + ACCESS_TOKEN_TTL_SECS),
            Err(TokenError::Expired)
        );
    }

    #[test]
    fn tampering_and_foreign_keys_are_rejected() {
        let s = signer();
        let t = s.issue(Uuid::new_v4(), 0).token;
        let v = s.verifier();

        // A different user ID in the body breaks the signature.
        let mut parts: Vec<String> = t.split('.').map(str::to_owned).collect();
        let mut claims: Claims = serde_json::from_slice(&B64.decode(&parts[1]).unwrap()).unwrap();
        claims.sub = Uuid::new_v4();
        parts[1] = B64.encode(serde_json::to_vec(&claims).unwrap());
        assert_eq!(v.verify(&parts.join("."), 0), Err(TokenError::BadSignature));

        // alg:none and other headers are refused before any signature check.
        let none = format!("{}.{}.", B64.encode(r#"{"alg":"none"}"#), parts[1]);
        assert_eq!(v.verify(&none, 0), Err(TokenError::BadHeader));

        assert_eq!(
            signer().verifier().verify(&t, 0),
            Err(TokenError::BadSignature)
        );
        assert_eq!(v.verify("a.b", 0), Err(TokenError::Malformed));
        assert_eq!(v.verify(&format!("{t}.x"), 0), Err(TokenError::Malformed));
    }

    #[test]
    fn refresh_tokens_are_random_and_only_the_hash_is_kept() {
        let (a, ha) = new_refresh_token();
        let (b, hb) = new_refresh_token();
        assert_ne!(a, b);
        assert_eq!(ha, hash_refresh_token(&a));
        assert_ne!(ha, hb);
        assert_eq!(B64.decode(&a).unwrap().len(), 32);
    }
}
