//! Register, login, refresh rotation and logout (spec: "Security").

use std::sync::LazyLock;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use auth::{REFRESH_TOKEN_TTL_SECS, hash_refresh_token, new_refresh_token};
use proto_gen::accounts::v1 as pb;
use uuid::Uuid;

use crate::ledger::Tx;
use crate::{Accounts, AccountsError, Result, now_ns};

const MIN_PASSWORD: usize = 8;
const MAX_PASSWORD: usize = 128;
const MAX_EMAIL: usize = 254;

/// Verified against when the email is unknown, so a login takes about as
/// long whether or not the account exists.
static DUMMY_HASH: LazyLock<String> = LazyLock::new(|| hash_password("not-a-real-password"));

fn hash_password(password: &str) -> String {
    let mut salt = [0u8; 16];
    getrandom_fill(&mut salt);
    let salt = SaltString::encode_b64(&salt).expect("16-byte salt encodes");
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2id with default parameters")
        .to_string()
}

fn verify_password(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash)
        .map(|h| {
            Argon2::default()
                .verify_password(password.as_bytes(), &h)
                .is_ok()
        })
        .unwrap_or(false)
}

fn getrandom_fill(buf: &mut [u8]) {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    OsRng.fill_bytes(buf);
}

fn normalize_email(email: &str) -> Result<String> {
    let email = email.trim().to_lowercase();
    let valid = email.len() <= MAX_EMAIL
        && matches!(email.split_once('@'), Some((local, domain))
            if !local.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.'))
        && !email.chars().any(char::is_whitespace);
    if valid {
        Ok(email)
    } else {
        Err(AccountsError::Validation("email is not valid".into()))
    }
}

fn check_password(password: &str) -> Result<()> {
    let n = password.chars().count();
    if (MIN_PASSWORD..=MAX_PASSWORD).contains(&n) {
        Ok(())
    } else {
        Err(AccountsError::Validation(format!(
            "password must be {MIN_PASSWORD} to {MAX_PASSWORD} characters"
        )))
    }
}

/// Argon2id is deliberately slow; keep it off the async worker threads.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f)
        .await
        .expect("password hashing panicked")
}

impl Accounts {
    pub async fn register(&self, email: &str, password: &str) -> Result<pb::RegisterResponse> {
        let email = normalize_email(email)?;
        check_password(password)?;
        let password = password.to_owned();
        let hash = blocking(move || hash_password(&password)).await;
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(&email)
            .bind(hash)
            .execute(&self.pool)
            .await
            .map_err(|e| match &e {
                sqlx::Error::Database(d) if d.code().as_deref() == Some("23505") => {
                    AccountsError::AlreadyExists("email is already registered")
                }
                _ => AccountsError::Db(e),
            })?;
        Ok(pb::RegisterResponse {
            user_id: id.to_string(),
        })
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<pb::LoginResponse> {
        // A malformed email cannot match an account; fail the same way.
        let email = normalize_email(email).unwrap_or_default();
        let row: Option<(Uuid, String)> =
            sqlx::query_as("SELECT id, password_hash FROM users WHERE email = $1")
                .bind(&email)
                .fetch_optional(&self.pool)
                .await?;
        let password = password.to_owned();
        let (user, hash) = match row {
            Some((id, hash)) => (Some(id), hash),
            None => (None, DUMMY_HASH.clone()),
        };
        let ok = blocking(move || verify_password(&password, &hash)).await;
        let user = match (user, ok) {
            (Some(user), true) => user,
            _ => return Err(AccountsError::Unauthenticated),
        };
        let mut tx = self.pool.begin().await?;
        let tokens = self.issue_tokens(&mut tx, user).await?;
        tx.commit().await?;
        Ok(pb::LoginResponse {
            user_id: user.to_string(),
            tokens: Some(tokens),
        })
    }

    /// Exchanges a refresh token for new tokens. Each refresh token works
    /// once; presenting a used one again revokes every refresh token the
    /// user has, because it means the token was stolen or replayed.
    pub async fn refresh(&self, refresh_token: &str) -> Result<pb::RefreshResponse> {
        let hash = hash_refresh_token(refresh_token);
        let mut tx = self.pool.begin().await?;
        let rotated: Option<(Uuid,)> = sqlx::query_as(
            "UPDATE refresh_tokens SET revoked_at = now()
             WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > now()
             RETURNING user_id",
        )
        .bind(&hash)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((user,)) = rotated {
            let tokens = self.issue_tokens(&mut tx, user).await?;
            tx.commit().await?;
            return Ok(pb::RefreshResponse {
                tokens: Some(tokens),
            });
        }
        tx.rollback().await?;

        let used: Option<(Uuid, bool)> = sqlx::query_as(
            "SELECT user_id, revoked_at IS NOT NULL FROM refresh_tokens WHERE token_hash = $1",
        )
        .bind(&hash)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((user, true)) = used {
            let n = sqlx::query(
                "UPDATE refresh_tokens SET revoked_at = now()
                 WHERE user_id = $1 AND revoked_at IS NULL",
            )
            .bind(user)
            .execute(&self.pool)
            .await?
            .rows_affected();
            tracing::warn!(%user, revoked = n, "refresh token reused; revoked all of the user's refresh tokens");
        }
        Err(AccountsError::Unauthenticated)
    }

    /// Revokes the user's refresh token. Succeeds even if it was already
    /// revoked, so a retried logout is harmless.
    ///
    /// The access token stays valid until it expires (at most 15 minutes)
    /// unless the Gateway adds its ID to the Redis revocation list, which
    /// arrives with the Gateway in Phase 3.
    pub async fn logout(&self, user: &str, refresh_token: &str) -> Result<pb::LogoutResponse> {
        let user = Uuid::parse_str(user)
            .map_err(|_| AccountsError::Validation("user_id must be a UUID".into()))?;
        sqlx::query(
            "UPDATE refresh_tokens SET revoked_at = now()
             WHERE token_hash = $1 AND user_id = $2 AND revoked_at IS NULL",
        )
        .bind(hash_refresh_token(refresh_token))
        .bind(user)
        .execute(&self.pool)
        .await?;
        Ok(pb::LogoutResponse {})
    }

    async fn issue_tokens(&self, tx: &mut Tx<'_>, user: Uuid) -> Result<pb::Tokens> {
        let now_ns = now_ns();
        let now_secs = now_ns / 1_000_000_000;
        let access = self.signer.issue(user, now_secs);
        let (refresh, refresh_hash) = new_refresh_token();
        let refresh_expires = now_secs + REFRESH_TOKEN_TTL_SECS;
        sqlx::query(
            "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(Uuid::now_v7())
        .bind(user)
        .bind(refresh_hash)
        .bind(
            time::OffsetDateTime::from_unix_timestamp(refresh_expires)
                .map_err(|e| AccountsError::Invariant(e.to_string()))?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(pb::Tokens {
            access_token: access.token,
            access_expires_at_ns: access.claims.exp * 1_000_000_000,
            refresh_token: refresh,
            refresh_expires_at_ns: refresh_expires * 1_000_000_000,
        })
    }
}
