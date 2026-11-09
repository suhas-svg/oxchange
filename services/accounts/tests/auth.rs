//! Register, login, refresh rotation and logout against a real PostgreSQL.

mod common;

use accounts::AccountsError;
use common::db;

const PW: &str = "correct horse battery";

fn unauthenticated<T: std::fmt::Debug>(r: Result<T, AccountsError>) -> bool {
    matches!(r, Err(AccountsError::Unauthenticated))
}

#[tokio::test]
async fn register_and_login_issue_tokens_for_that_user() {
    let d = db().await;
    let a = &d.accounts;
    let id = a.register(" Ann@Example.com ", PW).await.unwrap().user_id;
    assert!(matches!(
        a.register("ann@example.com", PW).await,
        Err(AccountsError::AlreadyExists(_))
    ));
    for (email, pw) in [
        ("ann@example.com", "wrong password"),
        ("nobody@example.com", PW),
        ("not-an-email", PW),
    ] {
        assert!(unauthenticated(a.login(email, pw).await), "{email}");
    }
    let login = a.login("ANN@example.com", PW).await.unwrap();
    assert_eq!(login.user_id, id);
    let tokens = login.tokens.unwrap();
    let now = tokens.access_expires_at_ns / 1_000_000_000 - 1;
    let claims = a
        .token_verifier()
        .verify(&tokens.access_token, now)
        .unwrap();
    assert_eq!(claims.sub.to_string(), id);
    assert!(tokens.refresh_expires_at_ns > tokens.access_expires_at_ns);

    // Only an Argon2id hash is stored, never the password.
    let (stored,): (String,) = sqlx::query_as("SELECT password_hash FROM users")
        .fetch_one(a.pool())
        .await
        .unwrap();
    assert!(stored.starts_with("$argon2id$") && !stored.contains(PW));
}

#[tokio::test]
async fn bad_emails_and_passwords_are_refused() {
    let d = db().await;
    for (email, pw) in [
        ("a@b", PW),
        ("@b.com", PW),
        ("a b@c.com", PW),
        ("a@b.com", "short"),
    ] {
        assert!(
            matches!(
                d.accounts.register(email, pw).await,
                Err(AccountsError::Validation(_))
            ),
            "{email} / {pw}"
        );
    }
}

#[tokio::test]
async fn a_refresh_token_works_once_and_reuse_revokes_them_all() {
    let d = db().await;
    let a = &d.accounts;
    a.register("bob@example.com", PW).await.unwrap();
    let t1 = a
        .login("bob@example.com", PW)
        .await
        .unwrap()
        .tokens
        .unwrap();
    let other = a
        .login("bob@example.com", PW)
        .await
        .unwrap()
        .tokens
        .unwrap();

    let t2 = a.refresh(&t1.refresh_token).await.unwrap().tokens.unwrap();
    assert_ne!(t2.refresh_token, t1.refresh_token);
    // Reusing t1 means it leaked: refuse it and revoke every session.
    assert!(unauthenticated(a.refresh(&t1.refresh_token).await));
    assert!(unauthenticated(a.refresh(&t2.refresh_token).await));
    assert!(unauthenticated(a.refresh(&other.refresh_token).await));
    assert!(unauthenticated(a.refresh("made-up").await));
}

#[tokio::test]
async fn logout_revokes_the_refresh_token_and_can_be_retried() {
    let d = db().await;
    let a = &d.accounts;
    let id = a.register("cat@example.com", PW).await.unwrap().user_id;
    let t = a
        .login("cat@example.com", PW)
        .await
        .unwrap()
        .tokens
        .unwrap();
    a.logout(&id, &t.refresh_token).await.unwrap();
    a.logout(&id, &t.refresh_token).await.unwrap();
    assert!(unauthenticated(a.refresh(&t.refresh_token).await));
}

#[tokio::test]
async fn an_expired_refresh_token_is_refused() {
    let d = db().await;
    let a = &d.accounts;
    a.register("dan@example.com", PW).await.unwrap();
    let t = a
        .login("dan@example.com", PW)
        .await
        .unwrap()
        .tokens
        .unwrap();
    sqlx::query("UPDATE refresh_tokens SET expires_at = now() - interval '1 second'")
        .execute(a.pool())
        .await
        .unwrap();
    assert!(unauthenticated(a.refresh(&t.refresh_token).await));
}
