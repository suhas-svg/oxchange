//! A fresh PostgreSQL 16 per test (testcontainers), migrated and wrapped in
//! the Accounts service. Needs Docker running.

#![allow(dead_code)]

use std::sync::Arc;

use accounts::audit::{self, When};
use accounts::{Accounts, MIGRATOR, TokenSigner};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use uuid::Uuid;

pub struct Db {
    pub accounts: Arc<Accounts>,
    // Dropping the handle removes the container.
    _container: ContainerAsync<Postgres>,
}

pub async fn db() -> Db {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres (is Docker running?)");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(32)
        .connect(&url)
        .await
        .unwrap();
    MIGRATOR.run(&pool).await.unwrap();
    let (private, _) = TokenSigner::generate_pem().unwrap();
    let signer = TokenSigner::from_pem(&private).unwrap();
    Db {
        accounts: Arc::new(Accounts::new(pool, signer)),
        _container: container,
    }
}

impl Db {
    /// Registers a user and returns its ID.
    pub async fn user(&self, name: &str) -> String {
        self.accounts
            .register(&format!("{name}@example.com"), "correct horse battery")
            .await
            .unwrap()
            .user_id
    }

    pub async fn funded(&self, name: &str, cents: i64) -> String {
        let u = self.user(name).await;
        self.accounts
            .deposit(&u, &format!("seed-{name}"), "USD", cents)
            .await
            .unwrap();
        u
    }

    /// (available, reserved) of one asset.
    pub async fn balance(&self, user: &str, asset: &str) -> (i64, i64) {
        self.accounts
            .balances(user)
            .await
            .unwrap()
            .balances
            .into_iter()
            .find(|b| b.asset == asset)
            .map(|b| (b.available, b.reserved))
            .unwrap_or((0, 0))
    }

    /// Asserts all five Accounts auditor rules hold.
    pub async fn audit(&self) {
        let problems = audit::check(self.accounts.pool(), When::TrafficStopped)
            .await
            .unwrap();
        assert!(problems.is_empty(), "auditor found: {problems:#?}");
    }
}

pub fn order() -> String {
    Uuid::now_v7().to_string()
}
