//! Double-entry posting and idempotency, inside one database transaction.

use std::collections::BTreeMap;

use prost::Message;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{AccountsError, Result};

pub(crate) type Tx<'a> = Transaction<'a, Postgres>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Kind {
    Available,
    Reserved,
    External,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Available => "available",
            Kind::Reserved => "reserved",
            Kind::External => "external",
        }
    }
}

/// One ledger account: (owner, asset, kind). The external account for an
/// asset has no owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AccountKey {
    pub owner: Option<Uuid>,
    pub asset: String,
    pub kind: Kind,
}

impl AccountKey {
    pub fn user(owner: Uuid, asset: &str, kind: Kind) -> Self {
        Self {
            owner: Some(owner),
            asset: asset.to_owned(),
            kind,
        }
    }
    pub fn external(asset: &str) -> Self {
        Self {
            owner: None,
            asset: asset.to_owned(),
            kind: Kind::External,
        }
    }
}

/// Balances after a posting, by account.
pub(crate) type Balances = BTreeMap<AccountKey, i64>;

pub(crate) fn request_hash(parts: &[&str]) -> Vec<u8> {
    let mut h = Sha256::new();
    for p in parts {
        // Length-prefixed, so ("ab","c") and ("a","bc") differ.
        h.update((p.len() as u64).to_le_bytes());
        h.update(p.as_bytes());
    }
    h.finalize().to_vec()
}

pub(crate) enum Claim<R> {
    /// First use of the key: do the work under this transaction ID.
    New(Uuid),
    /// The key was used before with the same request: here is the first response.
    Replay(R),
}

/// Claims an idempotency key for this transaction.
///
/// A concurrent request with the same key waits on the unique index until
/// the first one commits (then replays its response) or rolls back (then
/// does the work itself).
pub(crate) async fn claim<R: Message + Default>(
    tx: &mut Tx<'_>,
    key: &str,
    kind: &str,
    hash: &[u8],
) -> Result<Claim<R>> {
    let id = Uuid::now_v7();
    let inserted = sqlx::query(
        "INSERT INTO ledger_transactions (id, idempotency_key, kind, request_hash)
         VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind(id)
    .bind(key)
    .bind(kind)
    .bind(hash)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if inserted == 1 {
        return Ok(Claim::New(id));
    }
    let (first_hash, response): (Vec<u8>, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT request_hash, response FROM ledger_transactions WHERE idempotency_key = $1",
    )
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    if first_hash != hash {
        return Err(AccountsError::IdempotencyConflict);
    }
    let response = response.ok_or_else(|| {
        AccountsError::Invariant(format!("transaction {key} committed without a response"))
    })?;
    R::decode(response.as_slice())
        .map(Claim::Replay)
        .map_err(|e| AccountsError::Invariant(format!("stored response for {key}: {e}")))
}

/// Stores the response a retry of this key will get. Call before commit.
pub(crate) async fn finish<R: Message>(tx: &mut Tx<'_>, id: Uuid, response: &R) -> Result<()> {
    sqlx::query("UPDATE ledger_transactions SET response = $2 WHERE id = $1")
        .bind(id)
        .bind(response.encode_to_vec())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Posts `legs` under transaction `txn` and returns every touched account's
/// new balance.
///
/// Legs must add up to zero per asset. A user account that would go
/// negative fails with [`AccountsError::InsufficientFunds`] (the database's
/// CHECK would refuse it anyway). Zero legs are skipped.
pub(crate) async fn post(
    tx: &mut Tx<'_>,
    txn: Uuid,
    legs: &[(AccountKey, i64)],
) -> Result<Balances> {
    let mut by_asset: BTreeMap<&str, i128> = BTreeMap::new();
    let mut delta: Balances = BTreeMap::new();
    for (k, amount) in legs.iter().filter(|(_, a)| *a != 0) {
        *by_asset.entry(&k.asset).or_default() += i128::from(*amount);
        let d = delta.entry(k.clone()).or_default();
        *d = d
            .checked_add(*amount)
            .ok_or_else(|| AccountsError::Invariant("posting overflows".into()))?;
    }
    if let Some((asset, sum)) = by_asset.iter().find(|(_, s)| **s != 0) {
        return Err(AccountsError::Invariant(format!(
            "postings for {asset} add up to {sum}, not zero"
        )));
    }
    if delta.is_empty() {
        return Ok(Balances::new());
    }

    // BTreeMap iteration is sorted, so concurrent transactions create
    // missing accounts in the same order.
    let owners: Vec<Option<Uuid>> = delta.keys().map(|k| k.owner).collect();
    let assets: Vec<String> = delta.keys().map(|k| k.asset.clone()).collect();
    let kinds: Vec<&str> = delta.keys().map(|k| k.kind.as_str()).collect();
    sqlx::query(
        "INSERT INTO ledger_accounts (owner_id, asset, kind)
         SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[])
         ON CONFLICT DO NOTHING",
    )
    .bind(&owners)
    .bind(&assets)
    .bind(&kinds)
    .execute(&mut **tx)
    .await
    .map_err(map_missing_user)?;

    // Lock in ascending ID order: the rule that prevents deadlocks. The
    // inner lookup finds each ID through the (owner_id, asset, kind) index:
    // equality for user accounts, IS NULL for external ones. A join on
    // `IS NOT DISTINCT FROM` cannot use the index and read every account
    // on every posting (half of Accounts' database time under load).
    let rows: Vec<(i64, Option<Uuid>, String, String, i64)> = sqlx::query_as(
        "SELECT a.id, a.owner_id, a.asset, a.kind, a.balance
         FROM ledger_accounts a
         WHERE a.id = ANY(ARRAY(
           SELECT l.id
           FROM UNNEST($1::uuid[], $2::text[], $3::text[]) AS k(owner_id, asset, kind),
           LATERAL (
             SELECT x.id FROM ledger_accounts x
             WHERE x.owner_id = k.owner_id AND x.asset = k.asset AND x.kind = k.kind
             UNION ALL
             SELECT x.id FROM ledger_accounts x
             WHERE k.owner_id IS NULL AND x.owner_id IS NULL
               AND x.asset = k.asset AND x.kind = k.kind
           ) l))
         ORDER BY a.id
         FOR UPDATE OF a",
    )
    .bind(&owners)
    .bind(&assets)
    .bind(&kinds)
    .fetch_all(&mut **tx)
    .await?;
    if rows.len() != delta.len() {
        return Err(AccountsError::Invariant(
            "ledger account lookup mismatch".into(),
        ));
    }

    let mut ids = Vec::with_capacity(rows.len());
    let mut amounts = Vec::with_capacity(rows.len());
    let mut new_balances = Vec::with_capacity(rows.len());
    let mut out = Balances::new();
    for (id, owner, asset, kind, balance) in rows {
        let kind = match kind.as_str() {
            "available" => Kind::Available,
            "reserved" => Kind::Reserved,
            _ => Kind::External,
        };
        let key = AccountKey { owner, asset, kind };
        let d = delta[&key];
        let new = balance
            .checked_add(d)
            .ok_or_else(|| AccountsError::Invariant("balance overflows".into()))?;
        if kind != Kind::External && new < 0 {
            return Err(AccountsError::InsufficientFunds);
        }
        ids.push(id);
        amounts.push(d);
        new_balances.push(new);
        out.insert(key, new);
    }

    sqlx::query(
        "INSERT INTO ledger_entries (transaction_id, account_id, amount)
         SELECT $1, * FROM UNNEST($2::bigint[], $3::bigint[])",
    )
    .bind(txn)
    .bind(&ids)
    .bind(&amounts)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE ledger_accounts a SET balance = v.balance
         FROM UNNEST($1::bigint[], $2::bigint[]) AS v(id, balance) WHERE a.id = v.id",
    )
    .bind(&ids)
    .bind(&new_balances)
    .execute(&mut **tx)
    .await?;
    Ok(out)
}

/// A foreign-key failure on `owner_id` means the user does not exist.
pub(crate) fn map_missing_user(e: sqlx::Error) -> AccountsError {
    match &e {
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23503") => {
            AccountsError::NotFound("user")
        }
        _ => AccountsError::Db(e),
    }
}
