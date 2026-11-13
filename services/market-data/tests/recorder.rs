//! The recorder: public trades, 1-minute candles, and the Redis ticker.

mod common;

use common::{SEC, T0, env, other, trade};

#[tokio::test]
async fn trades_build_candles_and_the_ticker_and_repeats_change_nothing() {
    let e = env().await;
    // Minute 14:00 gets three trades, 14:01 one.
    let events = [
        other("ACME", 1),
        trade("ACME", 2, T0, 1000, 10),
        trade("ACME", 3, T0 + 20 * SEC, 1010, 5),
        trade("ACME", 4, T0 + 40 * SEC, 990, 1),
        trade("ACME", 5, T0 + 70 * SEC, 1005, 2),
    ];
    for ev in &events {
        e.recorder.apply_event(ev).await.unwrap();
    }
    // Kafka redelivers all of it.
    for ev in &events {
        e.recorder.apply_event(ev).await.unwrap();
    }

    assert_eq!(e.count("SELECT count(*) FROM market_trades").await, 4);
    let candles: Vec<(i64, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT open_cents, high_cents, low_cents, close_cents, volume FROM candles_1m
         WHERE symbol = 'ACME' ORDER BY bucket_start",
    )
    .fetch_all(&e.pool)
    .await
    .unwrap();
    assert_eq!(
        candles,
        vec![(1000, 1010, 990, 990, 16), (1005, 1005, 1005, 1005, 2)]
    );
    let minute: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM bucket_start)::bigint FROM candles_1m ORDER BY bucket_start LIMIT 1",
    )
    .fetch_one(&e.pool)
    .await
    .unwrap();
    assert_eq!(minute, T0 / SEC - 5, "buckets start on the minute");

    let tick = e.ticker.read("ACME").await.unwrap().unwrap();
    assert_eq!(
        (tick.last_price_cents, tick.day_volume, tick.seq),
        (1005, 18, 5)
    );
    // A late write from an older trade never moves the price back.
    let stale = ticker::Tick {
        last_price_cents: 1,
        day_volume: 1,
        seq: 3,
        traded_at_ns: T0,
    };
    assert!(!e.ticker.write("ACME", stale).await.unwrap());
    assert_eq!(e.ticker.read("ACME").await.unwrap(), Some(tick));
}

/// The consume loop hands the recorder every waiting message at once: the
/// result matches one event at a time, across stocks, with repeats and a
/// gap inside the batch.
#[tokio::test]
async fn a_batch_records_the_same_as_single_events() {
    let single = env().await;
    let batched = env().await;
    let events = vec![
        other("ACME", 1),
        trade("ACME", 2, T0, 1000, 10),
        trade("BOLT", 1, T0, 2500, 3),
        trade("ACME", 2, T0, 1000, 10),
        trade("ACME", 3, T0 + 20 * SEC, 1010, 5),
        // ACME 4 and 5 are missing: logged, and recording carries on.
        trade("ACME", 6, T0 + 70 * SEC, 1005, 2),
        trade("BOLT", 2, T0 + 5 * SEC, 2490, 4),
        trade("BOLT", 1, T0, 2500, 3),
    ];
    for ev in &events {
        single.recorder.apply_event(ev).await.unwrap();
    }
    batched.recorder.apply_events(&events).await.unwrap();

    for e in [&single, &batched] {
        assert_eq!(e.count("SELECT count(*) FROM market_trades").await, 5);
    }
    let candles = |pool: sqlx::PgPool| async move {
        sqlx::query_as::<_, (String, i64, i64, i64, i64, i64)>(
            "SELECT symbol, open_cents, high_cents, low_cents, close_cents, volume
             FROM candles_1m ORDER BY symbol, bucket_start",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    };
    assert_eq!(
        candles(single.pool.clone()).await,
        candles(batched.pool.clone()).await
    );
    for symbol in ["ACME", "BOLT"] {
        assert_eq!(
            single.ticker.read(symbol).await.unwrap(),
            batched.ticker.read(symbol).await.unwrap(),
            "{symbol} ticker"
        );
    }
    let progress = |pool: sqlx::PgPool| async move {
        sqlx::query_as::<_, (String, i64)>(
            "SELECT symbol, last_seq FROM consumer_progress ORDER BY symbol",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    };
    let p = progress(batched.pool.clone()).await;
    assert_eq!(p, vec![("ACME".into(), 6), ("BOLT".into(), 2)]);
    assert_eq!(progress(single.pool.clone()).await, p);
}

/// Spec failure test "Redis wipe": reduced mode, then full recovery.
#[tokio::test]
async fn a_redis_wipe_is_refilled_from_the_recorded_trades() {
    let e = env().await;
    e.recorder
        .apply_event(&trade("ACME", 1, T0, 1000, 10))
        .await
        .unwrap();
    e.recorder
        .apply_event(&trade("BOLT", 1, T0, 2500, 3))
        .await
        .unwrap();
    let before = (
        e.ticker.read("ACME").await.unwrap(),
        e.ticker.read("BOLT").await.unwrap(),
    );
    assert!(before.0.is_some() && before.1.is_some());

    e.wipe_redis().await;
    assert_eq!(e.ticker.read("ACME").await.unwrap(), None);
    assert_eq!(e.recorder.refresh_tickers().await.unwrap(), 2);
    assert_eq!(
        (
            e.ticker.read("ACME").await.unwrap(),
            e.ticker.read("BOLT").await.unwrap()
        ),
        before
    );
    // Nothing newer to write the second time.
    assert_eq!(e.recorder.refresh_tickers().await.unwrap(), 0);
}

#[tokio::test]
async fn trades_record_while_redis_is_down() {
    let e = env().await;
    // A store pointed at nothing: every Redis call fails fast.
    let down = std::sync::Arc::new(ticker::Store::new("redis://127.0.0.1:1").unwrap());
    let recorder = market_data::Recorder::new(e.pool.clone(), down.clone());
    recorder
        .apply_event(&trade("ACME", 1, T0, 1000, 10))
        .await
        .unwrap();
    assert!(down.read("ACME").await.is_err());
    assert_eq!(e.count("SELECT count(*) FROM market_trades").await, 1);
    // Back in full mode, the refresh fills the ticker in.
    assert_eq!(e.recorder.refresh_tickers().await.unwrap(), 1);
    assert_eq!(
        e.ticker
            .read("ACME")
            .await
            .unwrap()
            .unwrap()
            .last_price_cents,
        1000
    );
}

#[tokio::test]
async fn a_seq_gap_is_logged_and_recording_continues() {
    let e = env().await;
    e.recorder
        .apply_event(&trade("ACME", 1, T0, 1000, 10))
        .await
        .unwrap();
    e.recorder
        .apply_event(&trade("ACME", 5, T0 + SEC, 1001, 1))
        .await
        .unwrap();
    assert_eq!(e.count("SELECT count(*) FROM market_trades").await, 2);
    assert_eq!(
        e.count("SELECT last_seq FROM consumer_progress WHERE symbol = 'ACME'")
            .await,
        5
    );
    // The skipped seqs arriving late are repeats now.
    e.recorder
        .apply_event(&trade("ACME", 3, T0, 999, 1))
        .await
        .unwrap();
    assert_eq!(e.count("SELECT count(*) FROM market_trades").await, 2);
}
