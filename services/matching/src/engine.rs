use std::collections::BTreeMap;

use domain::{CommandId, Symbol};
use order_book::{ApplyOutcome, BookEvent, BookState, Command, OrderBook, RestoreError};
use thiserror::Error;

use crate::dedup::DedupWindow;

/// What happened to one input command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The book processed it; its events were appended.
    Applied,
    /// The offset was below `next_offset`: this input was already applied
    /// (for example Kafka redelivered it). Nothing changed.
    AlreadyApplied,
    /// The command ID is in the duplicate memory (the outbox published it
    /// twice). Nothing but the offset changed.
    DuplicateCommand,
    /// A new order reused a resting order's ID; the book ignored it. This means
    /// a duplicate escaped the duplicate memory: count it and alert.
    DuplicateOrderId,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    /// The never-crossed rule broke. The partition must halt: continuing would
    /// publish trades from a corrupt book.
    #[error("book {symbol} crossed after command {command_id}")]
    Crossed {
        symbol: Symbol,
        command_id: CommandId,
    },
}

/// The full state of one partition, as saved in a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineState {
    pub partition: u32,
    /// The offset of the next command to read from `orders.commands`.
    pub next_offset: u64,
    /// Duplicate memory, oldest first.
    pub dedup_ids: Vec<CommandId>,
    /// One per stock, in symbol order.
    pub books: Vec<BookState>,
}

#[derive(Debug, Error)]
pub enum FromStateError {
    #[error("book {symbol}: {source}")]
    Book {
        symbol: Symbol,
        #[source]
        source: RestoreError,
    },
    #[error("book {0} appears twice")]
    DuplicateSymbol(Symbol),
}

/// All books of one partition plus the bookkeeping that makes replay exact:
/// the input offset and the duplicate memory.
///
/// Single-threaded by design (spec: "one single-threaded loop per partition").
#[derive(Debug, Clone)]
pub struct Engine {
    partition: u32,
    books: BTreeMap<Symbol, OrderBook>,
    dedup: DedupWindow,
    next_offset: u64,
    since_snapshot: u64,
}

impl Engine {
    pub fn new(partition: u32, dedup_capacity: usize) -> Self {
        Self {
            partition,
            books: BTreeMap::new(),
            dedup: DedupWindow::new(dedup_capacity),
            next_offset: 0,
            since_snapshot: 0,
        }
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn book(&self, symbol: &Symbol) -> Option<&OrderBook> {
        self.books.get(symbol)
    }

    pub fn dedup(&self) -> &DedupWindow {
        &self.dedup
    }

    /// Applies the command read at `offset` and appends its events to `out`.
    ///
    /// A stock's book is created on its first command. Risk only sends
    /// commands for listed stocks, so the engine needs no listing of its own.
    pub fn process(
        &mut self,
        offset: u64,
        symbol: &Symbol,
        cmd: &Command,
        out: &mut Vec<BookEvent>,
    ) -> Result<ProcessOutcome, EngineError> {
        if offset < self.next_offset {
            return Ok(ProcessOutcome::AlreadyApplied);
        }
        // Kafka offsets may skip numbers (transaction markers), so only order matters.
        self.next_offset = offset + 1;
        self.since_snapshot += 1;

        if !self.dedup.insert(cmd.command_id) {
            return Ok(ProcessOutcome::DuplicateCommand);
        }

        // Look up before inserting, so the hot path never allocates a key.
        if !self.books.contains_key(symbol) {
            self.books
                .insert(symbol.clone(), OrderBook::new(symbol.clone()));
        }
        let book = self.books.get_mut(symbol).expect("inserted above");
        let outcome = book.apply(cmd, out);
        if book.is_crossed() {
            return Err(EngineError::Crossed {
                symbol: symbol.clone(),
                command_id: cmd.command_id,
            });
        }
        Ok(match outcome {
            ApplyOutcome::Applied => ProcessOutcome::Applied,
            ApplyOutcome::IgnoredDuplicateOrderId => ProcessOutcome::DuplicateOrderId,
        })
    }

    /// Records that the input at `offset` was consumed without applying it
    /// (it could not be decoded and went to the dead-letter queue). Returns
    /// false if that offset was already behind the engine.
    pub fn skip(&mut self, offset: u64) -> bool {
        if offset < self.next_offset {
            return false;
        }
        self.next_offset = offset + 1;
        self.since_snapshot += 1;
        true
    }

    /// Commands read since the last snapshot was taken (or since start).
    pub fn commands_since_snapshot(&self) -> u64 {
        self.since_snapshot
    }

    /// Captures the full state and resets the snapshot counter.
    pub fn take_snapshot(&mut self) -> EngineState {
        self.since_snapshot = 0;
        self.state()
    }

    pub fn state(&self) -> EngineState {
        EngineState {
            partition: self.partition,
            next_offset: self.next_offset,
            dedup_ids: self.dedup.ids().copied().collect(),
            books: self.books.values().map(OrderBook::export).collect(),
        }
    }

    pub fn from_state(state: EngineState, dedup_capacity: usize) -> Result<Self, FromStateError> {
        let mut books = BTreeMap::new();
        for b in state.books {
            let symbol = b.symbol.clone();
            let book = OrderBook::restore(b).map_err(|source| FromStateError::Book {
                symbol: symbol.clone(),
                source,
            })?;
            if books.insert(symbol.clone(), book).is_some() {
                return Err(FromStateError::DuplicateSymbol(symbol));
            }
        }
        Ok(Self {
            partition: state.partition,
            books,
            dedup: DedupWindow::from_ids(dedup_capacity, state.dedup_ids),
            next_offset: state.next_offset,
            since_snapshot: 0,
        })
    }
}
