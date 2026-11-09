//! Generated code for the Oxchange contracts in `/proto`.
//!
//! Module paths mirror the proto packages: `events.v1` is [`events::v1`].
//! Edit the `.proto` files, never this crate's output; `buf breaking` in CI
//! guards compatibility (add fields only, never reuse a field number).

pub mod events {
    pub mod v1 {
        #![allow(clippy::all, clippy::pedantic, missing_docs)]
        tonic::include_proto!("events.v1");
    }
}

pub mod accounts {
    pub mod v1 {
        #![allow(clippy::all, clippy::pedantic, missing_docs)]
        tonic::include_proto!("accounts.v1");
    }
}

pub mod risk {
    pub mod v1 {
        #![allow(clippy::all, clippy::pedantic, missing_docs)]
        tonic::include_proto!("risk.v1");
    }
}

pub mod matching {
    pub mod v1 {
        #![allow(clippy::all, clippy::pedantic, missing_docs)]
        tonic::include_proto!("matching.v1");
    }
}
