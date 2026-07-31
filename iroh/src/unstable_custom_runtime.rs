//! Host-provided QUIC timing services.
//!
//! This API is unstable and gated behind the `unstable-custom-runtime` feature.
//! It is not covered by semantic versioning guarantees and may change in any
//! release without a major version bump.

#[cfg(not(wasm_browser))]
pub use std::time::Instant;
use std::{fmt::Debug, pin::Pin};

#[cfg(wasm_browser)]
pub use n0_future::time::Instant;

/// Supplies the monotonic clock and timers used by an endpoint's QUIC runtime.
///
/// Iroh continues to own task spawning, tracing, and endpoint shutdown. A custom
/// time source only replaces the clock and timer implementation used by noq.
///
/// Implementations must return time that never moves backwards. A timer may wake
/// late, but must not wake before its deadline. Resetting a timer must invalidate
/// any wakeup for its previous deadline.
pub trait QuicTimeSource: Send + Sync + Debug + 'static {
    /// Constructs a timer that expires at `deadline`.
    fn new_timer(&self, deadline: Instant) -> Pin<Box<dyn noq::AsyncTimer>>;

    /// Returns the current monotonic time.
    fn now(&self) -> Instant;
}
