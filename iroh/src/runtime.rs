use std::pin::Pin;
#[cfg(feature = "unstable-custom-runtime")]
use std::sync::Arc;
#[cfg(not(wasm_browser))]
use std::time::Instant;

use iroh_base::EndpointId;
#[cfg(wasm_browser)]
use n0_future::time::Instant;
use portable_atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;
#[cfg(not(wasm_browser))]
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub(crate) struct Runtime {
    id: EndpointId,
    #[cfg(feature = "unstable-custom-runtime")]
    time_source: Option<Arc<dyn crate::unstable_custom_runtime::QuicTimeSource>>,
    #[cfg(not(wasm_browser))]
    tasks: TaskTracker,
    #[cfg(not(wasm_browser))]
    cancel: CancellationToken,
    #[cfg(not(wasm_browser))]
    task_counter: AtomicU64,
}

impl Runtime {
    /// Create a new [`Runtime`] that manages shutting down tasks properly,
    /// whether gracefully or un-gracefully.
    pub(crate) fn new(
        id: EndpointId,
        #[cfg(feature = "unstable-custom-runtime")] time_source: Option<
            Arc<dyn crate::unstable_custom_runtime::QuicTimeSource>,
        >,
    ) -> Self {
        Self {
            id,
            #[cfg(feature = "unstable-custom-runtime")]
            time_source,
            #[cfg(not(wasm_browser))]
            tasks: TaskTracker::new(),
            #[cfg(not(wasm_browser))]
            cancel: CancellationToken::new(),
            #[cfg(not(wasm_browser))]
            task_counter: AtomicU64::new(0),
        }
    }

    /// Shutdown the runtime gracefully.
    ///
    /// Closes the task tracker and waits for all spawned tasks to finish naturally.
    #[cfg(not(wasm_browser))]
    pub(crate) async fn shutdown(&self) {
        self.abort();
        // Waits for all tasks to stop (and thus drop all of their futures).
        // If the task tracker had already been closed and tasks have all been cleaned up,
        // this returns immediately.
        self.tasks.wait().await;
    }

    /// Shutdown the runtime ASAP, not waiting for any graceful closing of tasks.
    #[cfg(not(wasm_browser))]
    pub(crate) fn abort(&self) {
        // Signal all spawned tasks to stop immediately.
        self.cancel.cancel();
        // Signal that the runtime should be closed.
        self.tasks.close();
        // Does not wait for the tasks to return.
    }

    /// No-op on wasm. There is no task tracker to close or wait on.
    #[cfg(wasm_browser)]
    pub(crate) async fn shutdown(&self) {}

    /// No-op on wasm. There is no task tracker or cancellation to perform.
    #[cfg(wasm_browser)]
    pub(crate) fn abort(&self) {}
}

impl noq::Runtime for Runtime {
    #[cfg(not(wasm_browser))]
    fn new_timer(&self, i: std::time::Instant) -> Pin<Box<dyn noq::AsyncTimer>> {
        #[cfg(feature = "unstable-custom-runtime")]
        if let Some(time_source) = &self.time_source {
            return time_source.new_timer(i);
        }
        noq::TokioRuntime.new_timer(i)
    }

    #[cfg(wasm_browser)]
    fn new_timer(&self, deadline: n0_future::time::Instant) -> Pin<Box<dyn noq::AsyncTimer>> {
        #[cfg(feature = "unstable-custom-runtime")]
        if let Some(time_source) = &self.time_source {
            return time_source.new_timer(deadline);
        }
        Box::pin(web::Timer(n0_future::time::sleep_until(deadline)))
    }

    #[cfg(not(wasm_browser))]
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        // Do not allow spawning more tasks if the runtime should be closed.
        if self.tasks.is_closed() {
            tracing::debug!(me = %self.id.fmt_short(), "runtime closed, dropping spawned task");
            return;
        }

        use tracing::{Instrument, trace_span};

        let task_id = self.task_counter.fetch_add(1, Ordering::Relaxed);
        let cancel = self.cancel.clone();
        let span = trace_span!("runtime", me = %self.id.fmt_short(), task_id);
        self.tasks.spawn(async move {
            // wrapping the future in a cancellation token is what allows
            // us to "abort" tasks in the event the runtime is meant to
            // close quickly and ungracefully
            cancel.run_until_cancelled(future.instrument(span)).await;
        });
    }

    #[cfg(wasm_browser)]
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        wasm_bindgen_futures::spawn_local(future);
    }

    fn now(&self) -> Instant {
        #[cfg(feature = "unstable-custom-runtime")]
        if let Some(time_source) = &self.time_source {
            return time_source.now();
        }
        Instant::now()
    }

    // We're not actually using this function in iroh
    #[cfg(not(wasm_browser))]
    fn wrap_udp_socket(
        &self,
        t: std::net::UdpSocket,
    ) -> std::io::Result<Box<dyn noq::AsyncUdpSocket>> {
        noq::TokioRuntime.wrap_udp_socket(t)
    }
}

#[cfg(wasm_browser)]
mod web {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use n0_future::time;

    #[derive(Debug)]
    pub(crate) struct Timer(pub(crate) time::Sleep);

    impl noq::AsyncTimer for Timer {
        fn reset(mut self: Pin<&mut Self>, deadline: time::Instant) {
            Pin::new(&mut self.0).reset(deadline)
        }

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            Pin::new(&mut self.0).poll(cx)
        }
    }
}

#[cfg(all(test, feature = "unstable-custom-runtime", not(wasm_browser)))]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use iroh_base::SecretKey;
    use noq::Runtime as _;

    use super::Runtime;
    use crate::unstable_custom_runtime::QuicTimeSource;

    #[derive(Debug)]
    struct TestTimeSource {
        now: Instant,
        timers_created: AtomicUsize,
    }

    impl QuicTimeSource for TestTimeSource {
        fn new_timer(&self, deadline: Instant) -> Pin<Box<dyn noq::AsyncTimer>> {
            self.timers_created.fetch_add(1, Ordering::Relaxed);
            Box::pin(TestTimer { deadline })
        }

        fn now(&self) -> Instant {
            self.now
        }
    }

    #[derive(Debug)]
    struct TestTimer {
        deadline: Instant,
    }

    impl noq::AsyncTimer for TestTimer {
        fn reset(mut self: Pin<&mut Self>, deadline: Instant) {
            self.deadline = deadline;
        }

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    #[test]
    fn custom_time_source_drives_noq_clock_and_timers() {
        let now = Instant::now() + Duration::from_secs(10);
        let time_source = Arc::new(TestTimeSource {
            now,
            timers_created: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(
            SecretKey::generate().public(),
            Some(time_source.clone() as Arc<dyn QuicTimeSource>),
        );

        assert_eq!(runtime.now(), now);
        let _timer = runtime.new_timer(now + Duration::from_secs(1));
        assert_eq!(time_source.timers_created.load(Ordering::Relaxed), 1);
    }
}
