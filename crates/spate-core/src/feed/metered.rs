//! The runtime's wrapper around the feed it hands a source.
//!
//! [`MeteredFeed`] forwards every call to the backend unchanged and records
//! the calls that receive, renew, settle, hand back and publish units under
//! `spate_feed_*`, so every backend is counted the same way with no code of
//! its own.

use super::{
    Confirmation, Delivery, DepthOf, DepthReading, Feed, FeedError, FeedErrorKind, FeedWaker,
    Receipt, Unit,
};
use crate::clock::{SharedClock, Stopwatch};
use crate::metrics::{FeedMetrics, FeedOp};
use std::sync::Arc;
use std::time::Duration;

/// A feed that records the unit and publish calls made through it.
///
/// Each call reaches the backend once, with the same arguments, and returns
/// what the backend returned.
pub(crate) struct MeteredFeed {
    inner: Arc<dyn Feed>,
    metrics: Arc<FeedMetrics>,
    clock: SharedClock,
}

impl MeteredFeed {
    /// Wrap `inner`, timing its calls on `clock`.
    pub(crate) fn new(
        inner: Arc<dyn Feed>,
        metrics: FeedMetrics,
        clock: SharedClock,
    ) -> MeteredFeed {
        MeteredFeed {
            inner,
            metrics: Arc::new(metrics),
            clock,
        }
    }

    fn call<T>(
        &self,
        op: FeedOp,
        call: impl FnOnce() -> Result<T, FeedError>,
    ) -> Result<T, FeedError> {
        let watch = Stopwatch::start(&*self.clock);
        let result = call();
        self.metrics.call(op, watch.elapsed(&*self.clock));
        if let Err(e) = &result {
            self.metrics.error(op, e.kind);
        }
        result
    }

    /// Record a call that settles a delivery. The source lets go of it once
    /// the feed accepts the call or answers Gone, and the ack rule has it make
    /// no later call with that receipt, so a plain count stays exact.
    fn settle(
        &self,
        op: FeedOp,
        accepted: fn(&FeedMetrics),
        call: impl FnOnce() -> Result<(), FeedError>,
    ) -> Result<(), FeedError> {
        let result = self.call(op, call);
        match &result {
            Ok(()) => {
                accepted(&self.metrics);
                self.metrics.released();
            }
            Err(e) if e.kind == FeedErrorKind::Gone => self.metrics.released(),
            Err(_) => {}
        }
        result
    }
}

impl Feed for MeteredFeed {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let deliveries = self.call(FeedOp::Receive, || self.inner.receive(max))?;
        self.metrics.received(deliveries.len());
        Ok(deliveries)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        let result = self.call(FeedOp::Renew, || self.inner.renew(receipt));
        match &result {
            Ok(()) => self.metrics.renewed(),
            Err(e) if e.kind == FeedErrorKind::Gone => self.metrics.released(),
            Err(_) => {}
        }
        result
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.settle(FeedOp::Acknowledge, FeedMetrics::acknowledged, || {
            self.inner.acknowledge(receipt)
        })
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.settle(FeedOp::Fail, FeedMetrics::failed, || {
            self.inner.fail(receipt, reason)
        })
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let watch = Stopwatch::start(&*self.clock);
        let confirmation = self.inner.publish(unit);
        self.metrics
            .call(FeedOp::Publish, watch.elapsed(&*self.clock));
        let metrics = Arc::clone(&self.metrics);
        let clock = Arc::clone(&self.clock);
        confirmation.on_settle(move |outcome| {
            metrics.publish_settled(outcome, watch.elapsed(&*clock));
        })
    }

    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        self.inner.read_depth(of)
    }

    fn hands_back(&self) -> bool {
        self.inner.hands_back()
    }

    fn stop_receiving(&self) {
        self.inner.stop_receiving();
    }

    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.settle(FeedOp::HandBack, FeedMetrics::handed_back, || {
            self.inner.hand_back(receipt)
        })
    }

    fn close(&self, within: Duration) -> Option<Confirmation> {
        self.inner.close(within)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::feed::{Confirmer, confirmation};
    use crate::metrics::ComponentLabels;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// A backend that logs every call, answers the calls that return a
    /// `Result` from a script of refusals, and leaves each publish and close
    /// for the test to settle.
    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        refusals: Mutex<VecDeque<FeedErrorKind>>,
        deliveries: Mutex<Vec<Delivery>>,
        confirmers: Mutex<Vec<Confirmer>>,
        // Moved by each renewal and publish, so a test can see what the call
        // took.
        call_takes: Mutex<Option<(Arc<TestClock>, Duration)>>,
    }

    impl Fake {
        fn log(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn answer(&self) -> Result<(), FeedError> {
            match self.refusals.lock().unwrap().pop_front() {
                Some(kind) => Err(FeedError::new(kind, "refused by the fake")),
                None => Ok(()),
            }
        }

        fn refuse(&self, kind: FeedErrorKind) {
            self.refusals.lock().unwrap().push_back(kind);
        }

        fn deliver(&self, receipts: impl IntoIterator<Item = u64>) {
            self.deliveries.lock().unwrap().extend(
                receipts
                    .into_iter()
                    .map(|n| Delivery::new(Unit::new(format!("u{n}"), Vec::new()), Receipt(n), 1)),
            );
        }

        fn take_time(&self) {
            if let Some((clock, by)) = &*self.call_takes.lock().unwrap() {
                clock.advance(*by);
            }
        }

        fn take_confirmer(&self) -> Confirmer {
            self.confirmers.lock().unwrap().remove(0)
        }
    }

    impl Feed for Fake {
        fn backend(&self) -> &str {
            "fake"
        }

        fn set_waker(&self, _waker: FeedWaker) {
            self.log("set_waker".into());
        }

        fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
            self.log(format!("receive {max}"));
            self.answer()?;
            Ok(std::mem::take(&mut *self.deliveries.lock().unwrap()))
        }

        fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("renew {}", receipt.0));
            self.take_time();
            self.answer()
        }

        fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("acknowledge {}", receipt.0));
            self.answer()
        }

        fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
            self.log(format!("fail {} {reason}", receipt.0));
            self.answer()
        }

        fn publish(&self, unit: Unit) -> Confirmation {
            self.log(format!("publish {}", unit.id));
            self.take_time();
            let (confirmer, confirmation) = confirmation();
            self.confirmers.lock().unwrap().push(confirmer);
            confirmation
        }

        fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
            self.log(format!("read_depth {of:?}"));
            None
        }

        fn hands_back(&self) -> bool {
            self.log("hands_back".into());
            true
        }

        fn stop_receiving(&self) {
            self.log("stop_receiving".into());
        }

        fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("hand_back {}", receipt.0));
            self.answer()
        }

        fn close(&self, within: Duration) -> Option<Confirmation> {
            self.log(format!("close {within:?}"));
            let (confirmer, confirmation) = confirmation();
            self.confirmers.lock().unwrap().push(confirmer);
            Some(confirmation)
        }
    }

    /// A metered fake whose handles record into a recorder of the test's
    /// own, under `pipeline`.
    struct Rig {
        feed: MeteredFeed,
        fake: Arc<Fake>,
        clock: Arc<TestClock>,
        recorder: PrometheusRecorder,
        pipeline: &'static str,
    }

    impl Rig {
        fn new(pipeline: &'static str) -> Rig {
            let recorder = PrometheusBuilder::new().build_recorder();
            let fake = Arc::new(Fake::default());
            let clock = TestClock::frozen();
            let feed = metrics::with_local_recorder(&recorder, || {
                let labels = ComponentLabels::new(pipeline, "feed", "fake");
                let metrics = FeedMetrics::try_new(&labels).expect("free series");
                MeteredFeed::new(
                    Arc::clone(&fake) as Arc<dyn Feed>,
                    metrics,
                    Arc::clone(&clock) as SharedClock,
                )
            });
            Rig {
                feed,
                fake,
                clock,
                recorder,
                pipeline,
            }
        }

        /// The rendered value of `name` carrying the standard labels and
        /// then `extra`, such as `,op="renew"`; `None` when absent.
        fn value(&self, name: &str, extra: &str) -> Option<f64> {
            let series = format!(
                "{name}{{pipeline=\"{}\",component=\"feed\",component_type=\"fake\"{extra}}} ",
                self.pipeline
            );
            let text = self.recorder.handle().render();
            text.lines()
                .find_map(|line| line.strip_prefix(&series))
                .map(|value| value.parse().expect("a number"))
        }

        fn counter(&self, name: &str) -> f64 {
            self.value(name, "").unwrap_or(0.0)
        }

        fn errors(&self, op: &str, error_type: &str) -> f64 {
            let extra = format!(",op=\"{op}\",error_type=\"{error_type}\"");
            self.value("spate_feed_errors_total", &extra).unwrap_or(0.0)
        }

        fn held(&self) -> Option<f64> {
            self.value("spate_feed_held_units", "")
        }

        fn calls_timed(&self, op: &str) -> f64 {
            let extra = format!(",op=\"{op}\"");
            self.value("spate_feed_call_duration_seconds_count", &extra)
                .unwrap_or(0.0)
        }
    }

    /// Pins that every call reaches the backend once, with its arguments,
    /// and returns the backend's answer unchanged, including the answers of
    /// methods the trait gives defaults for.
    #[test]
    fn every_call_reaches_the_backend_once_with_its_answer() {
        let rig = Rig::new("metered-forwards");
        let (waker, _park) = crate::feed::wake_channel();
        rig.fake.deliver([7]);

        assert_eq!(rig.feed.backend(), "fake");
        rig.feed.set_waker(waker);
        let delivered = rig.feed.receive(4).unwrap();
        assert_eq!(delivered[0].receipt, Receipt(7));
        rig.fake.refuse(FeedErrorKind::Retryable);
        let refused = rig.feed.renew(Receipt(7)).unwrap_err();
        assert_eq!(
            refused,
            FeedError::new(FeedErrorKind::Retryable, "refused by the fake")
        );
        rig.feed.acknowledge(Receipt(7)).unwrap();
        rig.feed.fail(Receipt(8), "unreadable").unwrap();
        drop(rig.feed.publish(Unit::new("day-1", Vec::new())));
        assert!(rig.feed.read_depth(DepthOf::Feed).is_none());
        assert!(rig.feed.hands_back());
        rig.feed.stop_receiving();
        rig.feed.hand_back(Receipt(9)).unwrap();
        assert!(rig.feed.close(Duration::from_secs(2)).is_some());

        assert_eq!(
            *rig.fake.calls.lock().unwrap(),
            [
                "set_waker",
                "receive 4",
                "renew 7",
                "acknowledge 7",
                "fail 8 unreadable",
                "publish day-1",
                "read_depth Feed",
                "hands_back",
                "stop_receiving",
                "hand_back 9",
                "close 2s",
            ]
        );
    }

    /// Pins that the held gauge counts each delivery from its receipt until
    /// the feed accepts a call that settles it or answers Gone.
    #[test]
    fn the_held_gauge_counts_each_delivery_until_it_settles() {
        let rig = Rig::new("metered-held");
        assert_eq!(rig.held(), Some(0.0));
        assert!(rig.feed.receive(8).unwrap().is_empty());
        assert_eq!(rig.counter("spate_feed_units_received_total"), 0.0);

        rig.fake.deliver(1..=5);
        rig.feed.receive(8).unwrap();
        assert_eq!(rig.counter("spate_feed_units_received_total"), 5.0);
        assert_eq!(rig.held(), Some(5.0));

        rig.feed.acknowledge(Receipt(1)).unwrap();
        rig.feed.renew(Receipt(2)).unwrap();
        assert_eq!(rig.held(), Some(4.0), "a renewal keeps the delivery");
        rig.fake.refuse(FeedErrorKind::Gone);
        rig.feed.renew(Receipt(2)).unwrap_err();
        assert_eq!(rig.held(), Some(3.0));

        rig.feed.fail(Receipt(3), "unreadable").unwrap();
        rig.feed.hand_back(Receipt(4)).unwrap();
        assert_eq!(rig.held(), Some(1.0));

        assert_eq!(rig.counter("spate_feed_units_acknowledged_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_units_failed_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_units_handed_back_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_renewals_total"), 1.0);
        assert_eq!(rig.errors("renew", "gone"), 1.0);
    }

    /// Pins that an answer other than Gone keeps the delivery held and counts
    /// nothing as settled, and that each error lands under its call and
    /// kind.
    #[test]
    fn a_refused_call_keeps_the_delivery_and_counts_its_kind() {
        let rig = Rig::new("metered-refused");
        rig.fake.deliver([1]);
        rig.feed.receive(8).unwrap();

        rig.fake.refuse(FeedErrorKind::Retryable);
        rig.feed.acknowledge(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Refused);
        rig.feed.fail(Receipt(1), "unreadable").unwrap_err();
        rig.fake.refuse(FeedErrorKind::Fatal);
        rig.feed.hand_back(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Retryable);
        rig.feed.renew(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Fatal);
        rig.feed.receive(8).unwrap_err();

        assert_eq!(rig.held(), Some(1.0));
        assert_eq!(rig.errors("acknowledge", "retryable"), 1.0);
        assert_eq!(rig.errors("fail", "refused"), 1.0);
        assert_eq!(rig.errors("hand_back", "fatal"), 1.0);
        assert_eq!(rig.errors("renew", "retryable"), 1.0);
        assert_eq!(rig.errors("receive", "fatal"), 1.0);
        assert_eq!(rig.errors("receive", "retryable"), 0.0);
        for counter in [
            "spate_feed_units_acknowledged_total",
            "spate_feed_units_failed_total",
            "spate_feed_units_handed_back_total",
            "spate_feed_renewals_total",
        ] {
            assert_eq!(rig.counter(counter), 0.0, "{counter}");
        }

        rig.fake.refuse(FeedErrorKind::Gone);
        rig.feed.hand_back(Receipt(1)).unwrap_err();
        assert_eq!(rig.held(), Some(0.0), "Gone lets go of the delivery");
        assert_eq!(rig.counter("spate_feed_units_handed_back_total"), 0.0);
    }

    /// Pins that every recorded call is timed under its own `op`, across the
    /// backend's call, and that a publish dropped unseen counts nothing.
    #[test]
    fn every_call_is_timed_under_its_op() {
        let rig = Rig::new("metered-timed");
        *rig.fake.call_takes.lock().unwrap() =
            Some((Arc::clone(&rig.clock), Duration::from_millis(2)));
        rig.feed.receive(1).unwrap();
        rig.feed.receive(1).unwrap();
        rig.feed.renew(Receipt(1)).unwrap();
        rig.feed.acknowledge(Receipt(1)).unwrap();
        rig.feed.fail(Receipt(2), "x").unwrap();
        rig.feed.hand_back(Receipt(3)).unwrap();
        drop(rig.feed.publish(Unit::new("u", Vec::new())));

        assert_eq!(rig.calls_timed("receive"), 2.0);
        for op in ["renew", "acknowledge", "fail", "hand_back", "publish"] {
            assert_eq!(rig.calls_timed(op), 1.0, "{op}");
        }
        for op in ["renew", "publish"] {
            let extra = format!(",op=\"{op}\"");
            assert_eq!(
                rig.value("spate_feed_call_duration_seconds_sum", &extra),
                Some(0.002),
                "{op}"
            );
        }
        assert_eq!(rig.counter("spate_feed_units_published_total"), 0.0);
    }

    /// Pins that a publish whose backend drops its confirmer unsettled counts
    /// as a retryable publish error, on a check and on an await.
    #[test]
    fn a_publish_the_backend_drops_counts_as_a_retryable_error() {
        let rig = Rig::new("metered-dropped");
        let mut checked = rig.feed.publish(Unit::new("a", Vec::new()));
        drop(rig.fake.take_confirmer());
        let err = checked.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Retryable);
        assert_eq!(rig.errors("publish", "retryable"), 1.0);

        let awaited = rig.feed.publish(Unit::new("b", Vec::new()));
        drop(rig.fake.take_confirmer());
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(awaited).unwrap_err();
        assert_eq!(rig.errors("publish", "retryable"), 2.0);
    }

    /// Pins that a publish is counted once, by outcome, when a check or an
    /// await first finds its confirmation settled, timed on the feed's clock,
    /// and that the caller sees the backend's outcome.
    #[test]
    fn a_publish_is_counted_when_its_confirmation_settles() {
        let rig = Rig::new("metered-publish");
        let mut checked = rig.feed.publish(Unit::new("a", Vec::new()));
        assert!(checked.try_outcome().is_none());
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_count", ""),
            Some(0.0)
        );
        rig.clock.advance(Duration::from_secs(3));
        rig.fake.take_confirmer().confirm();
        assert!(matches!(checked.try_outcome(), Some(Ok(()))));
        assert!(matches!(checked.try_outcome(), Some(Ok(()))));
        assert_eq!(rig.counter("spate_feed_units_published_total"), 1.0);
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_sum", ""),
            Some(3.0)
        );

        let awaited = rig.feed.publish(Unit::new("b", Vec::new()));
        rig.fake
            .take_confirmer()
            .reject(FeedError::new(FeedErrorKind::Refused, "too large"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt.block_on(awaited).unwrap_err();
        assert_eq!(err, FeedError::new(FeedErrorKind::Refused, "too large"));

        assert_eq!(rig.counter("spate_feed_units_published_total"), 1.0);
        assert_eq!(rig.errors("publish", "refused"), 1.0);
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_count", ""),
            Some(2.0)
        );
    }
}
