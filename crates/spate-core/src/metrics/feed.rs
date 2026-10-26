//! Feed handles (`spate_feed_*`): what the runtime records about the calls a
//! source makes through its feed.
//!
//! One handle set per pipeline, resolved by the runtime when it wraps the feed
//! it hands the source. Every label takes its values from a fixed set: the
//! recorded feed calls, the [`FeedErrorKind`]s, and the backend's key as the
//! `component_type`.

use super::MetricsError;
use super::labels::ComponentLabels;
use super::names;
use super::ownership::{SeriesClaim, series_key};
use crate::feed::{FeedError, FeedErrorKind};
use metrics::{Counter, Gauge, Histogram};
use std::sync::OnceLock;
use std::time::Duration;

/// A feed call, as the `op` label names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeedOp {
    Receive,
    Renew,
    Acknowledge,
    Fail,
    HandBack,
    Publish,
    Close,
}

impl FeedOp {
    /// Every call, in discriminant order, so `op as usize` indexes a table
    /// built from it.
    const ALL: [FeedOp; 7] = [
        FeedOp::Receive,
        FeedOp::Renew,
        FeedOp::Acknowledge,
        FeedOp::Fail,
        FeedOp::HandBack,
        FeedOp::Publish,
        FeedOp::Close,
    ];

    fn label(self) -> &'static str {
        match self {
            FeedOp::Receive => "receive",
            FeedOp::Renew => "renew",
            FeedOp::Acknowledge => "acknowledge",
            FeedOp::Fail => "fail",
            FeedOp::HandBack => "hand_back",
            FeedOp::Publish => "publish",
            FeedOp::Close => "close",
        }
    }
}

/// The `error_type` label values, in the order [`kind_index`] numbers them.
const ERROR_TYPES: [&str; 4] = ["gone", "refused", "retryable", "fatal"];

fn kind_index(kind: FeedErrorKind) -> usize {
    match kind {
        FeedErrorKind::Gone => 0,
        FeedErrorKind::Refused => 1,
        FeedErrorKind::Retryable => 2,
        FeedErrorKind::Fatal => 3,
    }
}

/// Feed handles (`spate_feed_*`).
#[derive(Debug)]
pub(crate) struct FeedMetrics {
    labels: ComponentLabels,
    received: Counter,
    acknowledged: Counter,
    failed: Counter,
    handed_back: Counter,
    published: Counter,
    renewals: Counter,
    errors: [[Counter; ERROR_TYPES.len()]; FeedOp::ALL.len()],
    call_duration: [Histogram; FeedOp::ALL.len()],
    publish_duration: Histogram,
    held_units: Gauge,
    // Registered by the first reading, so the series is absent until a depth
    // has been measured rather than reading as an empty destination.
    dead_letter_units: OnceLock<Gauge>,
    _claim: SeriesClaim,
}

impl FeedMetrics {
    /// Resolve the feed handles, failing when another live handle set already
    /// owns the series.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub(crate) fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(series_key("feed", labels, ""))?;
        let held_units = labels.gauge(names::FEED_HELD_UNITS);
        held_units.set(0.0);
        Ok(FeedMetrics {
            labels: labels.clone(),
            received: labels.counter(names::FEED_UNITS_RECEIVED_TOTAL),
            acknowledged: labels.counter(names::FEED_UNITS_ACKNOWLEDGED_TOTAL),
            failed: labels.counter(names::FEED_UNITS_FAILED_TOTAL),
            handed_back: labels.counter(names::FEED_UNITS_HANDED_BACK_TOTAL),
            published: labels.counter(names::FEED_UNITS_PUBLISHED_TOTAL),
            renewals: labels.counter(names::FEED_RENEWALS_TOTAL),
            errors: FeedOp::ALL.map(|op| {
                ERROR_TYPES.map(|error_type| {
                    labels.counter2(
                        names::FEED_ERRORS_TOTAL,
                        names::L_OP,
                        op.label(),
                        names::L_ERROR_TYPE,
                        error_type,
                    )
                })
            }),
            call_duration: FeedOp::ALL.map(|op| {
                labels.histogram1(names::FEED_CALL_DURATION_SECONDS, names::L_OP, op.label())
            }),
            publish_duration: labels.histogram(names::FEED_PUBLISH_DURATION_SECONDS),
            held_units,
            dead_letter_units: OnceLock::new(),
            _claim: claim,
        })
    }

    pub(crate) fn received(&self, units: usize) {
        self.received.increment(units as u64);
    }

    pub(crate) fn acknowledged(&self) {
        self.acknowledged.increment(1);
    }

    pub(crate) fn failed(&self) {
        self.failed.increment(1);
    }

    pub(crate) fn handed_back(&self) {
        self.handed_back.increment(1);
    }

    pub(crate) fn renewed(&self) {
        self.renewals.increment(1);
    }

    pub(crate) fn error(&self, op: FeedOp, kind: FeedErrorKind) {
        self.errors[op as usize][kind_index(kind)].increment(1);
    }

    pub(crate) fn call(&self, op: FeedOp, elapsed: Duration) {
        self.call_duration[op as usize].record(elapsed);
    }

    /// Record a publish that settled `elapsed` after it was made.
    pub(crate) fn publish_settled(&self, outcome: &Result<(), FeedError>, elapsed: Duration) {
        self.publish_duration.record(elapsed);
        match outcome {
            Ok(()) => self.published.increment(1),
            Err(e) => self.error(FeedOp::Publish, e.kind),
        }
    }

    pub(crate) fn set_held(&self, units: usize) {
        self.held_units.set(units as f64);
    }

    pub(crate) fn set_dead_letters(&self, units: u64) {
        self.dead_letter_units
            .get_or_init(|| self.labels.gauge(names::FEED_DEAD_LETTER_UNITS))
            .set(units as f64);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::metrics::render_local;

    /// Pins that `ALL` lists the calls in discriminant order, which the
    /// handle tables are indexed by.
    #[test]
    fn every_feed_op_sits_at_its_discriminant() {
        for (i, op) in FeedOp::ALL.into_iter().enumerate() {
            assert_eq!(op as usize, i, "{op:?}");
        }
    }

    /// Pins the `op` and `error_type` label each error lands under.
    #[test]
    fn an_error_is_counted_under_its_call_and_kind() {
        let text = render_local(|| {
            let labels = ComponentLabels::new("feed-error-labels", "feed", "memory");
            let metrics = FeedMetrics::try_new(&labels).unwrap();
            let kinds = [
                FeedErrorKind::Gone,
                FeedErrorKind::Refused,
                FeedErrorKind::Retryable,
                FeedErrorKind::Fatal,
            ];
            for (n, kind) in kinds.into_iter().enumerate() {
                for _ in 0..=n {
                    metrics.error(FeedOp::HandBack, kind);
                }
            }
        });
        for (n, error_type) in ERROR_TYPES.into_iter().enumerate() {
            let series = format!(
                "spate_feed_errors_total{{pipeline=\"feed-error-labels\",component=\"feed\",\
                 component_type=\"memory\",op=\"hand_back\",error_type=\"{error_type}\"}} {}",
                n + 1
            );
            assert!(text.contains(&series), "missing {series} in:\n{text}");
        }
    }

    /// Pins that the dead-letter gauge is absent until a reading is
    /// published, and that the held gauge reads 0 from the start.
    #[test]
    fn the_dead_letter_gauge_is_absent_until_measured() {
        let labels = ComponentLabels::new("feed-dead-letters-absent", "feed", "memory");
        let unmeasured = render_local(|| drop(FeedMetrics::try_new(&labels).unwrap()));
        assert!(!unmeasured.contains("spate_feed_dead_letter_units"));
        assert!(unmeasured.contains("spate_feed_held_units{"));

        let measured = render_local(|| {
            FeedMetrics::try_new(&labels).unwrap().set_dead_letters(3);
        });
        assert!(
            measured.contains(
                "spate_feed_dead_letter_units{pipeline=\"feed-dead-letters-absent\",\
                 component=\"feed\",component_type=\"memory\"} 3"
            ),
            "{measured}"
        );
    }

    /// Pins that a second handle set on the same labels is refused, so the
    /// held and dead-letter gauges have one owner (INV-10).
    #[test]
    fn a_second_feed_handle_set_is_refused() {
        let labels = ComponentLabels::new("feed-duplicate", "feed", "memory");
        let owner = FeedMetrics::try_new(&labels).unwrap();
        let err = FeedMetrics::try_new(&labels).unwrap_err();
        assert!(matches!(err, MetricsError::DuplicateSeries(_)), "{err}");
        drop(owner);
        FeedMetrics::try_new(&labels).expect("the claim frees with its owner");
    }
}
