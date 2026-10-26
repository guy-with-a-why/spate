**Breaking:** **The runtime records every work feed call under `spate_feed_*`** (`spate-core`)

When a pipeline's source reads a work feed named by the `feed:` section, the runtime records each call the source makes on the feed, whatever the backend. The `spate_feed_*` families count units received, held, acknowledged, failed, handed back and published, and renewals. They count errors by call and by `FeedErrorKind`, and time each call and each publish's confirmation. The series carry `component="feed"` and the backend's key as `component_type`, and `docs/METRICS.md` describes each one.

`spate_feed_dead_letter_units` reports how many units the feed's dead-letter destination holds. Each instance reads it at startup and then once a minute, whether or not the job ends on its own. On a backend whose depth read is a request to its service, such as SQS, that is one request per instance each minute. An alert on `spate_coordination_splits_quarantined` from v0.3.0 can move to this gauge, aggregated across instances with `max`.

`RuntimeOptions::dead_letter_read_interval` sets how often the runtime reads the depth, and `None` turns those reads off, as a test that scripts or counts a feed's depth reads needs. Code that builds `RuntimeOptions` with a struct literal must set the new field or end with `..RuntimeOptions::default()`.

`feed` is a reserved metric namespace, so `Meter::with_namespace("feed", …)` panics, as it does for the other framework roots. A connector or pipeline that registered its own `spate_feed_*` families must move them to another namespace.
