**Breaking:** **The runtime publishes the feed's dead-letter depth** (`spate-core`)

`spate_feed_dead_letter_units` reports how many units the feed's dead-letter destination holds. Each instance reads it at startup and then once a minute, whether or not the job ends on its own, so a job that runs until a shutdown has a dead-letter alert. On a backend whose depth read is a request to its service, such as SQS, that is one request per instance each minute. An alert on `spate_coordination_splits_quarantined` from v0.3.0 can move to this gauge, aggregated across instances with `max`. On the Kafka feed the gauge never falls, so alert on a rise there.

`RuntimeOptions::dead_letter_read_interval` sets how often the runtime reads the depth, and `None` turns the reads off. Code that builds `RuntimeOptions` with a struct literal must set the new field or end with `..RuntimeOptions::default()`. A feed backend's `read_depth` may be called from a thread other than the source's.
