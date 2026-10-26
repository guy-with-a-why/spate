**Breaking:** **A feed backend receives a `Meter` for its own metrics** (`spate-core`)

`Feed::attach_metrics` hands a feed backend a `Meter` for its own metric families. The runtime calls it once, before the source is handed the feed. The families land under `spate_<backend>_feed_*`, with `component="feed"` and the backend's key as `component_type`, beside the framework's `spate_feed_*` families for the same feed. The method has a default that ignores the `Meter`, so an existing backend compiles unchanged.

`feed` joins `ROLE_SEGMENTS`. A `Meter` local name that starts with `feed_` panics, as one that starts with `source_` or `sink_` does. A connector that registered a family under such a name must rename it.
