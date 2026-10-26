**Breaking:** **A `Meter` local name may not start with `feed_`** (`spate-core`)

`Meter::counter`, `Meter::gauge` and `Meter::histogram` panic on a local name that starts with `feed_`, as they do for `source_` and `sink_`. The rule holds in every namespace, including the `spate_custom_*` names `ChainCtx::meter` registers. v0.3.0 accepted such names, so a connector or pipeline author who registered one must rename it. `ROLE_SEGMENTS` lists `feed`.

The segment is reserved for a feed backend's own families. `Feed::attach_metrics` hands a backend a `Meter` scoped `spate_<backend>_feed_*`, with `component="feed"` and the backend's key as `component_type`. The runtime calls it when it runs a pipeline on the feed, before the source is handed the feed. Calls made before then, and a feed used outside a pipeline, get no `Meter`.
