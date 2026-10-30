**Breaking:** **The runtime records work feed calls under `spate_feed_*`** (`spate-core`)

When a pipeline's source reads a work feed named by the `feed:` section, the runtime records the calls the source makes on it, whatever the backend. The series sit under `spate_feed_*`, with `component="feed"` and the backend's key as `component_type`, and `docs/METRICS.md` describes each family.

`feed` is a reserved metric namespace. `Meter::with_namespace("feed", …)` panics, as it does for the other framework roots, and a source or sink whose `component_type` is `feed` receives no `Meter`. A connector or pipeline that registered its own `spate_feed_*` families must move them to another namespace.
