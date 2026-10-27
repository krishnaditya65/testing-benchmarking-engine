# Scaling notes

`workflow-canvas-frontend` is a stateless Next.js application: it holds no
server-side session state of its own, so any number of replicas can serve
traffic behind a load balancer/CDN interchangeably, and static assets (JS
bundles, CSS) can be edge-cached aggressively since they are immutable per
build. Authentication and session identity live in a JWT/cookie that this
frontend forwards and that `pipeline-api-gateway` validates — this service
never persists auth state locally. It scales horizontally simply by adding
replicas behind the load balancer, with no coordination required between
instances. Its failure mode is intentionally boring: a crashed pod is killed
and restarted with zero data loss, because all durable state — pipeline
definitions, run history, telemetry — lives server-side in
pipeline-api-gateway's stores, not in this frontend. The one piece of client
state that matters, the live run monitor's WebSocket subscription
(`useLiveDagRunStatus`), is built on `graphql-ws`'s client, which reconnects
automatically with backoff on network blips or gateway restarts, so viewers
resume live updates without a manual page reload.
