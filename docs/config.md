---
id: config
title: Configuration
sidebar_label: Config
---
TODO: Describe builders used for various configuration aspects. Mention that optional `ciris` / `hocon` module
might be added.

### Common

TODO: explain common methods on `OptionsBuilder`

### Single Node

TODO: explain specific methods on `SingleNodeBuilder`

### Cluster

TODO: explain specific methods on `ClusterBuilder`

### Subscription Channel Pool

Subscriptions are infinite gRPC streams. HTTP/2 caps concurrent streams per connection, and when a connection hits
the server-side `MaxStreamsPerConnection` limit, grpc-java silently queues new calls: no error, no log line. In
practice this looks like subscriptions that "start" but never receive events.

@libName@ can route subscriptions over a pool of dedicated channels so that a channel never carries more concurrent
streams than the configured limit. Reads, appends and gossip stay on the regular channel. The pool is off by
default; enable it on either builder:

```scala mdoc:compile-only
import cats.effect.*
import cats.syntax.all.*
import sec.api.*
import sec.api.pool.*

val client: Resource[IO, EsClient[IO]] =
  Resource.eval(PoolConfig.of[IO](streamsPerChannel = 100, limit = Limit.Bounded(10))) >>= { pc =>
    EsClient
      .singleNode[IO](Endpoint("127.0.0.1", 2113))
      .withSubscriptionPool(pc)
      .resource
  }
```

`PoolConfig` validates its input - `streamsPerChannel` and the limit values must be positive - so construction
returns `Either[InvalidInput, PoolConfig]`; `PoolConfig.of` lifts the validation error into `F`.

`streamsPerChannel` has no default on purpose: it must mirror the server-side HTTP/2 concurrent-stream limit
(Kestrel `MaxStreamsPerConnection`, default 100). A silent client-side default could quietly disagree with the
server, which is exactly the failure class the pool exists to eliminate.

The `limit` controls growth. The pool never shrinks - subscriptions are long-lived, so it settles at its natural
size of `ceil(subscriptions / streamsPerChannel)` channels:

 - `Limit.Bounded(max)` - grow up to `max` channels, then fail fast with
   `sec.api.exceptions.SubscriptionPoolExhausted` instead of queueing silently. This is the default, with
   `max = 10`.
 - `Limit.Unbounded(sanityCap)` - never reject, but log at error level with per-channel occupancy for every growth
   past `sanityCap`, which distinguishes a subscription leak from legitimate load.

When loading configuration from [Typesafe Config](https://github.com/lightbend/config) via `EsClient.fromConfig`
the pool is configured as follows:

```hocon
sec.subscription-pool {
  enabled             = true      # kill-switch, default true
  streams-per-channel = 100      # required - the pool stays off without it
  limit               = bounded  # bounded | unbounded
  max-channels        = 10       # used when limit = bounded
  sanity-cap          = 10       # used when limit = unbounded
}
```

The pool only activates when `streams-per-channel` is present. `enabled = false` is an operational kill-switch: it
turns the pool off - restoring single-channel behavior - without having to remove the rest of the section.

While a pool is active, @libName@ also observes the regular channel and warns when its in-flight calls approach
`streams-per-channel`, since sustained saturation there usually indicates a long-lived read that belongs on a
subscription.