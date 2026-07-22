---
id: multi-append
title: Multi-Stream Append
sidebar_label: Multi-Append
---

:::caution Experimental
The underlying v2 protocol is marked unstable by KurrentDB — its schema carries the header *"This
protocol is UNSTABLE in the sense of being subject to change"* — and this API may change with it.
It requires a KurrentDB 26.1+ server with the v2 protocol enabled.
:::

`multiStreamAppend` appends records to one or more streams **atomically**: either every record in
the request is committed, or none are. In addition, consistency conditions can be placed on streams
that are read from but not written to, so the boundary of a consistent operation is chosen per
append rather than fixed per stream — often referred to as a *dynamic consistency boundary*.

### The model

A [`RecordData`](types.md) differs from `EventData` in being honest about the v2 shape: there are
no raw metadata bytes. Instead a record carries a `Schema` — whose name surfaces as the v1
`eventType` when read back — and validated `Properties`. The server synthesizes the v1 metadata
view as a JSON object of the user properties merged with reserved `$`-prefixed schema keys, which
is why `Properties.of` rejects keys using the `$` namespace.

Every written stream carries a mandatory `StreamState` expectation via `StreamAppend`, and
`StreamGuard` expresses conditions on unwritten streams. A stream may appear only once across
appends and guards, and a duplicate is rejected by the server. A `StreamState.Any` expectation
produces no check on the wire - the v2 protocol has
no Any sentinel and expresses it as absence.

### Example

Below, a seat reservation is appended only if the flight stream is still at the stream position observed
when the decision was made — although nothing is written to the flight stream:

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.syntax.all.*
import cats.effect.*
import sec.*
import sec.api.*
import sec.syntax.all.*
import scodec.bits.ByteVector

object MultiAppend extends IOApp:

  def run(args: List[String]): IO[ExitCode] = EsClient
    .singleNode[IO](Endpoint("127.0.0.1", 2113))
    .resource
    .use(client => program(client.streams))
    .as(ExitCode.Success)

  def program(streams: Streams[IO]): IO[Unit] =
    for
      reservation <- StreamId("reservation-1832").liftTo[IO]
      flight      <- StreamId("flight-ua123").liftTo[IO]
      uuid        <- mkUuid[IO]
      data        <- ByteVector.encodeUtf8("""{ "seat": "12A" }""").liftTo[IO]
      properties  <- Properties
                       .of("tenant" -> PropertyValue.Str("acme"))
                       .leftMap(new IllegalArgumentException(_))
                       .liftTo[IO]
      record       = RecordData(uuid, Schema.json("SeatReserved"), data, properties)
      result      <- streams.multiStreamAppend(
                       appends = NonEmptyList.one(
                         StreamAppend(reservation, StreamState.NoStream, NonEmptyList.one(record))
                       ),
                       guards = List(StreamGuard(flight, StreamPosition(42L)))
                     )
      _           <- IO.println(s"committed at ${result.position}, streamPositions ${result.streamPositions}")
    yield ()
```

### Results and errors

A successful append yields a `MultiAppendResult` with the transaction's single `LogPosition.Exact`
— the v2 protocol reports one position, mapped as commit == prepare — and the resulting
`StreamPosition.Exact` per written stream.

A violated expectation or guard raises `AppendConsistencyViolation`, naming each violating stream
with its expected and actual state. Other failures surface as `StreamConditionMismatch`,
`AppendRecordSizeExceeded`, `AppendTransactionSizeExceeded`, `StreamAlreadyExists`, or
`StreamTombstoned`. Streams that do not exist or are deleted raise the same `StreamNotFound` and
`StreamDeleted` as the v1 operations. A malformed request - a duplicate stream, an empty property
key - is rejected by the server as `InvalidRequest`, carrying the per-field violations.
