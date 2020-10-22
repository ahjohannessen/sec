---
id: types
title: Basic Data Types
sidebar_label: Data Types
---

In @libName@ some basic data types surface in many use cases when interacting with @esdb@. In order to establish some basics
these are described in the following - More details about the particulars of these types are in API documentation.  

### StreamId

Streams in @esdb@ have stream identifiers that can be classified as user defined and system defined.
In @esdb@ streams prefixed with `$` are reserved for the system, for instance `$settings`. 
Furthermore, @esdb@ also has a concept of metadata streams for streams. Metadata streams for system streams are prefixed
with `$$`, e.g. the corresponding metadata stream for `$settings` is `$$$settings`. Metadata streams for user defined 
streams are prefixed with a `$`, e.g. `user_stream` has a corresponding metadata stream named `$user_stream`.

In @libName@ a stream identifier is an ADT called `StreamId` with variants `Id` and `MetaId` - `Id`
has two variants, `Normal` and `System`. The stream identifiers that a user can create are `Normal` and `System`,
this is done with `StreamId.apply` that returns an `Either[InvalidInput, Id]`. Some examples of `StreamId` construction 
are:

```scala mdoc:silent
import cats.syntax.all._
import sec.StreamId

val user   = StreamId("user_stream")    // Right(Normal("user_stream")
val system = StreamId("$system_stream") // Right(System("system_stream"))

// MetaId for above you get from the metaId method:

user.map(_.metaId.show)   // Right("$$user_stream")
system.map(_.metaId.show) // Right("$$$system_stream")

// Invalid stream identifiers

StreamId("")        // Left(InvalidInput("id cannot be empty"))
StreamId("$$oops")  // Left(InvalidInput("value must not start with $$, but is $$oops"))
```
Moreover, a few common system stream identififiers are located in the `StreamId` companion object, for instance:

```scala mdoc:silent
import sec.StreamId

StreamId.All.show               // $all
StreamId.All.metaId.show        // $$$all
StreamId.Scavenges.show         // $scavenges
StreamId.Scavenges.metaId.show  // $$$scavenges
```


### StreamPosition

When you store an event in @esdb@ it is assigned a **stream position** in the individual stream it belongs to. 
In @libName@ a stream position is an ADT called `StreamPosition` with two variants, `StreamPosition.Exact` that contains
a `Long` and `StreamPosition.End` that is an object representing the end of a stream.

A `StreamPosition` that you can create is `Exact` and this is done with `StreamPosition.apply` that returns an 
`Either[InvalidInput, Exact]`. Examples of `StreamPosition` construction:

```scala mdoc:silent
import sec.StreamPosition

StreamPosition.Start // Exact(0L)
StreamPosition(1L)   // Right(Exact(1L))
StreamPosition.End   // End
StreamPosition(-1L)  // Left(InvalidInput("value must be >= 0, but is -1"))
```

One use case where you need to construct a `StreamPosition` is when you to store a pointer of the last processed 
event for a particular stream as a `Long` in a read model and, e.g. after a restart of your application, 
need to resume reading from @esdb@.

### LogPosition

All events in @esdb@ have a logical position in the global transaction log. A logical position consists of a *commit position* value
and a *prepare position* value. In @libName@ this is modelled as an ADT called `LogPosition` with two variants, `LogPosition.Exact` 
that contains two `Long` values and `LogPosition.End` representing the end of the log.

A `LogPosition` that you can create from `Long` values is `Exact`, this is done with `LogPosition.apply` that returns an
`Either[InvalidInput, Exact]`. Examples of `LogPosition` construction:

```scala mdoc:silent
import sec.LogPosition

LogPosition.Start    // Exact(0L, 0L)
LogPosition.End      // End
LogPosition(1L, 1L)  // Right(Exact(1L, 1L))
LogPosition(-1L, 0L) // Left(InvalidInput("commit must be >= 0, but is -1"))
LogPosition(0L, -1L) // Left(InvalidInput("prepare must be >= 0, but is -1"))
LogPosition(0L, 1L)  // Left(InvalidInput("commit must be >= prepare, but 0 < 1"))
``` 

Cases where you construct a `LogPosition` is similar to that of `StreamPosition`, maintaining a pointer of last 
processed event. However, here you keep a pointer to the global log, `StreamId.All`, instead of an individual 
stream.

### StreamState
 
Some operations, such as appending events to a stream, require that you provide an *expectation* of what **state**
the stream currently is in. If the stream state does not fullfil that expectation then an exception will be raised by @esdb@. 
In @libName@ this expected stream state is represented by the ADT `StreamState` that has four variants:

 - `NoStream` - The stream does not exist yet. 
 - `Any` - No expectation about the current state of the stream.
 - `StreamExists` - The stream, or its metadata stream, is present.
 - `StreamPosition.Exact` - The stream exists and its last written stream position is expected to be `Exact`.

The `StreamState` expectation can be used to implement optimistic concurrency. When you retrieve a stream from @esdb@, 
you can take note of the current stream position, then when you append to the stream you can determine if the stream has 
been modified in the meantime.

### EventData

The event data you store in @esdb@ is composed of an event type, an event id, payload data, metadata and a content type. 
In @libName@ this is modelled as `EventData` with types that are explained below.

#### EventType

An event type should be supplied for your event data. This is a unique string used to identify the type of event you are 
saving. One might be tempted to use language runtime types for event types as it might make marshalling more convenient. 
However, this is not recommended as it couples storage to your types. Instead, you can use a mapping between event 
types stored in @esdb@ and your concrete runtime types.

The string that @esdb@ uses for the event type is modelled in @libName@ as an ADT with two main variants
`Normal` and `System`. The type that you can create is `Normal` and this is done with `EventType.apply` 
that returns `Either[InvalidInput, Normal]`. Input is validated for emptiness and not starting with `$` that @esdb@ 
uses for reserved system defined types such as `$>` and `$metadata`.

Examples of `EventType` construction:

```scala mdoc:silent
import sec.EventType

EventType("foo.bar.baz") // Right(Normal("foo.bar.baz")
EventType("")            // Left(InvalidInput("Event type name cannot be empty"))
EventType("$@")          // Left(InvalidInput("value must not start with $, but is $@"))
```

Common system types are located in the companion of `EventType`, some examples are:

```scala mdoc:silent
import sec.EventType

EventType.LinkTo.show          // $>
EventType.StreamMetadata.show  // $metadata
EventType.Settings.show        // $settings
EventType.StreamReference.show // $@
```

#### EventId

The format of an event identifier is a *[uuid](https://en.wikipedia.org/wiki/Universally_unique_identifier)* and is 
used by @esdb@ to uniquely identify the event you are trying to append. If two events with the same uuid are appended to
the same stream in quick succession @esdb@ only appends one copy of the event to the stream. More information about this 
is in the @esdb@ [docs](https://eventstore.com/docs/dotnet-api/optimistic-concurrency-and-idempotence/index.html#idempotence) 
about concurrency and idempotence.

#### Data

The `data` field on `EventData` is a [scodec](https://github.com/scodec/scodec-bits) `ByteVector` encoded representation 
of your event data. If you store your data as JSON you can make use of @esdb@ functionality for projections. 
However, it is also common to store data in a [`protocol buffers`](https://developers.google.com/protocol-buffers) format.

#### Metadata

It is common to store additional information along side your event data. This can be correlation id, timestamp, audit, 
marshalling info and so on. @esdb@ allows you to store a separate byte array containing this information to keep data 
and metadata separate. These extra bytes are stored in a `ByteVector` field of `EventData` called `metadata`.

#### ContentType

The `data` and `metadata` fields on `EventData` have a content type, `ContentType`, with 
variants `Binary` and `Json`. This is used to provide @esdb@ information about whether the data is stored as json or as 
binary. As you might have noticed, both `ByteVector` fields of `EventData` share the same content type, this is because 
@esdb@ does not support different content types for `data` and `metadata`.

When `EventType` is translated to the protocol of @esdb@ it becomes `application/octet-stream` for `Binary` and 
`application/json` for `Json`. In the future there might come more content types, e.g. something that corresponds to 
`application/proto` or `application/avro`.

### Event

Data arriving from @esdb@ is modelled as an `Event`, which is an *ADT* that has two variants, `EventRecord` and 
`ResolvedEvent`. 

An `EventRecord` consists of the following data types:

  - `streamId: StreamId` - The stream the event belongs to.
  - `streamPosition: StreamPosition.Exact` - The position of the event in the stream.
  - `logPosition: LogPosition.Exact` - The position of the event in the global log.
  - `eventData: EventData` - The data of the event.
  - `created: ZonedDateTime` - The time the event was created.
  
A `ResolvedEvent` is used when consuming streams that link to other streams. It consists of:

 - `event: EventRecord` - The linked event.
 - `link: EventRecord` - The linking event record.

Later on when using the [EsClient API](client-api.md), you will learn about reading from streams and instruct @esdb@ to 
resolve links such that you get events of type `ResolvedEvent` back.