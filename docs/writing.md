---
id: writing
title: Writing Events
sidebar_label: Writing
---

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.syntax.all.*
import cats.effect.*
import sec.*
import sec.api.*
import sec.syntax.all.*
import scodec.bits.ByteVector

object WritingEvents extends IOApp:

  def run(args: List[String]): IO[ExitCode] = EsClient
    .singleNode[IO](Endpoint("127.0.0.1", 2113))
    .resource
    .use(client => useStreams(client.streams))
    .as(ExitCode.Success)

  def useStreams(streams: Streams[IO]): IO[Unit] = 

    val mkStreamId: IO[StreamId] = 
      for
        uuid     <- mkUuid[IO]
        streamId <- StreamId(s"write_example-$uuid").liftTo[IO]
      yield streamId

    def mkEventData(json: String): IO[EventData] = 
      for
        uuid <- mkUuid[IO]
        data <- ByteVector.encodeUtf8(json).liftTo[IO]
        et   <- EventType("event-type").liftTo[IO]
      yield EventData(et, uuid, data, ContentType.Json)

    val eventData1: IO[EventData] = mkEventData("""{ "data" : "hello" }""")
    val eventData2: IO[EventData] = mkEventData("""{ "data" : "world" }""")

    for
      streamId <- mkStreamId
      data     <- NonEmptyList.of(eventData1, eventData2).sequence
      _        <- streams.appendToStream(streamId, StreamState.NoStream, data)
      _        <- streams.readStreamForwards(streamId).debug(_.render).compile.drain
    yield ()
  
  

```
