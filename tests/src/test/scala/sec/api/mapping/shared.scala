/*
 * Copyright 2020 Scala EventStoreDB Client
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api
package mapping

import java.util.{UUID => JUUID}
import com.google.protobuf.ByteString
import cats.syntax.all._
import com.eventstore.dbclient.proto.{shared => ps}
import sec.api.exceptions.{MaximumAppendSizeExceeded, StreamDeleted, WrongExpectedState}
import sec.api.mapping.shared._
import sec.helpers.implicits._

class SharedMappingSuite extends SecSuite {

  group("shared") {

    val uuidString     = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val juuid          = JUUID.fromString(uuidString)
    val uuidStructured = ps.UUID.Structured(juuid.getMostSignificantBits(), juuid.getLeastSignificantBits())
    val mkId           = mkJuuid[ErrorOr] _

    test("mkJuuid") {
      assertEquals(mkId(ps.UUID().withValue(ps.UUID.Value.Empty)), ProtoResultError("UUID is missing").asLeft)
      assertEquals(mkId(ps.UUID().withString(uuidString)), juuid.asRight)
      assertEquals(mkId(ps.UUID().withStructured(uuidStructured)), juuid.asRight)
    }

    test("mkUuid") {
      assertEquals(mkUuid(juuid), ps.UUID().withStructured(uuidStructured))
    }

    test("mkStreamId") {

      import StreamId.{MetaId, All}

      assertEquals(mkStreamId[ErrorOr](ps.StreamIdentifier()), ProtoResultError("name cannot be empty").asLeft)
      assertEquals(mkStreamId[ErrorOr]("".toStreamIdentifer), ProtoResultError("name cannot be empty").asLeft)
      assertEquals(mkStreamId[ErrorOr]("abc".toStreamIdentifer), StreamId.normal("abc").unsafe.asRight)
      assertEquals(mkStreamId[ErrorOr]("$abc".toStreamIdentifer), StreamId.system("abc").unsafe.asRight)
      assertEquals(mkStreamId[ErrorOr]("$all".toStreamIdentifer), All.asRight)
      assertEquals(mkStreamId[ErrorOr]("$$abc".toStreamIdentifer), MetaId(StreamId.normal("abc").unsafe).asRight)
      assertEquals(mkStreamId[ErrorOr]("$$$streams".toStreamIdentifer), MetaId(StreamId.Streams).asRight)

    }

    test("mkWrongExpectedStreamState") {

      val empty = com.google.protobuf.empty.Empty()
      val sid   = StreamId("stream-a").unsafe
      val wev   = ps.WrongExpectedVersion()

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev),
        ProtoResultError("ExpectedStreamPositionOption is missing").asLeft
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedStreamPosition(1L)),
        ProtoResultError("CurrentStreamRevisionOption is missing").asLeft
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedStreamPosition(1L).withCurrentStreamRevision(0L)),
        WrongExpectedState(sid, StreamPosition(1L), StreamPosition(0L)).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedNoStream(empty).withCurrentStreamRevision(0L)),
        WrongExpectedState(sid, StreamState.NoStream, StreamPosition(0L)).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedAny(empty).withCurrentStreamRevision(0L)),
        WrongExpectedState(sid, StreamState.Any, StreamPosition(0L)).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedStreamExists(empty).withCurrentStreamRevision(0L)),
        WrongExpectedState(sid, StreamState.StreamExists, StreamPosition(0L)).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedStreamPosition(1L).withCurrentNoStream(empty)),
        WrongExpectedState(sid, StreamPosition(1L), StreamState.NoStream).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedAny(empty).withCurrentNoStream(empty)),
        WrongExpectedState(sid, StreamState.Any, StreamState.NoStream).asRight
      )

      assertEquals(
        mkWrongExpectedStreamState[ErrorOr](sid, wev.withExpectedStreamExists(empty).withCurrentNoStream(empty)),
        WrongExpectedState(sid, StreamState.StreamExists, StreamState.NoStream).asRight
      )

    }

    test("mkStreamDeleted") {
      assertEquals(
        mkStreamDeleted[ErrorOr](ps.StreamDeleted(ps.StreamIdentifier.of(ByteString.copyFromUtf8("acracadabra")).some)),
        StreamDeleted("acracadabra").asRight
      )

      assertEquals(mkStreamDeleted[ErrorOr](ps.StreamDeleted(none)), StreamDeleted("<unknown>").asRight)
    }

    test("mkMaximumAppendSizeExceeded") {
      assertEquals(
        mkMaximumAppendSizeExceeded(ps.MaximumAppendSizeExceeded.of(2 * 1024 * 1024)),
        MaximumAppendSizeExceeded((2 * 1024 * 1024).some)
      )
    }

    test("mkError") {
      assertEquals(mkError[Int]("Oopsie"), ProtoResultError("Oopsie").asLeft[Int])
    }

  }

}
