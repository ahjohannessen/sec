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
import cats.syntax.all._
import com.eventstore.dbclient.proto.shared._
import sec.api.mapping.shared._
import sec.helpers.implicits._

class SharedMappingSuite extends SecSuite {

  group("shared") {

    val uuidString     = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val juuid          = JUUID.fromString(uuidString)
    val uuidStructured = UUID.Structured(juuid.getMostSignificantBits(), juuid.getLeastSignificantBits())

    test("mkJuuid") {
      assertEquals(mkJuuid[ErrorOr](UUID().withValue(UUID.Value.Empty)), ProtoResultError("UUID is missing").asLeft)
      assertEquals(mkJuuid[ErrorOr](UUID().withString(uuidString)), juuid.asRight)
      assertEquals(mkJuuid[ErrorOr](UUID().withStructured(uuidStructured)), juuid.asRight)
    }

    test("mkUuid") {
      assertEquals(mkUuid(juuid), UUID().withStructured(uuidStructured))
    }

    test("mkStreamId") {

      import StreamId.{MetaId, All}

      assertEquals(mkStreamId[ErrorOr](StreamIdentifier()), ProtoResultError("name cannot be empty").asLeft)
      assertEquals(mkStreamId[ErrorOr]("".toStreamIdentifer), ProtoResultError("name cannot be empty").asLeft)
      assertEquals(mkStreamId[ErrorOr]("abc".toStreamIdentifer), StreamId.normal("abc").unsafe.asRight)
      assertEquals(mkStreamId[ErrorOr]("$abc".toStreamIdentifer), StreamId.system("abc").unsafe.asRight)
      assertEquals(mkStreamId[ErrorOr]("$all".toStreamIdentifer), All.asRight)
      assertEquals(mkStreamId[ErrorOr]("$$abc".toStreamIdentifer), MetaId(StreamId.normal("abc").unsafe).asRight)
      assertEquals(mkStreamId[ErrorOr]("$$$streams".toStreamIdentifer), MetaId(StreamId.Streams).asRight)

    }

  }

}
