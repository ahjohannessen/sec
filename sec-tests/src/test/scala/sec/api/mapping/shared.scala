/*
 * Copyright 2020 Alex Henning Johannessen
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
import org.specs2._
import com.eventstore.dbclient.proto.shared._
import sec.StreamId
import sec.api.mapping.shared._

class SharedMappingSpec extends mutable.Specification {

  "shared" >> {

    val uuidString     = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val juuid          = JUUID.fromString(uuidString)
    val uuidStructured = UUID.Structured(juuid.getMostSignificantBits(), juuid.getLeastSignificantBits())

    "mkJuuid" >> {
      mkJuuid[ErrorOr](UUID().withValue(UUID.Value.Empty)) shouldEqual ProtoResultError("UUID is missing").asLeft
      mkJuuid[ErrorOr](UUID().withString(uuidString)) shouldEqual juuid.asRight
      mkJuuid[ErrorOr](UUID().withStructured(uuidStructured)) shouldEqual juuid.asRight
    }

    "mkUuid" >> {
      mkUuid(juuid) shouldEqual UUID().withStructured(uuidStructured)
    }

    "mkStreamId" >> {
      mkStreamId[ErrorOr](StreamIdentifier()) shouldEqual ProtoResultError("name cannot be empty").asLeft
      mkStreamId[ErrorOr]("".toStreamIdentifer) shouldEqual ProtoResultError("name cannot be empty").asLeft
      mkStreamId[ErrorOr]("abc".toStreamIdentifer) shouldEqual StreamId.normal("abc").unsafe.asRight
      mkStreamId[ErrorOr]("$abc".toStreamIdentifer) shouldEqual StreamId.system("abc").unsafe.asRight
      mkStreamId[ErrorOr]("$all".toStreamIdentifer) shouldEqual StreamId.All.asRight
      mkStreamId[ErrorOr]("$$abc".toStreamIdentifer) shouldEqual StreamId.MetaId(StreamId.normal("abc").unsafe).asRight
      mkStreamId[ErrorOr]("$$$streams".toStreamIdentifer) shouldEqual StreamId.MetaId(StreamId.Streams).asRight
    }

  }

}
