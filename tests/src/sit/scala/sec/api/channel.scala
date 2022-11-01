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

import cats.syntax.all._
import cats.effect.IO
import fs2.text
import fs2.io.file._
import io.grpc.{ChannelCredentials, TlsChannelCredentials}
import channel.mkCredentials
import ConnectionMode._

class ChannelSuite extends SecEffectSuite {

  group("mkCredentials") {

    test("ConnectionMode.Insecure yields none[ChannelCredentials]") {
      assertIO(mkCredentials[IO](Insecure), none[ChannelCredentials])
    }

    test("ConnectionMode.Secure(file) yields some[TlsChannelCredentials]") {
      assertIOBoolean(
        mkCredentials[IO](Secure(SnSuite.cert)).map(_.exists(_.isInstanceOf[TlsChannelCredentials]))
      )
    }

    test("ConnectionMode.Secure(base64) yields some[TlsChannelCredentials]") {
      assertIOBoolean(
        Files[IO]
          .readAll(Path(SnSuite.cert.getPath))
          .through(text.base64.encode)
          .through(text.lines)
          .compile
          .foldMonoid
          .flatMap(c64 => mkCredentials[IO](Secure(c64)).map(_.exists(_.isInstanceOf[TlsChannelCredentials])))
      )
    }

  }

}
