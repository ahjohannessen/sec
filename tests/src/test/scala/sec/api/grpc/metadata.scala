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
package grpc

import cats.syntax.all._
import io.grpc.Metadata
import org.specs2._

import grpc.metadata._
import grpc.metadata.keys._
import grpc.constants.Headers._

class MetadataSpec extends mutable.Specification {

  "ContextOps.toMetadata" >> {

    val creds = UserCredentials.unsafe("hello", "world")
    val name  = "abc"

    val md1 = Context(name, None, false).toMetadata
    val md2 = Context(name, creds.some, true).toMetadata

    Option(md1.get(connectionName)) should beSome(name)
    Option(md1.get(authorization)) should beNone
    Option(md1.get(requiresLeader)) should beSome(false)

    Option(md2.get(connectionName)) should beSome(name)
    Option(md2.get(Metadata.Key.of(Authorization, StringMarshaller))) should beSome("Basic aGVsbG86d29ybGQ=")
    Option(md2.get(requiresLeader)) should beSome(true)

  }

}
