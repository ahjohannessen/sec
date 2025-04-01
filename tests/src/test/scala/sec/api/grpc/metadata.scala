/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import cats.syntax.all.*
import io.grpc.Metadata
import grpc.metadata.*
import grpc.metadata.keys.*
import grpc.constants.Headers.*

class MetadataSuite extends SecSuite:

  test("ContextOps.toMetadata") {

    val creds = UserCredentials.unsafe("hello", "world")
    val name  = "abc"

    val md1 = Context(name, None, false).toMetadata
    val md2 = Context(name, creds.some, true).toMetadata

    assertEquals(Option(md1.get(connectionName)), Some(name))
    assertEquals(Option(md1.get(authorization)), None)
    assertEquals(Option(md1.get(requiresLeader)), Some(false))

    assertEquals(Option(md2.get(connectionName)), Some(name))
    assertEquals(Option(md2.get(Metadata.Key.of(Authorization, StringMarshaller))), Some("Basic aGVsbG86d29ybGQ="))
    assertEquals(Option(md2.get(requiresLeader)), Some(true))

  }
