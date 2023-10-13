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
package grpc

import io.grpc.Metadata
import grpc.constants.Headers.{Authorization, ConnectionName, RequiresLeader}

private[sec] object metadata:

  private[grpc] object keys:
    val authorization: Metadata.Key[UserCredentials] = Metadata.Key.of(Authorization, UserCredentialsMarshaller)
    val connectionName: Metadata.Key[String]         = Metadata.Key.of(ConnectionName, StringMarshaller)
    val requiresLeader: Metadata.Key[Boolean]        = Metadata.Key.of(RequiresLeader, BooleanMarshaller)

  extension (ctx: Context)
    def toMetadata: Metadata =
      val md = new Metadata()
      ctx.userCreds.foreach(md.put(keys.authorization, _))
      md.put(keys.connectionName, ctx.connectionName)
      md.put(keys.requiresLeader, ctx.requiresLeader)
      md
