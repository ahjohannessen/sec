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

import cats.ApplicativeThrow
import cats.syntax.all.*
import com.google.protobuf.ByteString
import scodec.bits.ByteVector

extension (bv: ByteVector)
  private[sec] def toByteString: ByteString =
    ByteString.copyFrom(bv.toByteBuffer)

extension (bs: ByteString)
  private[sec] def toByteVector: ByteVector =
    ByteVector.view(bs.asReadOnlyByteBuffer())

//

extension [A](o: Option[A])

  private[sec] def require[F[_]: ApplicativeThrow](value: String): F[A] = require[F](value, None)
  private[sec] def require[F[_]: ApplicativeThrow](value: String, details: Option[String]) =
    def extra = details.map(d => s" $d").getOrElse("")
    def msg   = s"Required value $value missing or invalid.$extra"
    o.toRight(ProtoResultError(msg)).liftTo[F]
