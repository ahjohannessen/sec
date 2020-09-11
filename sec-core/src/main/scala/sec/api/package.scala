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

import java.util.UUID
import cats.syntax.all._
import cats.effect.Sync
import io.circe.Printer

package object api {

//======================================================================================================================

  private[sec] val jsonPrinter: Printer = Printer.noSpaces.copy(dropNullValues = true)

//======================================================================================================================

  private[sec] def uuid[F[_]: Sync]: F[UUID]    = Sync[F].delay(UUID.randomUUID())
  private[sec] def uuidS[F[_]: Sync]: F[String] = uuid[F].map(_.toString)

//======================================================================================================================

}
