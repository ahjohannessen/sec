package sec

import java.util.UUID
import cats.implicits._
import cats.effect.Sync
import io.circe.Printer

package object api {

//======================================================================================================================

  private[sec] val jsonPrinter: Printer = Printer.noSpaces.copy(dropNullValues = true)

//======================================================================================================================

  private[sec] def uuid[F[_]: Sync]: F[UUID]    = Sync[F].delay(UUID.randomUUID())
  private[sec] def uuidS[F[_]: Sync]: F[String] = uuid[F].map(_.toString)

//======================================================================================================================

  type DeleteResult = sec.api.Streams.DeleteResult
  type WriteResult  = sec.api.Streams.WriteResult

//======================================================================================================================

}
