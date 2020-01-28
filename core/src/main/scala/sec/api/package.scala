package sec

import java.util.UUID
import cats.implicits._
import cats.effect.Sync

package object api {

//======================================================================================================================

  private[sec] def uuid[F[_]: Sync]: F[UUID]    = Sync[F].delay(UUID.randomUUID())
  private[sec] def uuidS[F[_]: Sync]: F[String] = uuid[F].map(_.toString)

//======================================================================================================================

}
