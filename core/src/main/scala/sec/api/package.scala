package sec

import java.util.UUID
import cats.{ApplicativeError, MonadError}
import cats.implicits._
import cats.effect.Sync

package object api {

//======================================================================================================================

  private[sec] type ErrorM[F[_]] = MonadError[F, Throwable]
  private[sec] type ErrorA[F[_]] = ApplicativeError[F, Throwable]

//======================================================================================================================

  private[sec] def uuid[F[_]: Sync]: F[UUID]    = Sync[F].delay(UUID.randomUUID())
  private[sec] def uuidS[F[_]: Sync]: F[String] = uuid[F].map(_.toString)

//======================================================================================================================

}
