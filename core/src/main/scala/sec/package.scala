import java.util.UUID
import cats.effect.Sync

package object sec {

  type Attempt[T] = Either[String, T]

  ///

  def uuid[F[_]: Sync]: F[UUID] = Sync[F].delay(UUID.randomUUID())

}
