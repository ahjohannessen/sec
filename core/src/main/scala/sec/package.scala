import java.util.UUID
import cats.implicits._
import cats.effect.Sync
import com.google.protobuf.ByteString
import sec.format._

package object sec {

  type Attempt[T] = Either[String, T]

  ///

  def uuid[F[_]: Sync]: F[UUID]         = Sync[F].delay(UUID.randomUUID())
  def uuidBS[F[_]: Sync]: F[ByteString] = uuid[F].map(_.toBS)

}
