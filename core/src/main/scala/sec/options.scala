package sec

import cats.implicits._
import api._

//======================================================================================================================

final case class Options(
  defaultCreds: Option[UserCredentials]
)

object Options {
  val default = Options(UserCredentials.unsafe("admin", "changeit").some)
}

//======================================================================================================================
