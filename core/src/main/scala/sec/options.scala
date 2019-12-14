package sec

import cats.implicits._
import api._

//======================================================================================================================

final case class Options(
  connectionName: String,
  defaultCreds: Option[UserCredentials]
)

object Options {
  val default = Options("sec", UserCredentials.unsafe("admin", "changeit").some)
}

//======================================================================================================================
