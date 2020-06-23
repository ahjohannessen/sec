package sec

import cats.implicits._

//======================================================================================================================

// TODO: Redo this wrt. cluster / operation / log / ... settings

final case class Options(
  connectionName: String,
  nodePreference: NodePreference,
  defaultCreds: Option[UserCredentials]
)

object Options {
  val default: Options =
    Options("sec", NodePreference.Leader, UserCredentials.unsafe("admin", "changeit").some)
}

//======================================================================================================================
