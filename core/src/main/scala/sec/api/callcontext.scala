package sec
package api

import cats.implicits._

//======================================================================================================================

final private[sec] case class Context(
  connectionName: String,
  userCreds: Option[UserCredentials],
  requiresLeader: Boolean
)

//======================================================================================================================

sealed abstract case class UserCredentials(username: String, password: String) {
  override def toString = s"UserCredentials(username = $username, password = 🤐)"
}

object UserCredentials {

  private[sec] def unsafe(username: String, password: String): UserCredentials =
    new UserCredentials(username, password) {}

  def apply(username: String, password: String): Attempt[UserCredentials] = {

    def validate(value: String, name: String) = {

      def nonEmpty(v: String): Attempt[String] =
        Option(v).filter(_.nonEmpty).toRight(s"$name is empty")

      def validChars(v: String): Attempt[String] =
        Option.unless(v.contains(':'))(v).toRight(s"$name cannot contain characters [':']")

      nonEmpty(value) >>= validChars
    }

    (validate(username, "username"), validate(password, "password")).mapN(UserCredentials.unsafe)
  }
}

//======================================================================================================================
