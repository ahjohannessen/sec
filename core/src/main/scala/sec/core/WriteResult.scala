package sec
package core

import cats.Show

final case class WriteResult(
  currentRevision: StreamRevision.Exact
)

object WriteResult {
  implicit val showForWriteResult: Show[WriteResult] = Show.show { wr =>
    s"WriteResult(currentRevision = ${wr.currentRevision.value})"
  }
}
