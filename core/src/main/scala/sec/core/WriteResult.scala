package sec
package core

import java.util.UUID

final case class WriteResult(
  id: UUID,
  currentRevision: StreamRevision.Exact
)
