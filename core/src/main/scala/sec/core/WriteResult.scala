package sec
package core

final case class WriteResult(version: StreamRevision.Exact, position: Position)
