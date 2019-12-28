package sec
package api
package mapping

//======================================================================================================================

private[sec] final case class EncodingError(msg: String) extends RuntimeException(msg)
private[sec] object EncodingError {
  def apply(e: Throwable): EncodingError = EncodingError(e.getMessage)
}

//======================================================================================================================

private[sec] final case class DecodingError(msg: String) extends RuntimeException(msg)
private[sec] object DecodingError {
  def apply(e: Throwable): DecodingError = DecodingError(e.getMessage)
}

//======================================================================================================================

private[sec] final case class ProtoResultError(msg: String) extends RuntimeException(msg)

//======================================================================================================================
