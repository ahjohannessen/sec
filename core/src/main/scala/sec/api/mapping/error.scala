package sec
package api
package mapping

//======================================================================================================================

final private[sec] case class EncodingError(msg: String) extends RuntimeException(msg)
private[sec] object EncodingError {
  def apply(e: Throwable): EncodingError = EncodingError(e.getMessage)
}

//======================================================================================================================

final private[sec] case class DecodingError(msg: String) extends RuntimeException(msg)
private[sec] object DecodingError {
  def apply(e: Throwable): DecodingError = DecodingError(e.getMessage)
}

//======================================================================================================================

final private[sec] case class ProtoResultError(msg: String) extends RuntimeException(msg)

//======================================================================================================================
