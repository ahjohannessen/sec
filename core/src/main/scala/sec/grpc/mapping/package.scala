package sec.grpc

package object mapping {

  final case class ProtoResultError(msg: String) extends RuntimeException(msg)

}
