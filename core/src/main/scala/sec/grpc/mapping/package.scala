package sec
package grpc

import scodec.bits.ByteVector
import com.google.protobuf.ByteString

package object mapping {

  implicit final class ByteVectorOps(val bv: ByteVector) extends AnyVal {
    def toByteString: ByteString = ByteString.copyFrom(bv.toByteBuffer)
  }

  implicit final class ByteStringOps(val bs: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.view(bs.asReadOnlyByteBuffer())
  }

  ///

  // Break up into different types
  final case class ProtoResultError(msg: String) extends RuntimeException(msg)

}
