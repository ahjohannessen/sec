package sec

import java.util.UUID
import scodec.bits.{ByteOrdering, ByteVector => BV}
import com.google.protobuf.{ByteString => BS}

// TODO: Temporary stuff for spiking
package object format {

  def bs2bv(bs: BS): BV               = BV(bs.asReadOnlyByteBuffer())
  def bs2bv(bs: Option[BS]): BV       = bs.fold(BV.empty)(bs2bv)
  def bv2bs(bv: BV): BS               = BS.copyFrom(bv.toByteBuffer)
  def bv2bsOpt(bv: BV): Option[BS]    = option(bv.isEmpty, bv2bs(bv))
  def readUuid(bs: BS): Attempt[UUID] = UuidFormat.readId(BV(bs.toByteArray))
  def writeUuid(id: UUID): BS         = BS.copyFrom(UuidFormat.writeId(id).toByteBuffer)

  def option[T](b: Boolean, f: => T): Option[T] = if (b) Some(f) else None

  ///

  implicit final class UUIDOps(val uuid: UUID) extends AnyVal {
    def toBS: BS = writeUuid(uuid)
  }

  implicit final class ByteVectorOps(val bv: BV) extends AnyVal {
    def toBS: BS = bv2bs(bv)
  }

  implicit final class ByteStringOps(val bs: BS) extends AnyVal {
    def toBV: BV              = bs2bv(bs)
    def toUUID: Attempt[UUID] = readUuid(bs)
  }

  private[sec] object UuidFormat {

    private val length = 16

    def inverseBitMagic(mostSignificant: Long): Long = {
      val a: Long = (mostSignificant >> 16) & 0xFFFF
      val b: Long = (mostSignificant >> 48) & 0xFFFF
      val c: Long = (mostSignificant >> 32) & 0xFFFF
      val d: Long = mostSignificant & 0xFFFF

      (a << 48) | (d << 32) | (c << 16) | b
    }

    def bitMagic(mostSignificant: Long): Long = {
      val a: Long = mostSignificant & 0xFFFF
      val b: Long = (mostSignificant >> 16) & 0xFFFF
      val c: Long = (mostSignificant >> 48) & 0xFFFF
      val d: Long = (mostSignificant >> 32) & 0xFFFF

      (a << 48) | (b << 32) | (c << 16) | d
    }

    def writeId(id: UUID): BV = {
      val mostSignificant  = bitMagic(id.getMostSignificantBits)
      val leastSignificant = id.getLeastSignificantBits
      BV.fromLong(mostSignificant, size    = 8, ByteOrdering.LittleEndian) ++
        BV.fromLong(leastSignificant, size = 8, ByteOrdering.BigEndian)
    }

    def readId(bi: BV): Attempt[UUID] = {

      def fail: String = s"cannot parse uuid, actual length: ${bi.length}, expected: $length"

      def readUuid: UUID = {

        val most             = bi.take(8)
        val least            = bi.drop(8).take(8)
        val mostSignificant  = inverseBitMagic(most.toLong(signed = true, ByteOrdering.LittleEndian))
        val leastSignificant = least.toLong(signed = true, ByteOrdering.BigEndian)

        new UUID(mostSignificant, leastSignificant)
      }

      Either.cond(bi.length >= length, readUuid, fail)

    }

  }

}
