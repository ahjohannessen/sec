package sec
package core

import io.circe.Codec
import constants.SystemMetadata.Acl._

/**
 * @param readRoles Roles and users permitted to read the stream
 * @param writeRoles Roles and users permitted to write to the stream
 * @param deleteRoles Roles and users permitted to delete the stream
 * @param metaReadRoles Roles and users permitted to read stream metadata
 * @param metaWriteRoles Roles and users permitted to write stream metadata
 * */
final case class StreamAcl(
  readRoles: Set[String],
  writeRoles: Set[String],
  deleteRoles: Set[String],
  metaReadRoles: Set[String],
  metaWriteRoles: Set[String]
)

object StreamAcl {

  implicit val codecForStreamAcl: Codec[StreamAcl] =
    Codec.forProduct5[StreamAcl, Set[String], Set[String], Set[String], Set[String], Set[String]](
      AclRead,
      AclWrite,
      AclDelete,
      AclMetaRead,
      AclMetaWrite
    )((r, w, d, mr, mw) => StreamAcl(r, w, d, mr, mw)) { sa =>
      (sa.readRoles, sa.writeRoles, sa.deleteRoles, sa.metaReadRoles, sa.metaWriteRoles)
    }
}
