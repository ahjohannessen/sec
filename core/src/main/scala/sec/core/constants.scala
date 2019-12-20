package sec
package core

private[sec] object constants {

//======================================================================================================================

  object SystemMetadata {

    final val MaxAge: String         = "$maxAge"
    final val MaxCount: String       = "$maxCount"
    final val TruncateBefore: String = "$tb"
    final val CacheControl: String   = "$cacheControl"
    final val Acl: String            = "$acl"

    object AclKeys {
      final val Read: String            = "$r"
      final val Write: String           = "$w"
      final val Delete: String          = "$d"
      final val MetaRead: String        = "$mr"
      final val MetaWrite: String       = "$mw"
      final val UserStreamAcl: String   = "$userStreamAcl"
      final val SystemStreamAcl: String = "$systemStreamAcl"
    }
  }

//======================================================================================================================

}
