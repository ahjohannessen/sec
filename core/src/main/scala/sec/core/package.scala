package sec

package object core {

  private[sec] object constants {

    object SystemEventTypes {
      final val StreamDeleted: String   = "$streamDeleted"
      final val StatsCollection: String = "$statsCollected"
      final val LinkTo: String          = "$>"
      final val StreamMetadata: String  = "$metadata"
      final val Settings: String        = "$settings"
      final val UserCreated: String     = "$UserCreated"
      final val UserUpdated: String     = "$UserUpdated"
      final val PasswordChanged: String = "$PasswordChanged"
    }

    object SystemMetadata {
      final val MaxAge: String         = "$maxAge"
      final val MaxCount: String       = "$maxCount"
      final val TruncateBefore: String = "$tb"
      final val CacheControl: String   = "$cacheControl"

      object Acl {
        final val Acl: String             = "$acl"
        final val AclRead: String         = "$r"
        final val AclWrite: String        = "$w"
        final val AclDelete: String       = "$d"
        final val AclMetaRead: String     = "$mr"
        final val AclMetaWrite: String    = "$mw"
        final val UserStreamAcl: String   = "$userStreamAcl"
        final val SystemStreamAcl: String = "$systemStreamAcl"
      }
    }

    object SystemStreams {
      final val StreamsStream: String     = "$streams"
      final val SettingsStream: String    = "$settings"
      final val StatsStreamPrefix: String = "$stats"
      final val MetadataPrefix: String    = "$$"
    }
  }

}
