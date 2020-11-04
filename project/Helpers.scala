import sbt._
import dotty.tools.sbtplugin.DottyPlugin.autoImport.DottyCompatModuleID

object Helpers {

  final private val ignores =
    Set("scala3-library", "scala3-compiler", "dotty", "dotty-library", "dotty-compiler")

  implicit class Scala3CompatModuleID(moduleID: ModuleID) {

    /**
     * Workaround for upstream
     */
    def withScala3Compat(scalaVersion: String): ModuleID =
      if (ignores.contains(moduleID.name)) moduleID
      else DottyCompatModuleID(moduleID).withDottyCompat(scalaVersion)
  }

}
