import sbt._

object Dependencies {

  object versions {

    val circe         = "0.12.2"
    val specs2        = "4.8.0"
    val kindProjector = "0.11.0"
  }


  // Compile

  val circe = "io.circe" %% "circe-core" % versions.circe

  // Testing

  val specs2            = "org.specs2"        %% "specs2-core"       % versions.specs2
  val specs2ScalaCheck  = "org.specs2"        %% "specs2-scalacheck" % versions.specs2

  // Compiler & SBT Plugins

  val kindProjector   = "org.typelevel" %% "kind-projector" % versions.kindProjector cross CrossVersion.full

}