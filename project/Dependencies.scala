import sbt._

object Dependencies {

  object versions {
    val catsCore      = "2.0.0"
    val catsEffect    = "2.0.0"
    val fs2           = "2.1.0"
    val scodecBits    = "1.1.12"
    val circe         = "0.12.3"
    val scalaPb       = scalapb.compiler.Version.scalapbVersion
    val grpc          = scalapb.compiler.Version.grpcJavaVersion
    val fs2Grpc       = org.lyranthe.fs2_grpc.buildinfo.BuildInfo.version
    val specs2        = "4.8.1"
    val kindProjector = "0.11.0"
  }

  // Compile

  val cats         = "org.typelevel" %% "cats-core"     % versions.catsCore
  val catsEffect   = "org.typelevel" %% "cats-effect"   % versions.catsEffect
  val fs2          = "co.fs2"        %% "fs2-core"      % versions.fs2
  val scodecBits   = "org.scodec"    %% "scodec-bits"   % versions.scodecBits
  val circe        = "io.circe"      %% "circe-core"    % versions.circe
  val circeParser  = "io.circe"      %% "circe-parser"  % versions.circe
  val circeGeneric = "io.circe"      %% "circe-generic" % versions.circe

  val scalaPb = "com.thesamet.scalapb" %% "scalapb-runtime" % versions.scalaPb

  val grpcNetty = "io.grpc"  % "grpc-netty"                      % versions.grpc
  val tcnative  = "io.netty" % "netty-tcnative-boringssl-static" % "2.0.25.Final"

  // Testing

  val specs2           = "org.specs2" %% "specs2-core"       % versions.specs2
  val specs2ScalaCheck = "org.specs2" %% "specs2-scalacheck" % versions.specs2

  // Compiler & SBT Plugins

  val kindProjector = "org.typelevel" %% "kind-projector" % versions.kindProjector cross CrossVersion.full

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
