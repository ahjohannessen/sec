import sbt._

object Dependencies {

  object versions {

    val catsCore          = "2.1.1"
    val catsEffect        = "2.1.3"
    val fs2               = "2.3.0"
    val scodecBits        = "1.1.14"
    val circe             = "0.13.0"
    val scalaPb           = scalapb.compiler.Version.scalapbVersion
    val grpc              = org.lyranthe.fs2_grpc.buildinfo.BuildInfo.grpcVersion
    val disciplineSpecs2  = "1.1.0"
    val specs2            = "4.9.4"
    val catsEffectTesting = "0.4.0"
    val kindProjector     = "0.11.0"

  }

  // Compile

  val cats         = "org.typelevel"        %% "cats-core"                      % versions.catsCore
  val catsLaws     = "org.typelevel"        %% "cats-laws"                      % versions.catsCore
  val catsEffect   = "org.typelevel"        %% "cats-effect"                    % versions.catsEffect
  val fs2          = "co.fs2"               %% "fs2-core"                       % versions.fs2
  val scodecBits   = "org.scodec"           %% "scodec-bits"                    % versions.scodecBits
  val circe        = "io.circe"             %% "circe-core"                     % versions.circe
  val circeParser  = "io.circe"             %% "circe-parser"                   % versions.circe
  val circeGeneric = "io.circe"             %% "circe-generic"                  % versions.circe
  val scalaPb      = "com.thesamet.scalapb" %% "scalapb-runtime"                % versions.scalaPb
  val grpcNetty    = "io.grpc"              % "grpc-netty"                      % versions.grpc
  val tcnative     = "io.netty"             % "netty-tcnative-boringssl-static" % "2.0.28.Final"

  // Testing

  val specs2            = "org.specs2"     %% "specs2-core"                % versions.specs2
  val specs2ScalaCheck  = "org.specs2"     %% "specs2-scalacheck"          % versions.specs2
  val specs2Cats        = "org.specs2"     %% "specs2-cats"                % versions.specs2
  val disciplineSpecs2  = "org.typelevel"  %% "discipline-specs2"          % versions.disciplineSpecs2
  val catsEffectTesting = "com.codecommit" %% "cats-effect-testing-specs2" % versions.catsEffectTesting

  // Compiler & SBT Plugins

  val kindProjector = "org.typelevel" %% "kind-projector" % versions.kindProjector cross CrossVersion.full

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
