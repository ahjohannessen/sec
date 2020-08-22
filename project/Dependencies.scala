import sbt._

object Dependencies {

  object versions {

    val catsCore          = "2.1.1"
    val catsEffect        = "2.1.4"
    val fs2               = "2.4.4"
    val scodecBits        = "1.1.18"
    val circe             = "0.13.0"
    val scalaPb           = scalapb.compiler.Version.scalapbVersion
    val grpc              = org.lyranthe.fs2_grpc.buildinfo.BuildInfo.grpcVersion
    val disciplineSpecs2  = "1.1.0"
    val specs2            = "4.10.3"
    val catsEffectTesting = "0.4.1"
    val kindProjector     = "0.11.0"

  }

  // Compile

  val cats        = "org.typelevel"        %% "cats-core"       % versions.catsCore
  val catsEffect  = "org.typelevel"        %% "cats-effect"     % versions.catsEffect
  val fs2         = "co.fs2"               %% "fs2-core"        % versions.fs2
  val scodecBits  = "org.scodec"           %% "scodec-bits"     % versions.scodecBits
  val circe       = "io.circe"             %% "circe-core"      % versions.circe
  val circeParser = "io.circe"             %% "circe-parser"    % versions.circe
  val scalaPb     = "com.thesamet.scalapb" %% "scalapb-runtime" % versions.scalaPb

  // Testing

  val grpcNetty = "io.grpc"  % "grpc-netty"                      % versions.grpc
  val tcnative  = "io.netty" % "netty-tcnative-boringssl-static" % "2.0.28.Final"

  val specs2            = "org.specs2"     %% "specs2-core"                % versions.specs2
  val specs2ScalaCheck  = "org.specs2"     %% "specs2-scalacheck"          % versions.specs2
  val specs2Cats        = "org.specs2"     %% "specs2-cats"                % versions.specs2
  val circeGeneric      = "io.circe"       %% "circe-generic"              % versions.circe
  val disciplineSpecs2  = "org.typelevel"  %% "discipline-specs2"          % versions.disciplineSpecs2
  val catsLaws          = "org.typelevel"  %% "cats-laws"                  % versions.catsCore
  val catsEffectTesting = "com.codecommit" %% "cats-effect-testing-specs2" % versions.catsEffectTesting
  val catsEffectLaws    = "org.typelevel"  %% "cats-effect-laws"           % versions.catsEffect

  // Compiler & SBT Plugins

  val kindProjector = "org.typelevel" %% "kind-projector" % versions.kindProjector cross CrossVersion.full

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
