import sbt._
import sbt.Keys._

object Dependencies {

  object versions {

    val catsCore          = "2.2.0"
    val catsEffect        = "2.1.4"
    val fs2               = "2.4.4"
    val log4cats          = "1.1.1"
    val logback           = "1.2.3"
    val scodecBits        = "1.1.20"
    val circe             = "0.13.0"
    val scalaPb           = scalapb.compiler.Version.scalapbVersion
    val grpc              = org.lyranthe.fs2_grpc.buildinfo.BuildInfo.grpcVersion
    val tcnative          = "2.0.30.Final"
    val disciplineSpecs2  = "1.1.0"
    val specs2            = "4.10.3"
    val catsEffectTesting = "0.4.1"
    val weaver            = "0.4.3"

  }

  // Compile

  val cats        = "org.typelevel"        %% "cats-core"       % versions.catsCore
  val catsEffect  = "org.typelevel"        %% "cats-effect"     % versions.catsEffect
  val fs2         = "co.fs2"               %% "fs2-core"        % versions.fs2
  val log4cats    = "io.chrisdavenport"    %% "log4cats-core"   % versions.log4cats
  val scodecBits  = "org.scodec"           %% "scodec-bits"     % versions.scodecBits
  val circe       = "io.circe"             %% "circe-core"      % versions.circe
  val circeParser = "io.circe"             %% "circe-parser"    % versions.circe
  val scalaPb     = "com.thesamet.scalapb" %% "scalapb-runtime" % versions.scalaPb

  // Testing & Demo

  val specs2            = "org.specs2"          %% "specs2-core"                     % versions.specs2
  val specs2ScalaCheck  = "org.specs2"          %% "specs2-scalacheck"               % versions.specs2
  val specs2Cats        = "org.specs2"          %% "specs2-cats"                     % versions.specs2
  val disciplineSpecs2  = "org.typelevel"       %% "discipline-specs2"               % versions.disciplineSpecs2
  val catsLaws          = "org.typelevel"       %% "cats-laws"                       % versions.catsCore
  val catsEffectLaws    = "org.typelevel"       %% "cats-effect-laws"                % versions.catsEffect
  val weawer            = "com.disneystreaming" %% "weaver-framework"                % versions.weaver
  val weawerSpecs       = "com.disneystreaming" %% "weaver-specs2"                   % versions.weaver
  val catsEffectTesting = "com.codecommit"      %% "cats-effect-testing-specs2"      % versions.catsEffectTesting
  val logback           = "ch.qos.logback"       % "logback-classic"                 % versions.logback
  val log4catsNoop      = "io.chrisdavenport"   %% "log4cats-noop"                   % versions.log4cats
  val log4catsSlf4j     = "io.chrisdavenport"   %% "log4cats-slf4j"                  % versions.log4cats
  val grpcNetty         = "io.grpc"              % "grpc-netty"                      % versions.grpc
  val tcnative          = "io.netty"             % "netty-tcnative-boringssl-static" % versions.tcnative

  // Misc

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
