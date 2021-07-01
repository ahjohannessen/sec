import sbt._
import sbt.Keys._

object Dependencies {

  object versions {

    val catsCore         = "2.6.1"
    val catsEffect       = "3.1.1"
    val fs2              = "3.0.5"
    val log4cats         = "2.1.1"
    val logback          = "1.2.3"
    val scodecBits       = "1.1.27"
    val circe            = "0.14.1"
    val scalaPb          = scalapb.compiler.Version.scalapbVersion
    val grpc             = "1.39.0"
    val disciplineSpecs2 = "1.1.6"
    val specs2           = "4.12.2"
    val catsEffectSpecs2 = "1.1.1"
    val tsConfig         = "1.4.1"

  }

  // Compile

  val cats         = "org.typelevel"        %% "cats-core"         % versions.catsCore
  val catsEffect   = "org.typelevel"        %% "cats-effect"       % versions.catsEffect
  val fs2          = "co.fs2"               %% "fs2-core"          % versions.fs2
  val log4cats     = "org.typelevel"        %% "log4cats-core"     % versions.log4cats
  val log4catsNoop = "org.typelevel"        %% "log4cats-noop"     % versions.log4cats
  val scodecBits   = "org.scodec"           %% "scodec-bits"       % versions.scodecBits
  val circe        = "io.circe"             %% "circe-core"        % versions.circe
  val circeParser  = "io.circe"             %% "circe-parser"      % versions.circe
  val scalaPb      = "com.thesamet.scalapb" %% "scalapb-runtime"   % versions.scalaPb
  val grpcApi      = "io.grpc"               % "grpc-api"          % versions.grpc
  val grpcStub     = "io.grpc"               % "grpc-stub"         % versions.grpc
  val grpcCore     = "io.grpc"               % "grpc-core"         % versions.grpc
  val grpcProtobuf = "io.grpc"               % "grpc-protobuf"     % versions.grpc
  val grpcNetty    = "io.grpc"               % "grpc-netty-shaded" % versions.grpc
  val tsConfig     = "com.typesafe"          % "config"            % versions.tsConfig

  // Testing

  val specs2            = "org.specs2"    %% "specs2-core"                % versions.specs2
  val specs2ScalaCheck  = "org.specs2"    %% "specs2-scalacheck"          % versions.specs2
  val specs2Cats        = "org.specs2"    %% "specs2-cats"                % versions.specs2
  val disciplineSpecs2  = "org.typelevel" %% "discipline-specs2"          % versions.disciplineSpecs2
  val catsLaws          = "org.typelevel" %% "cats-laws"                  % versions.catsCore
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % versions.catsEffect
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % versions.catsEffectSpecs2
  val logback           = "ch.qos.logback" % "logback-classic"            % versions.logback
  val log4catsTesting   = "org.typelevel" %% "log4cats-testing"           % versions.log4cats
  val log4catsSlf4j     = "org.typelevel" %% "log4cats-slf4j"             % versions.log4cats

  // Scalafix

  val scalafixOrganizeImports = "com.github.liancheng" %% "organize-imports" % "0.4.3"

  // Misc

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
