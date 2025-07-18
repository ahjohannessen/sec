import sbt._
import sbt.Keys._

object Dependencies {

  object versions {

    val catsCore   = "2.13.0"
    val catsEffect = "3.6.2"
    val fs2        = "3.12.0"
    val ip4s       = "3.7.0"
    val log4cats   = "2.7.1"
    val logback    = "1.2.13"
    val scodecBits = "1.2.4"
    val unum       = "1.2.6"
    val circe      = "0.14.3"
    val scalaPb    = scalapb.compiler.Version.scalapbVersion
    val grpc       = "1.73.0"
    val tsConfig   = "1.4.4"

    val scalaCheck      = "1.18.1"
    val munitDiscipline = "2.0.0"
    val munitEffect     = "2.1.0"
    val munit           = "1.1.1"

  }

  // Compile

  val cats         = "org.typelevel"           %% "cats-core"         % versions.catsCore
  val catsEffect   = "org.typelevel"           %% "cats-effect"       % versions.catsEffect
  val fs2          = "co.fs2"                  %% "fs2-core"          % versions.fs2
  val fs2Io        = "co.fs2"                  %% "fs2-io"            % versions.fs2
  val ip4s         = "com.comcast"             %% "ip4s-core"         % versions.ip4s
  val log4cats     = "org.typelevel"           %% "log4cats-core"     % versions.log4cats
  val log4catsNoop = "org.typelevel"           %% "log4cats-noop"     % versions.log4cats
  val unum         = "io.github.ahjohannessen" %% "unum"              % versions.unum
  val scodecBits   = "org.scodec"              %% "scodec-bits"       % versions.scodecBits
  val circe        = "io.circe"                %% "circe-core"        % versions.circe
  val circeParser  = "io.circe"                %% "circe-parser"      % versions.circe
  val scalaPb      = "com.thesamet.scalapb"    %% "scalapb-runtime"   % versions.scalaPb
  val grpcApi      = "io.grpc"                  % "grpc-api"          % versions.grpc
  val grpcStub     = "io.grpc"                  % "grpc-stub"         % versions.grpc
  val grpcCore     = "io.grpc"                  % "grpc-core"         % versions.grpc
  val grpcProtobuf = "io.grpc"                  % "grpc-protobuf"     % versions.grpc
  val grpcNetty    = "io.grpc"                  % "grpc-netty-shaded" % versions.grpc
  val tsConfig     = "com.typesafe"             % "config"            % versions.tsConfig

  // Testing

  val munit             = "org.scalameta"  %% "munit"               % versions.munit
  val catsLaws          = "org.typelevel"  %% "cats-laws"           % versions.catsCore
  val scalaCheck        = "org.scalacheck" %% "scalacheck"          % versions.scalaCheck
  val munitDiscipline   = "org.typelevel"  %% "discipline-munit"    % versions.munitDiscipline
  val munitEffect       = "org.typelevel"  %% "munit-cats-effect"   % versions.munitEffect
  val catsEffectTestkit = "org.typelevel"  %% "cats-effect-testkit" % versions.catsEffect

  val logback         = "ch.qos.logback" % "logback-classic"  % versions.logback
  val log4catsTesting = "org.typelevel" %% "log4cats-testing" % versions.log4cats
  val log4catsSlf4j   = "org.typelevel" %% "log4cats-slf4j"   % versions.log4cats

  // Misc

  def protobufM(mids: ModuleID*): Seq[ModuleID] = mids.map(_ % "protobuf")
  def compileM(mids: ModuleID*): Seq[ModuleID]  = mids.map(_ % Compile)
  def testM(mids: ModuleID*): Seq[ModuleID]     = mids.map(_ % Test)

}
