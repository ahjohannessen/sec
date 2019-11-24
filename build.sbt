import Dependencies._

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core)

lazy val core = project
  .enablePlugins(Fs2Grpc)
  .settings(commonSettings)
  .settings(
    name := "sec",
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    libraryDependencies ++= compileM(
      cats, catsEffect, fs2, scodecBits, circe, circeParser,
      scalaPb, grpcNetty, tcnative
    ) ++ protobufM(scalaPb)
  )

// General Settings
lazy val commonSettings = Seq(
  scalaVersion := "2.13.1",
  addCompilerPlugin(kindProjector),
  libraryDependencies ++= testM(specs2, specs2ScalaCheck)
)

inThisBuild(
  List(
    organization := "io.ahjohannessen",
    developers := List(
      Developer(
        "ahjohannessen",
        "Alex Henning Johannessen",
        "ahjohannessen@gmail.com",
        url("https://github.com/ahjohannessen")
      )
    ),
    homepage := Some(url("https://github.com/ahjohannessen/sec")),
    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
    pomIncludeRepository := { _ =>
      false
    },
    scalacOptions in (Compile, doc) ++= Seq(
      "-groups",
      "-sourcepath",
      (baseDirectory in LocalRootProject).value.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/ahjohannessen/sec/blob/v" + version.value + "€{FILE_PATH}.scala"
    )
  )
)
