import Dependencies._

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .aggregate(core, netty, demo)

Global / onLoad ~= (_ andThen ("project core" :: _))

lazy val basePath = file("").getAbsoluteFile.toPath

lazy val IntegrationTest = config("it") extend Test

lazy val core = project
  .enablePlugins(Fs2Grpc)
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    // TODO: only produce certsPath in tests
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, "certsPath" -> basePath / "certs"),
    buildInfoPackage := "sec",
    Test / testOptions := Seq(Tests.Filter(_ endsWith "Spec")),
    IntegrationTest / testOptions := Seq(Tests.Filter(_ endsWith "ITest")),
    IntegrationTest / parallelExecution := false,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    libraryDependencies ++=
      compileM(cats, catsEffect, fs2, log4cats, scodecBits, circe, circeParser, scalaPb) ++ protobufM(scalaPb) ++
        testM(
          specs2Cats,
          catsEffectTesting,
          catsEffectLaws,
          specs2ScalaCheck,
          circeGeneric,
          grpcNetty,
          log4catsSlf4j,
          log4catsNoop,
          logback
        )
  )

lazy val netty = project
  .in(file("netty"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "sec",
    libraryDependencies ++= compileM(grpcNetty)
  )

lazy val demo = project
  .dependsOn(netty)
  .settings(
    name := "demo",
    libraryDependencies ++= compileM(log4catsSlf4j, logback),
    skip in publish := true
  )

// General Settings

lazy val commonSettings = Seq(
  addCompilerPlugin(kindProjector),
  Compile / scalacOptions ~= devScalacOptions,
  Test / scalacOptions ~= devScalacOptions,
  IntegrationTest / scalacOptions ~= devScalacOptions,
  libraryDependencies ++= testM(catsLaws, disciplineSpecs2, specs2, specs2ScalaCheck)
)

lazy val metalsEnabled =
  scala.util.Properties.envOrElse("METALS_ENABLED", "false").toBoolean

val devScalacOptions = { options: Seq[String] =>
  if (metalsEnabled)
    options.filterNot(Set("-Wunused:locals", "-Wunused:params", "-Wunused:imports", "-Wunused:privates"))
  else options
}

inThisBuild(
  List(
    scalaVersion := "2.13.2",
    organization := "io.github.ahjohannessen",
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
    ),
    shellPrompt := Prompt.enrichedShellPrompt
  )
)
