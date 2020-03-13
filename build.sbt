import Dependencies._

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .aggregate(core)

lazy val IntegrationTest = config("it") extend Test

lazy val core = project
  .enablePlugins(Fs2Grpc)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  .settings(commonSettings)
  .settings(
    name := "sec",
    Test            / testOptions := Seq(Tests.Filter(_ endsWith "Spec")),
    IntegrationTest / testOptions := Seq(Tests.Filter(_ endsWith "ITest")),
    IntegrationTest / parallelExecution := false,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    libraryDependencies ++=
      compileM(cats, catsEffect, fs2, scodecBits, circe, circeParser, scalaPb) ++
        protobufM(scalaPb) ++
        testM(specs2Cats, catsEffectTesting, specs2ScalaCheck, circeGeneric, grpcNetty, tcnative)
  )

// General Settings
lazy val commonSettings = Seq(
  addCompilerPlugin(kindProjector),
  Compile         / scalacOptions ~= devScalacOptions,
  Test            / scalacOptions ~= devScalacOptions,
  IntegrationTest / scalacOptions ~= devScalacOptions,
  libraryDependencies ++= testM(catsLaws, disciplineSpecs2, specs2, specs2ScalaCheck)
)

lazy val metalsEnabled = 
   scala.util.Properties.envOrElse("METALS_ENABLED", "false").toBoolean

val devScalacOptions = { options: Seq[String] =>
  if (metalsEnabled) options.filterNot(Set("-Wunused:locals", "-Wunused:params", "-Wunused:imports", "-Wunused:privates")) else options
}

inThisBuild(
  List(
    scalaVersion := "2.13.1",
    organization := "io.github.ahjohannessen",
    developers := List(
      Developer("ahjohannessen", "Alex Henning Johannessen", "ahjohannessen@gmail.com", url("https://github.com/ahjohannessen"))
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
