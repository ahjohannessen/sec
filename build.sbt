import Dependencies._

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .aggregate(core, netty, demo)

lazy val basePath     = file("").getAbsoluteFile.toPath
lazy val certsPathKey = "certsPath" -> basePath / "certs"

lazy val IntegrationTest = config("it") extend Test

lazy val core = project
  .enablePlugins(Fs2Grpc)
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    buildInfoPackage := "sec",
    Test / testOptions := Seq(Tests.Filter(_ endsWith "Spec")),
    IntegrationTest / testOptions := Seq(Tests.Filter(_ endsWith "ITest")),
    IntegrationTest / parallelExecution := false,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    libraryDependencies ++=
      compileM(cats, catsEffect, fs2, log4cats, log4catsNoop, scodecBits, circe, circeParser, scalaPb) ++
        protobufM(scalaPb) ++
        testM(specs2Cats, catsEffectTesting, catsEffectLaws, grpcNetty, tcnative, log4catsSlf4j, logback)
  )
  .settings(
    addBuildInfoToConfig(Test),
    Test / buildInfoKeys := buildInfoKeys.value :+ BuildInfoKey(certsPathKey),
    Test / buildInfoObject := "TestBuildInfo"
  )
  .settings(
    libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value))
  )

lazy val netty = project
  .in(file("netty"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "sec",
    libraryDependencies ++= compileM(grpcNetty, tcnative)
  )

lazy val demo = project
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(netty)
  .settings(
    name := "demo",
    buildInfoKeys := buildInfoKeys.value :+ BuildInfoKey(certsPathKey),
    buildInfoPackage := "sec.demo",
    libraryDependencies ++= compileM(log4catsSlf4j, logback).map(_.withDottyCompat(scalaVersion.value)),
    skip in publish := true
  )

// General Settings

lazy val commonSettings =
  AutomateHeaderPlugin.projectSettings ++
    Seq(
      scalacOptions ++= {
        if (isDotty.value) Seq("-source:3.0-migration") else Nil
      },
      libraryDependencies ++=
        testM(catsLaws, disciplineSpecs2, specs2, specs2ScalaCheck).map(_.withDottyCompat(scalaVersion.value)),
      pomPostProcess := { node =>
        import scala.xml._
        import scala.xml.transform._
        def stripIf(f: Node => Boolean) = new RewriteRule {
          override def transform(n: Node) = if (f(n)) NodeSeq.Empty else n
        }
        val stripTestScope = stripIf(n => n.label == "dependency" && (n \ "scope").text == "test")
        new RuleTransformer(stripTestScope).transform(node)(0)
      },
      Compile / doc / sources := {
        val old = (Compile / doc / sources).value
        if (isDotty.value) Nil else old
      }
    )

inThisBuild(
  List(
    scalaVersion := crossScalaVersions.value.last,
    crossScalaVersions := Seq("0.27.0-RC1", "2.13.3"),
    scalacOptions ++= Seq("-target:jvm-1.8"),
    javacOptions ++= Seq("-target", "8", "-source", "8"),
    organization := "io.github.ahjohannessen",
    organizationName := "Alex Henning Johannessen",
    homepage := Some(url("https://github.com/ahjohannessen/sec")),
    scmInfo := Some(ScmInfo(url("https://github.com/ahjohannessen/sec"), "git@github.com:ahjohannessen/sec.git")),
    startYear := Some(2020),
    licenses += (("Apache-2.0", url("http://www.apache.org/licenses/"))),
    developers := List(
      Developer(
        "ahjohannessen",
        "Alex Henning Johannessen",
        "ahjohannessen@gmail.com",
        url("https://github.com/ahjohannessen")
      )),
    shellPrompt := Prompt.enrichedShellPrompt,
    pomIncludeRepository := { _ => false },
    scalacOptions in (Compile, doc) ++= Seq(
      "-groups",
      "-sourcepath",
      (baseDirectory in LocalRootProject).value.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/ahjohannessen/sec/blob/v" + version.value + "€{FILE_PATH}.scala"
    )
  )
)

// Github Actions

inThisBuild(
  List(
    githubWorkflowJavaVersions := Seq("adopt@1.11"),
    githubWorkflowTargetTags += "v*",
    githubWorkflowBuildPreamble += WorkflowStep.Run(
      name     = Some("Start EventStore Nodes"),
      commands = List(".docker/single-node.sh up -d", ".docker/cluster.sh up -d")
    ),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name     = Some("Run tests"),
        commands = List("core/test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Run single node integration tests"),
        commands = List("core/it:test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Run cluster integration tests"),
        commands = List("demo/run"),
        env = Map(
          "SEC_DEMO_CERTS_PATH" -> "${{ github.workspace }}/certs",
          "SEC_DEMO_AUTHORITY"  -> "es.sec.local"
        )
      )
    ),
    githubWorkflowBuildPostamble += WorkflowStep.Run(
      name     = Some("Stop EventStore Nodes"),
      commands = List(".docker/single-node.sh down", ".docker/cluster.sh down"),
      cond     = Some("always()")
    ),
    githubWorkflowPublishTargetBranches += RefPredicate.StartsWith(Ref.Tag("v")),
    githubWorkflowPublishPreamble +=
      WorkflowStep.Use("olafurpg", "setup-gpg", "v2"),
    githubWorkflowPublish := Seq(
      WorkflowStep.Sbt(
        List("ci-release"),
        env = Map(
          "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
          "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
          "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
          "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
        )
      ))
  )
)
