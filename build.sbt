import Dependencies._

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .dependsOn(`sec-protos`, `sec-core`, `sec-netty`, `sec-tests`)
  .aggregate(`sec-protos`, `sec-core`, `sec-netty`, `sec-tests`)

lazy val `sec-protos` = project
  .in(file("sec-protos"))
  .enablePlugins(Fs2Grpc)
  .settings(commonSettings)
  .settings(
    name := "sec-protos",
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    libraryDependencies ++= compileM(scalaPb)
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))

lazy val `sec-core` = project
  .in(file("sec-core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    libraryDependencies ++= compileM(cats, catsEffect, fs2, log4cats, log4catsNoop, scodecBits, circe, circeParser)
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(`sec-protos`)

lazy val `sec-netty` = project
  .in(file("sec-netty"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec", libraryDependencies ++= compileM(grpcNetty, tcnative))
  .dependsOn(`sec-core`)

lazy val SingleNodeITest = config("sit") extend Test
lazy val ClusterITest    = config("cit") extend Test

lazy val `sec-tests` = project
  .in(file("sec-tests"))
  .enablePlugins(BuildInfoPlugin, AutomateHeaderPlugin)
  .configs(SingleNodeITest, ClusterITest)
  .settings(commonSettings)
  .settings(inConfig(SingleNodeITest)(Defaults.testSettings ++ Seq(parallelExecution := false)))
  .settings(inConfig(ClusterITest)(Defaults.testSettings ++ Seq(parallelExecution := false)))
  .settings(
    skip in publish := true,
    buildInfoPackage := "sec",
    buildInfoKeys := Seq(BuildInfoKey("certsPath" -> file("").getAbsoluteFile.toPath / "certs")),
    Test / headerSources ++= sources.in(SingleNodeITest).value ++ sources.in(ClusterITest).value,
    libraryDependencies ++= testM(
      catsLaws,
      disciplineSpecs2,
      specs2,
      specs2ScalaCheck,
      specs2Cats,
      catsEffectTesting,
      catsEffectLaws,
      log4catsSlf4j,
      logback
    )
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(`sec-core`, `sec-netty`)

// General Settings

lazy val commonSettings = Seq(
  scalacOptions ++= {
    if (isDotty.value) Seq("-source:3.0-migration") else Nil
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
    githubWorkflowTargetBranches := Seq("master"),
    githubWorkflowBuildPreamble += WorkflowStep.Run(
      name     = Some("Start EventStore Nodes"),
      commands = List(".docker/single-node.sh up -d", ".docker/cluster.sh up -d")
    ),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name     = Some("Run tests"),
        commands = List("compile", "sec-tests/test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Run single node integration tests"),
        commands = List("sec-tests/sit:test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Run cluster integration tests"),
        commands = List("sec-tests/cit:test"),
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
    githubWorkflowPublishTargetBranches := Seq(
      RefPredicate.Equals(Ref.Branch("master")),
      RefPredicate.StartsWith(Ref.Tag("v"))
    ),
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
