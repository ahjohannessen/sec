import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val Scala2 = "2.13.4"
lazy val Scala3 = "3.0.0-M2"

lazy val sec = project
  .in(file("."))
  .settings(skip in publish := true)
  .dependsOn(core, `fs2-core`, `fs2-netty`, tests, docs)
  .aggregate(core, `fs2-core`, `fs2-netty`, tests, docs)

//==== Core ============================================================================================================

lazy val core = project
  .in(file("core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    libraryDependencies ++=
      compileM(cats, scodecBits, circe, scalaPb, grpcApi, grpcStub, grpcProtobuf, grpcCore),
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf"),
    Compile / PB.targets := Seq(scalapb.gen(flatPackage = true, grpc = false) -> (sourceManaged in Compile).value)
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))

//==== FS2 =============================================================================================================

lazy val `fs2-core` = project
  .in(file("fs2"))
  .enablePlugins(AutomateHeaderPlugin, Fs2Grpc)
  .settings(commonSettings)
  .settings(
    name := "sec-fs2",
    libraryDependencies ++=
      compileM(cats, catsEffect, fs2, log4cats, log4catsNoop, scodecBits, circe, circeParser),
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf")
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(core)

lazy val `fs2-netty` = project
  .in(file("fs2-netty"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec-fs2-client", libraryDependencies ++= compileM(grpcNetty))
  .dependsOn(`fs2-core`)

//==== Tests ===========================================================================================================

lazy val SingleNodeITest = config("sit") extend Test
lazy val ClusterITest    = config("cit") extend Test

lazy val integrationSettings = Defaults.testSettings ++ Seq(
  parallelExecution := false
)

lazy val tests = project
  .in(file("tests"))
  .enablePlugins(BuildInfoPlugin, AutomateHeaderPlugin)
  .configs(SingleNodeITest, ClusterITest)
  .settings(commonSettings)
  .settings(inConfig(SingleNodeITest)(integrationSettings ++ scalafixConfigSettings(SingleNodeITest)))
  .settings(inConfig(ClusterITest)(integrationSettings ++ scalafixConfigSettings(ClusterITest)))
  .settings(
    skip in publish := true,
    buildInfoPackage := "sec",
    buildInfoKeys := Seq(BuildInfoKey("certsPath" -> file("").getAbsoluteFile.toPath / "certs")),
    Test / headerSources ++= sources.in(SingleNodeITest).value ++ sources.in(ClusterITest).value,
    libraryDependencies ++=
      compileM(catsLaws, catsEffectLaws, disciplineSpecs2, specs2ScalaCheck, specs2Cats) ++
        compileM(specs2, catsEffectSpecs2, log4catsSlf4j, log4catsTesting, logback)
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(core, `fs2-netty`)

//==== Docs ============================================================================================================

lazy val docs = project
  .in(file("sec-docs"))
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .dependsOn(`fs2-netty`)
  .settings(
    crossScalaVersions := Seq(Scala2),
    publish / skip := true,
    moduleName := "sec-docs",
    mdocIn := file("docs"),
    mdocVariables := Map(
      "libName"       -> "sec",
      "libVersion"    -> version.value.takeWhile(_ != '+'), // strip off the SNAPSHOT business
      "libGithubRepo" -> "https://github.com/ahjohannessen/sec",
      "grpcVersion"   -> versions.grpc,
      "esdb"          -> "EventStoreDB"
    )
  )

//==== Common ==========================================================================================================

lazy val commonSettings = Seq(
  scalacOptions ++= {
    if (isDotty.value) Seq("-source:3.0-migration") else Nil
  },
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (isDotty.value) Nil else old
  },
  Compile / doc / scalacOptions ~=
    (_.filterNot(_ == "-Xfatal-warnings"))
)

inThisBuild(
  List(
    scalaVersion := crossScalaVersions.value.last,
    crossScalaVersions := Seq(Scala3, Scala2),
    scalacOptions ++= Seq("-target:jvm-1.8"),
    javacOptions ++= Seq("-target", "8", "-source", "8"),
    organization := "io.github.ahjohannessen",
    organizationName := "Scala EventStoreDB Client",
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
    scalacOptions in (Compile, doc) ++= Seq(
      "-groups",
      "-sourcepath",
      (baseDirectory in LocalRootProject).value.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/ahjohannessen/sec/blob/v" + version.value + "€{FILE_PATH}.scala"
    )
// TODO: Include when Scala 3.0 is supported
//,
//    scalafixDependencies += scalafixOrganizeImports,
//    semanticdbEnabled := true,
//    semanticdbVersion := scalafixSemanticdb.revision,
//    scalafixOnCompile := sys.env.get("CI").fold(true)(_ => false)
  )
)

//==== Github Actions ==================================================================================================

addCommandAlias("compileTests", "tests/test:compile; tests/sit:compile; tests/cit:compile;")
addCommandAlias("compileDocs", "docs/mdoc")

def scalaCondition(version: String) = s"contains(matrix.scala, '$version')"

inThisBuild(
  List(
    githubWorkflowJavaVersions := Seq("adopt@1.11"),
    githubWorkflowTargetTags += "v*",
    githubWorkflowTargetBranches := Seq("master"),
    githubWorkflowBuildPreamble += WorkflowStep.Run(
      name     = Some("Start Single Node"),
      commands = List(".docker/single-node.sh up -d"),
      cond     = Some(scalaCondition(scalaVersion.value))
    ),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name     = Some("Compile docs"),
        commands = List("compileDocs"),
        cond     = Some(scalaCondition(scalaVersion.value))
      ),
      WorkflowStep.Sbt(
        name     = Some("Regular tests"),
        commands = List("compileTests", "tests/test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Single node integration tests"),
        commands = List("tests/sit:test"),
        env = Map(
          "SEC_SIT_CERTS_PATH" -> "${{ github.workspace }}/certs",
          "SEC_SIT_AUTHORITY"  -> "es.sec.local"
        ),
        cond = Some(scalaCondition(scalaVersion.value))
      )
    ),
    githubWorkflowBuildPostamble += WorkflowStep.Run(
      name     = Some("Stop Single Node"),
      commands = List(".docker/single-node.sh down"),
      cond     = Some(s"always() && ${scalaCondition(scalaVersion.value)}")
    ),
    githubWorkflowBuildPostamble ++= Seq(
      WorkflowStep.Run(
        name     = Some("Start Cluster Nodes"),
        commands = List(".docker/cluster.sh up -d"),
        cond     = Some(scalaCondition(scalaVersion.value))
      ),
      WorkflowStep.Sbt(
        name     = Some("Cluster integration tests"),
        commands = List("tests/cit:test"),
        env = Map(
          "SEC_CIT_CERTS_PATH" -> "${{ github.workspace }}/certs",
          "SEC_CIT_AUTHORITY"  -> "es.sec.local"
        ),
        cond = Some(scalaCondition(scalaVersion.value))
      ),
      WorkflowStep.Run(
        name     = Some("Stop Cluster Nodes"),
        commands = List(".docker/cluster.sh down"),
        cond     = Some(s"always() && ${scalaCondition(scalaVersion.value)}")
      )
    ),
    githubWorkflowPublishTargetBranches := Seq(
      RefPredicate.Equals(Ref.Branch("master")),
      RefPredicate.StartsWith(Ref.Tag("v"))
    ),
    githubWorkflowPublishPreamble +=
      WorkflowStep.Use("olafurpg", "setup-gpg", "v3"),
    githubWorkflowPublish := Seq(
      WorkflowStep.Sbt(
        List("ci-release"),
        env = Map(
          "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
          "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
          "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
          "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}",
          "GIT_DEPLOY_KEY"    -> "${{ secrets.GIT_DEPLOY_KEY }}"
        )
      ),
      WorkflowStep.Sbt(
        List("docs/docusaurusPublishGhpages"),
        env = Map(
          "GIT_DEPLOY_KEY" -> "${{ secrets.GIT_DEPLOY_KEY }}"
        ),
        cond = Some(scalaCondition(scalaVersion.value))
      )
    )
  )
)
