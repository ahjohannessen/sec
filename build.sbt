import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / lintUnusedKeysOnLoad := false

ThisBuild / assumedVersionScheme := VersionScheme.Always
ThisBuild / evictionErrorLevel := Level.Info

lazy val Scala2  = "2.13.6"
lazy val Scala3  = "3.0.0"
lazy val isScala3 = Def.setting[Boolean](scalaVersion.value.startsWith("3."))

lazy val sec = project
  .in(file("."))
  .settings(publish / skip := true)
  .dependsOn(core, `fs2-core`, `fs2-netty`, tsc, tests, docs)
  .aggregate(core, `fs2-core`, `fs2-netty`, tsc, tests, docs)

//==== Core ============================================================================================================

lazy val core = project
  .in(file("core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    libraryDependencies ++=
      compileM(grpcApi, grpcStub, grpcProtobuf, grpcCore) ++
        compileM(cats, scodecBits, circe, scalaPb),
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf"),
    Compile / PB.targets := Seq(scalapb.gen(flatPackage = true, grpc = false) -> (Compile / sourceManaged).value)
  )

//==== FS2 =============================================================================================================

lazy val `fs2-core` = project
  .in(file("fs2"))
  .enablePlugins(AutomateHeaderPlugin, Fs2Grpc)
  .settings(commonSettings)
  .settings(
    name := "sec-fs2",
    libraryDependencies ++=
      compileM(grpcApi, grpcStub, grpcProtobuf, grpcCore) ++
        compileM(cats, catsEffect, fs2, log4cats, log4catsNoop, scodecBits, circe, circeParser),
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf")
  )
  .dependsOn(core)

lazy val `fs2-netty` = project
  .in(file("fs2-netty"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec-fs2-client", libraryDependencies ++= compileM(grpcNetty))
  .dependsOn(`fs2-core`, tsc)

//==== Config ==========================================================================================================

lazy val tsc = project
  .in(file("tsc"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec-tsc", libraryDependencies ++= compileM(tsConfig))
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
    publish / skip := true,
    buildInfoPackage := "sec",
    buildInfoKeys := Seq(BuildInfoKey("certsPath" -> file("").getAbsoluteFile.toPath / "certs")),
    Test / headerSources ++= (SingleNodeITest / sources).value ++ (ClusterITest / sources).value,
    libraryDependencies := {
      if (isScala3.value)
        compileM(
          specs2.cross(CrossVersion.for3Use2_13),
          specs2ScalaCheck.cross(CrossVersion.for3Use2_13),
          specs2Cats.cross(CrossVersion.for3Use2_13),
          catsEffectSpecs2.cross(CrossVersion.for3Use2_13),
          disciplineSpecs2,
          catsLaws,
          catsEffectTestkit,
          log4catsSlf4j,
          log4catsTesting,
          logback
        ).map(
          _.exclude("org.typelevel", "cats-effect_2.13")
            .exclude("org.scalacheck", "scalacheck_2.13")
            .exclude("org.typelevel", "cats-core_2.13"))
      else
        compileM(catsEffect,
                 specs2,
                 specs2ScalaCheck,
                 specs2Cats,
                 catsEffectSpecs2,
                 disciplineSpecs2,
                 catsLaws,
                 catsEffectTestkit,
                 log4catsSlf4j,
                 log4catsTesting,
                 logback)
    }
  )
  .dependsOn(core, `fs2-netty`)

//==== Docs ============================================================================================================

lazy val docs = project
  .in(file("sec-docs"))
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .dependsOn(`fs2-netty`)
  .settings(
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
    if (isScala3.value) Seq("-source:3.0-migration") else Nil
  },
  scalacOptions ++= {
    if (isScala3.value) Seq("-Xtarget:8") else Seq("-target:8")
  },
  Compile / doc / scalacOptions ~=
    (_.filterNot(_ == "-Xfatal-warnings"))
)

inThisBuild(
  List(
    scalaVersion := crossScalaVersions.value.head,
    crossScalaVersions := Seq(Scala3, Scala2),
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
    (Compile / doc / scalacOptions) ++= Seq(
      "-groups",
      "-sourcepath",
      (LocalRootProject / baseDirectory).value.getAbsolutePath,
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

addCommandAlias("compileTests", "tests / Test / compile; tests / Sit / compile; tests / Cit / compile;")
addCommandAlias("compileDocs", "docs/mdoc")

def scalaCondition(version: String) = s"contains(matrix.scala, '$version')"
val docsOnMain                      = "github.ref == 'refs/heads/main'"

inThisBuild(
  List(
    githubWorkflowJavaVersions := Seq("adopt@1.11"),
    githubWorkflowTargetTags += "v*",
    githubWorkflowTargetBranches := Seq("main"),
    githubWorkflowBuildPreamble += WorkflowStep.Run(
      name     = Some("Start Single Node"),
      commands = List("pushd .docker", "./single-node.sh up -d", "popd"),
      cond     = Some(scalaCondition(Scala3)),
      env = Map(
        "SEC_GENCERT_CERTS_ROOT" -> "${{ github.workspace }}"
      )
    ),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name     = Some("Compile docs"),
        commands = List("compileDocs"),
        cond     = Some(scalaCondition(Scala2))
      ),
      WorkflowStep.Sbt(
        name     = Some("Regular tests"),
        commands = List("compileTests", "tests/test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Single node integration tests"),
        commands = List("tests / Sit / test"),
        env = Map(
          "SEC_SIT_CERTS_PATH" -> "${{ github.workspace }}/certs",
          "SEC_SIT_AUTHORITY"  -> "es.sec.local"
        ),
        cond = Some(scalaCondition(Scala3))
      )
    ),
    githubWorkflowBuildPostamble += WorkflowStep.Run(
      name     = Some("Stop Single Node"),
      commands = List("pushd .docker", "./single-node.sh down", "popd"),
      cond     = Some(s"always() && ${scalaCondition(Scala3)}")
    ),
    githubWorkflowBuildPostamble ++= Seq(
      WorkflowStep.Run(
        name     = Some("Start Cluster Nodes"),
        commands = List("pushd .docker", "./cluster.sh up -d", "popd"),
        cond     = Some(scalaCondition(Scala3)),
        env = Map(
          "SEC_GENCERT_CERTS_ROOT" -> "${{ github.workspace }}"
        )
      ),
      WorkflowStep.Sbt(
        name     = Some("Cluster integration tests"),
        commands = List("tests / Cit / test"),
        env = Map(
          "SEC_CIT_CERTS_PATH" -> "${{ github.workspace }}/certs",
          "SEC_CIT_AUTHORITY"  -> "es.sec.local"
        ),
        cond = Some(scalaCondition(Scala3))
      ),
      WorkflowStep.Run(
        name     = Some("Stop Cluster Nodes"),
        commands = List("pushd .docker", "./cluster.sh down", "popd"),
        cond     = Some(s"always() && ${scalaCondition(Scala3)}")
      )
    ),
    githubWorkflowPublishTargetBranches := Seq(
      RefPredicate.Equals(Ref.Branch("main")),
      RefPredicate.StartsWith(Ref.Tag("v"))
    ),
    githubWorkflowPublishPreamble +=
      WorkflowStep.Use(sbtghactions.UseRef.Public("olafurpg", "setup-gpg", "v3")),
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
        cond = Some(s"${scalaCondition(Scala2)} && $docsOnMain")
      )
    )
  )
)
