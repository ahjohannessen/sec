import Dependencies._

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .dependsOn(`sec-core`, `sec-fs2-core`, `sec-fs2-netty`, `sec-zio-core`, `sec-zio-netty`, `sec-tests`)
  .aggregate(`sec-core`, `sec-fs2-core`, `sec-fs2-netty`, `sec-zio-core`, `sec-zio-netty`, `sec-tests`)

//==== Core ============================================================================================================

lazy val `sec-core` = project
  .in(file("sec-core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "sec-core",
    libraryDependencies ++= compileM(cats, scodecBits, circe, scalaPb, grpcApi) ++ protobufM(scalaPb),
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf"),
    Compile / PB.targets := Seq(scalapb.gen(grpc = false) -> (sourceManaged in Compile).value)
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))

//==== FS2 =============================================================================================================

lazy val `sec-fs2-core` = project
  .in(file("sec-fs2-core"))
  .enablePlugins(AutomateHeaderPlugin, Fs2Grpc)
  .settings(commonSettings)
  .settings(
    name := "sec-fs2-core",
    libraryDependencies ++=
      compileM(cats, catsEffect, fs2, log4cats, log4catsNoop, circe, circeParser),
    Compile / unmanagedSourceDirectories ++=
      scalaVersionSpecificFolders("main", sourceDirectory.value, scalaVersion.value),
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf")
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(`sec-core`)

lazy val `sec-fs2-netty` = project
  .in(file("sec-fs2-netty"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec-fs2", libraryDependencies ++= compileM(grpcNetty, tcnative))
  .dependsOn(`sec-fs2-core`)

//==== ZIO =============================================================================================================

lazy val `sec-zio-core` = project
  .in(file("sec-zio-core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "sec-zio-core",
    libraryDependencies ++=
      compileM(grpcScalaPb) ++ protobufM(scalaPb),
    Compile / unmanagedSourceDirectories ++=
      scalaVersionSpecificFolders("main", sourceDirectory.value, scalaVersion.value),
    Compile / PB.protoSources := Seq((LocalRootProject / baseDirectory).value / "protobuf"),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (sourceManaged in Compile).value,
      scalapb.zio_grpc.ZioCodeGenerator -> (sourceManaged in Compile).value
    ),
    scalacOptions -= "-language:implicitConversions"
  )
  .settings(libraryDependencies := libraryDependencies.value.map(_.withDottyCompat(scalaVersion.value)))
  .dependsOn(`sec-core`)

lazy val `sec-zio-netty` = project
  .in(file("sec-zio-netty"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(name := "sec-zio", libraryDependencies ++= compileM(grpcNetty, tcnative))
  .dependsOn(`sec-zio-core`)

//==== Tests ===========================================================================================================

lazy val SingleNodeITest = config("sit") extend Test
lazy val ClusterITest    = config("cit") extend Test

lazy val integrationSettings = Defaults.testSettings ++ Seq(
  parallelExecution := false
)

lazy val `sec-tests` = project
  .in(file("sec-tests"))
  .enablePlugins(BuildInfoPlugin, AutomateHeaderPlugin)
  .configs(SingleNodeITest, ClusterITest)
  .settings(commonSettings)
  .settings(inConfig(SingleNodeITest)(integrationSettings))
  .settings(inConfig(ClusterITest)(integrationSettings))
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
  .dependsOn(`sec-core`, `sec-fs2-netty`)

//==== Common ==========================================================================================================

def scalaVersionSpecificFolders(srcName: String, srcBaseDir: java.io.File, scalaVersion: String) = {

  def extraDirs(suffix: String): List[File] =
    List(srcBaseDir / srcName / suffix)

  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, 13)) => extraDirs("scala-2.13")
    case Some((0, _))  => extraDirs("scala-3")
    case _             => Nil
  }
}

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
    crossScalaVersions := Seq("2.13.3"),
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

//==== Github Actions ==================================================================================================

addCommandAlias("compileTests", "sec-tests/test:compile; sec-tests/sit:compile; sec-tests/cit:compile;")

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
        name     = Some("Regular tests"),
        commands = List("compileTests", "sec-tests/test")
      ),
      WorkflowStep.Sbt(
        name     = Some("Single node integration tests"),
        commands = List("sec-tests/sit:test"),
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
        commands = List("sec-tests/cit:test"),
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
