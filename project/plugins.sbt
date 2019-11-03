//addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.8")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.4.31")
addSbtPlugin("io.chrisdavenport" % "sbt-no-publish" % "0.1.0")
addSbtPlugin("org.lyranthe.fs2-grpc" % "sbt-java-gen" % "0.5.4")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.27") // temporary until fs2-grpc 0.5.x with scalapb 0.9.5
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.5"  // temporary until fs2-grpc 0.5.x with scalapb 0.9.5
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")