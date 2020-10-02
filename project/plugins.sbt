addSbtPlugin("ch.epfl.lamp"              % "sbt-dotty"          % "0.4.2")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"       % "0.1.13")
addSbtPlugin("com.codecommit"            % "sbt-github-actions" % "0.9.3")
addSbtPlugin("de.heikoseeberger"         % "sbt-header"         % "5.6.0")
addSbtPlugin("com.geirsson"              % "sbt-ci-release"     % "1.5.3")
addSbtPlugin("org.scalameta"             % "sbt-scalafmt"       % "2.4.2")
addSbtPlugin("io.spray"                  % "sbt-revolver"       % "0.9.1")
addSbtPlugin("com.eed3si9n"              % "sbt-buildinfo"      % "0.10.0")
addSbtPlugin("com.thesamet"              % "sbt-protoc"         % "0.99.34")
addSbtPlugin("org.lyranthe.fs2-grpc"     % "sbt-java-gen"       % "0.7.3")

libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.4.0"
