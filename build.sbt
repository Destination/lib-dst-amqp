name := "dst-amqp"

organization := "se.destination"

version := "1.6-SNAPSHOT"

publishTo := {
  val repoPath = Path.userHome.absolutePath + "/Dropbox/Destination/dst_maven"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("file", new File(repoPath + "/snapshots"))(Resolver.ivyStylePatterns))
  else
    Some(Resolver.file("file", new File(repoPath + "/releases"))(Resolver.ivyStylePatterns))
}

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.0",
  "com.rabbitmq" % "amqp-client" % "2.8.1"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",         // warning and location for usages of deprecated APIs
  "-feature",             // warning and location for usages of features that should be imported explicitly
  "-unchecked",           // additional warnings where generated code depends on assumptions
  "-Xfatal-warnings",     // Fail the compilation if there are any warnings.
  "-Xlint",               // recommended additional warnings
  "-Ywarn-adapted-args",  // Warn if an argument list is modified to match the receiver
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
  "-Ywarn-inaccessible",
  "-Ywarn-dead-code",
  "-language:reflectiveCalls"
)
