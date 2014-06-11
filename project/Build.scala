import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "dst-amqp"
  val appVersion      = "1.3"

  val appDependencies = Seq(
    "com.rabbitmq" % "amqp-client" % "2.8.1"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    scalaVersion := "2.10.2",
    organization := "se.destination",
    sources in doc in Compile := List(),
    scalacOptions ++= Seq("-feature", "-language:postfixOps"),

    resolvers += "Typesafe" at "http://repo.typesafe.com/typesafe/repo",
    resolvers += "Maven Repository" at "http://repo1.maven.org/maven2/org/"
  )
}
