name := "dst-amqp"

organization := "se.destination"

version := "1.4"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "2.8.1"
)

scalacOptions ++= Seq(
  "-feature"
)

play.Project.playScalaSettings
