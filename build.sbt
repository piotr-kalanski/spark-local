name := "spark-local"

organization := "com.github.piotr-kalanski"

version := "0.4.0-SNAPSHOT"

scalaVersion := "2.11.8"

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/piotr-kalanski/spark-local"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/piotr-kalanski/spark-local"),
    "scm:git:ssh://github.com/piotr-kalanski/spark-local.git"
  )
)

developers := List(
  Developer(
    id    = "kalan",
    name  = "Piotr Kalanski",
    email = "piotr.kalanski@gmail.com",
    url   = url("https://github.com/piotr-kalanski")
  )
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.databricks" %% "spark-avro" % "3.2.0",
  "com.github.piotr-kalanski" %% "csv2class" % "0.1.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "junit" % "junit" % "4.10" % "test"
)

coverageExcludedPackages := "com.datawizards.sparklocal.examples.*"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
