import BuildKeys._

name := "spark-local"

organization := "com.github.piotr-kalanski"

version := "0.6.0"

scalaVersion := "2.11.8"

sparkVersion in ThisBuild := sys.props.getOrElse("spark.version", "2.1.1")

normalizedName := normalizedName.value + "_" + sparkVersion.value

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
  "org.apache.spark" %% "spark-sql" % sparkVersion.value,
  "org.apache.spark" %% "spark-hive" % sparkVersion.value,
  "com.databricks" %% "spark-avro" % "3.2.0",
  "com.github.piotr-kalanski" %% "csv2class" % "0.3.1",
  "org.apache.parquet" % "parquet-avro" % "1.9.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4",
  "com.github.piotr-kalanski" %% "class2sql" % "0.1.2",
  "com.github.piotr-kalanski" %% "es-client" % "0.2.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.4.1",
  "com.github.piotr-kalanski" %% "data-model-generator" % "0.5.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.h2database" % "h2" % "1.4.195" % "test",
  "com.storm-enroute" %% "scalameter-core" % "0.8.2" % "test",
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.5" % "test",
  "com.github.piotr-kalanski" % "splot" % "0.2.0" % "test"
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
